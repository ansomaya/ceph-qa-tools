package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	mrand "math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type result struct {
	key      string
	uploadID string
	onePart  bool
	err      error
}

func main() {
	var (
		endpoint      = flag.String("endpoint", "", "S3 endpoint URL, e.g. http://ceph-rgw:7480 (required)")
		region        = flag.String("region", "us-east-1", "AWS region string (S3-compatible services often accept any value)")
		bucket        = flag.String("bucket", "", "Bucket name (required)")
		prefixBase    = flag.String("prefix-base", "mpu", "Base path for auto-generated prefix")
		total         = flag.Int("n", 1000, "Number of multipart uploads to create")
		concurrency   = flag.Int("c", 256, "Number of concurrent workers")
		onePartPct    = flag.Float64("one-part-pct", 10.0, "Percent of MPUs to upload exactly 1 part for (0-100)")
		partSizeMiB   = flag.Int("part-mib", 5, "Part size (MiB) for the single part uploads (use >=5 for S3 semantics)")
		insecureTLS   = flag.Bool("insecure", false, "Skip TLS verification (only for https endpoints with self-signed certs)")
		forcePath     = flag.Bool("path-style", true, "Use path-style addressing (recommended for Ceph/MinIO)")
		createBucket  = flag.Bool("create-bucket", true, "Create bucket if missing")
		outFile       = flag.String("out", "mpu_ids.csv", "Output CSV file for key,uploadId,onePart (for cleanup)")
		ratePerSecond = flag.Float64("rate", 0, "Optional global rate limit (requests/sec). 0 = unlimited")
	)
	flag.Parse()

	if *endpoint == "" || *bucket == "" {
		flag.Usage()
		os.Exit(2)
	}
	if *onePartPct < 0 || *onePartPct > 100 {
		log.Fatalf("--one-part-pct must be between 0 and 100")
	}
	if *partSizeMiB < 5 {
		log.Printf("warning: --part-mib < 5; some S3 implementations may reject small parts")
	}

	prefix := autoPrefix(*prefixBase, *partSizeMiB, *total, *concurrency, *onePartPct)

	mrand.Seed(time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		log.Printf("interrupt received, stopping...")
		cancel()
	}()

	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        10000,
			MaxIdleConnsPerHost: 10000,
			IdleConnTimeout:     90 * time.Second,
			TLSClientConfig:     tlsConfig(*insecureTLS),
		},
		Timeout: 0,
	}

	ak := os.Getenv("AWS_ACCESS_KEY_ID")
	sk := os.Getenv("AWS_SECRET_ACCESS_KEY")
	st := os.Getenv("AWS_SESSION_TOKEN")

	if ak == "" || sk == "" {
		log.Fatalf("missing AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY environment variables")
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(*region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, st)),
		config.WithHTTPClient(httpClient),
	)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	cfg.BaseEndpoint = aws.String(strings.TrimRight(*endpoint, "/"))

	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = *forcePath
	})

	log.Printf("using prefix %q", prefix)

	if err := ensureBucket(ctx, s3c, *bucket, *createBucket); err != nil {
		log.Fatalf("ensure bucket failed: %v", err)
	}

	partBytes := make([]byte, (*partSizeMiB)*1024*1024)
	if _, err := rand.Read(partBytes); err != nil {
		log.Fatalf("rand: %v", err)
	}

	f, err := os.Create(*outFile)
	if err != nil {
		log.Fatalf("create output file: %v", err)
	}
	defer f.Close()
	fmt.Fprintln(f, "key,upload_id,one_part")

	var (
		created uint64
		failed  uint64
		partsOK uint64
	)

	var rateCh <-chan time.Time
	if *ratePerSecond > 0 {
		interval := time.Duration(float64(time.Second) / *ratePerSecond)
		t := time.NewTicker(interval)
		defer t.Stop()
		rateCh = t.C
	}

	jobs := make(chan int, *concurrency*4)
	results := make(chan result, *concurrency*4)

	var wg sync.WaitGroup
	wg.Add(*concurrency)

	for w := 0; w < *concurrency; w++ {
		go func() {
			defer wg.Done()
			for i := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}
				key := fmt.Sprintf("%s%s-%09d", ensureSlash(prefix), randHex(8), i)

				onePart := mrand.Float64()*100.0 < *onePartPct

				if rateCh != nil {
					select {
					case <-ctx.Done():
						return
					case <-rateCh:
					}
				}
				cmu, err := s3c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: bucket,
					Key:    aws.String(key),
				})
				if err != nil {
					atomic.AddUint64(&failed, 1)
					results <- result{key: key, onePart: onePart, err: err}
					continue
				}

				atomic.AddUint64(&created, 1)
				uploadID := aws.ToString(cmu.UploadId)

				if onePart {
					if rateCh != nil {
						select {
						case <-ctx.Done():
							return
						case <-rateCh:
						}
					}
					body := bytes.NewReader(partBytes)
					_, err = s3c.UploadPart(ctx, &s3.UploadPartInput{
						Bucket:     bucket,
						Key:        aws.String(key),
						UploadId:   aws.String(uploadID),
						PartNumber: aws.Int32(1),
						Body:       body,
					})
					if err == nil {
						atomic.AddUint64(&partsOK, 1)
					} else {
						atomic.AddUint64(&failed, 1)
						results <- result{key: key, uploadID: uploadID, onePart: onePart, err: err}
						continue
					}
				}

				results <- result{key: key, uploadID: uploadID, onePart: onePart, err: nil}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for i := 0; i < *total; i++ {
			select {
			case <-ctx.Done():
				return
			case jobs <- i:
			}
		}
	}()

	start := time.Now()
	doneCollect := make(chan struct{})
	go func() {
		defer close(doneCollect)
		var n int
		for r := range results {
			n++
			if r.err != nil && n <= 20 {
				log.Printf("error: key=%s onePart=%v uploadId=%s err=%v", r.key, r.onePart, r.uploadID, r.err)
			}
			fmt.Fprintf(f, "%s,%s,%t\n", r.key, r.uploadID, r.onePart)
		}
	}()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c := atomic.LoadUint64(&created)
				p := atomic.LoadUint64(&partsOK)
				fl := atomic.LoadUint64(&failed)
				elapsed := time.Since(start).Seconds()
				rps := float64(c) / math.Max(elapsed, 0.001)
				log.Printf("prefix=%s created=%d parts_ok=%d failed=%d elapsed=%.1fs created_rps=%.1f", prefix, c, p, fl, elapsed, rps)
			}
		}
	}()

	wg.Wait()
	close(results)
	<-doneCollect

	c := atomic.LoadUint64(&created)
	p := atomic.LoadUint64(&partsOK)
	fl := atomic.LoadUint64(&failed)
	elapsed := time.Since(start).Seconds()
	log.Printf("DONE prefix=%s created=%d parts_ok=%d failed=%d elapsed=%.1fs", prefix, c, p, fl, elapsed)
	log.Printf("upload IDs written to %s (use for abort/cleanup)", *outFile)
}

func autoPrefix(base string, partSizeMiB, count, concurrency int, onePartPct float64) string {
	base = strings.Trim(base, "/")
	if base == "" {
		base = "mpu"
	}
	return fmt.Sprintf("%s/%dmib-%d-c%d-p%s-%d/", base, partSizeMiB, count, concurrency, pctTag(onePartPct), time.Now().UnixMilli())
}

func pctTag(v float64) string {
	if v == math.Trunc(v) {
		return fmt.Sprintf("%.0f", v)
	}
	return strings.ReplaceAll(fmt.Sprintf("%.2f", v), ".", "_")
}

func ensureBucket(ctx context.Context, s3c *s3.Client, bucket string, createBucket bool) error {
	_, err := s3c.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err == nil {
		return nil
	}
	if !createBucket {
		return fmt.Errorf("bucket %q does not exist or is not accessible", bucket)
	}

	_, err = s3c.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err == nil {
		return nil
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		if code == "BucketAlreadyOwnedByYou" || code == "BucketAlreadyExists" {
			return nil
		}
	}
	return err
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func tlsConfig(insecure bool) *tls.Config {
	return &tls.Config{InsecureSkipVerify: insecure} //nolint:gosec
}

func ensureSlash(p string) string {
	if p == "" {
		return ""
	}
	if strings.HasSuffix(p, "/") {
		return p
	}
	return p + "/"
}
