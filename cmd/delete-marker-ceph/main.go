package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const deleteBatchSize = 1000

type cfg struct {
	Endpoint          string
	Region            string
	Bucket            string
	Prefix            string
	PrefixBase        string
	Count             int
	Concurrency       int
	DeleteConcurrency int
	Size              int
	Insecure          bool
	PathStyle         bool
	CreateBucket      bool
}

func main() {
	var c cfg
	flag.StringVar(&c.Endpoint, "endpoint", "", "S3 endpoint URL, e.g. https://ceph-rgw:443")
	flag.StringVar(&c.Region, "region", "us-east-1", "AWS region")
	flag.StringVar(&c.Bucket, "bucket", "", "Bucket name")
	flag.StringVar(&c.PrefixBase, "prefix-base", "dm", "Base path for auto-generated prefix")
	flag.IntVar(&c.Count, "n", 1000, "Number of objects")
	flag.IntVar(&c.Concurrency, "c", 64, "Upload concurrency")
	flag.IntVar(&c.DeleteConcurrency, "delete-c", 24, "DeleteObjects batch concurrency")
	flag.IntVar(&c.Size, "size", 128, "Object size in bytes")
	flag.BoolVar(&c.Insecure, "insecure", false, "Skip TLS verification")
	flag.BoolVar(&c.PathStyle, "path-style", true, "Use path-style addressing")
	flag.BoolVar(&c.CreateBucket, "create-bucket", true, "Create bucket if missing")
	flag.Parse()

	if c.Endpoint == "" || c.Bucket == "" {
		flag.Usage()
		os.Exit(2)
	}
	if c.DeleteConcurrency < 1 {
		log.Fatal("delete-c must be >= 1")
	}

	c.Prefix = autoPrefix(c.PrefixBase, c.Size, c.Count, c.Concurrency, c.DeleteConcurrency)

	ctx := context.Background()

	ak := os.Getenv("AWS_ACCESS_KEY_ID")
	sk := os.Getenv("AWS_SECRET_ACCESS_KEY")
	st := os.Getenv("AWS_SESSION_TOKEN")
	if ak == "" || sk == "" {
		log.Fatal("missing AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY")
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        10000,
			MaxIdleConnsPerHost: 10000,
			IdleConnTimeout:     90 * time.Second,
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: c.Insecure}, //nolint:gosec
		},
	}

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(c.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, st)),
		config.WithHTTPClient(httpClient),
	)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	awsCfg.BaseEndpoint = aws.String(strings.TrimRight(c.Endpoint, "/"))

	s3c := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = c.PathStyle
	})

	log.Printf("using prefix %q", c.Prefix)

	if err := ensureBucket(ctx, s3c, c); err != nil {
		log.Fatalf("ensure bucket failed: %v", err)
	}
	if err := enableVersioning(ctx, s3c, c.Bucket); err != nil {
		log.Fatalf("enable versioning failed: %v", err)
	}

	payload := bytes.Repeat([]byte("x"), c.Size)
	totalStart := time.Now()

	uploadStart := time.Now()
	up, err := uploadObjects(ctx, s3c, c, payload)
	uploadDur := time.Since(uploadStart)
	if err != nil {
		log.Fatalf("upload failed after %d objects: %v", up, err)
	}

	deleteStart := time.Now()
	del, err := deleteObjects(ctx, s3c, c)
	deleteDur := time.Since(deleteStart)
	if err != nil {
		log.Fatalf("delete failed after %d objects: %v", del, err)
	}

	countStart := time.Now()
	versions, markers, err := countVersionsAndMarkers(ctx, s3c, c)
	countDur := time.Since(countStart)
	if err != nil {
		log.Fatalf("count failed: %v", err)
	}

	fmt.Println("----- summary -----")
	fmt.Printf("bucket:             %s\n", c.Bucket)
	fmt.Printf("prefix:             %s\n", c.Prefix)
	fmt.Printf("uploaded:           %d\n", up)
	fmt.Printf("objects deleted:    %d\n", del)
	fmt.Printf("delete batches:     %d\n", numDeleteBatches(c.Count))
	fmt.Printf("delete batch size:  %d\n", deleteBatchSize)
	fmt.Printf("delete concurrency: %d\n", c.DeleteConcurrency)
	fmt.Printf("object versions:    %d\n", versions)
	fmt.Printf("delete markers:     %d\n", markers)
	fmt.Printf("expected markers:   %d\n", c.Count)
	fmt.Printf("upload runtime:     %s\n", uploadDur)
	fmt.Printf("delete runtime:     %s\n", deleteDur)
	fmt.Printf("count runtime:      %s\n", countDur)
	fmt.Printf("total runtime:      %s\n", time.Since(totalStart))
}

func autoPrefix(base string, size, count, concurrency, deleteConcurrency int) string {
	base = strings.Trim(base, "/")
	if base == "" {
		base = "dm"
	}
	return fmt.Sprintf("%s/%db-%d-c%d-dc%d-%d/", base, size, count, concurrency, deleteConcurrency, time.Now().UnixMilli())
}

func ensureBucket(ctx context.Context, s3c *s3.Client, c cfg) error {
	_, err := s3c.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(c.Bucket),
	})
	if err == nil {
		return nil
	}
	if !c.CreateBucket {
		return fmt.Errorf("bucket %q does not exist or is not accessible", c.Bucket)
	}

	_, err = s3c.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(c.Bucket),
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

func enableVersioning(ctx context.Context, s3c *s3.Client, bucket string) error {
	_, err := s3c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &s3types.VersioningConfiguration{
			Status: s3types.BucketVersioningStatusEnabled,
		},
	})
	return err
}

func uploadObjects(ctx context.Context, s3c *s3.Client, c cfg, payload []byte) (int64, error) {
	var done int64
	jobs := make(chan int, c.Concurrency*4)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for w := 0; w < c.Concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				key := fmt.Sprintf("%sobject-%08d", ensureSlash(c.Prefix), i)
				var err error
				for attempt := 1; attempt <= 5; attempt++ {
					_, err = s3c.PutObject(ctx, &s3.PutObjectInput{
						Bucket: aws.String(c.Bucket),
						Key:    aws.String(key),
						Body:   bytes.NewReader(payload),
					})
					if err == nil {
						n := atomic.AddInt64(&done, 1)
						if n%10000 == 0 {
							log.Printf("uploaded %d / %d", n, c.Count)
						}
						break
					}
					time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
				}
				if err != nil {
					select {
					case errCh <- fmt.Errorf("put %s: %w", key, err):
					default:
					}
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for i := 1; i <= c.Count; i++ {
			jobs <- i
		}
	}()

	wg.Wait()

	select {
	case err := <-errCh:
		return done, err
	default:
		return done, nil
	}
}

type deleteBatch struct {
	start int
	end   int
}

func deleteObjects(ctx context.Context, s3c *s3.Client, c cfg) (int64, error) {
	var deleted int64
	jobs := make(chan deleteBatch, c.DeleteConcurrency*2)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for w := 0; w < c.DeleteConcurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				objs := make([]s3types.ObjectIdentifier, 0, batch.end-batch.start+1)
				for i := batch.start; i <= batch.end; i++ {
					key := fmt.Sprintf("%sobject-%08d", ensureSlash(c.Prefix), i)
					objs = append(objs, s3types.ObjectIdentifier{Key: aws.String(key)})
				}

				out, err := s3c.DeleteObjects(ctx, &s3.DeleteObjectsInput{
					Bucket: aws.String(c.Bucket),
					Delete: &s3types.Delete{
						Objects: objs,
						Quiet:   aws.Bool(true),
					},
				})
				if err != nil {
					select {
					case errCh <- fmt.Errorf("delete batch %d-%d: %w", batch.start, batch.end, err):
					default:
					}
					return
				}

				if len(out.Errors) > 0 {
					first := out.Errors[0]
					key := ""
					code := ""
					msg := ""
					if first.Key != nil {
						key = aws.ToString(first.Key)
					}
					if first.Code != nil {
						code = aws.ToString(first.Code)
					}
					if first.Message != nil {
						msg = aws.ToString(first.Message)
					}
					select {
					case errCh <- fmt.Errorf("delete batch %d-%d had %d object errors; first key=%q code=%q message=%q", batch.start, batch.end, len(out.Errors), key, code, msg):
					default:
					}
					return
				}

				n := atomic.AddInt64(&deleted, int64(len(objs)))
				if n%10000 == 0 || n == int64(c.Count) {
					log.Printf("deleted %d / %d", n, c.Count)
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for start := 1; start <= c.Count; start += deleteBatchSize {
			end := start + deleteBatchSize - 1
			if end > c.Count {
				end = c.Count
			}
			jobs <- deleteBatch{start: start, end: end}
		}
	}()

	wg.Wait()

	select {
	case err := <-errCh:
		return deleted, err
	default:
		return deleted, nil
	}
}

func numDeleteBatches(count int) int {
	if count <= 0 {
		return 0
	}
	return (count + deleteBatchSize - 1) / deleteBatchSize
}

func countVersionsAndMarkers(ctx context.Context, s3c *s3.Client, c cfg) (int64, int64, error) {
	var versions int64
	var markers int64
	var keyMarker *string
	var versionMarker *string

	for {
		out, err := s3c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket:          aws.String(c.Bucket),
			Prefix:          aws.String(ensureSlash(c.Prefix)),
			KeyMarker:       keyMarker,
			VersionIdMarker: versionMarker,
		})
		if err != nil {
			return versions, markers, err
		}

		for range out.Versions {
			versions++
		}
		for range out.DeleteMarkers {
			markers++
		}

		if !aws.ToBool(out.IsTruncated) {
			break
		}
		keyMarker = out.NextKeyMarker
		versionMarker = out.NextVersionIdMarker
	}

	return versions, markers, nil
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
