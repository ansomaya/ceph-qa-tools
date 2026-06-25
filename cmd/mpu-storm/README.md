# mpu-storm

Standalone QA helper for creating large numbers of incomplete multipart uploads in a Ceph or other S3-compatible bucket.

## Overview

This tool performs one workflow only:

1. create the bucket if needed
2. generate a unique auto-generated prefix
3. create multipart uploads under that prefix
4. optionally upload one part for a percentage of MPUs
5. write key and upload ID details to a CSV file

The tool intentionally does not complete or abort the multipart uploads.

## Prerequisites

- Go installed
- valid S3-compatible credentials
- network access to the target endpoint

Credentials are read from environment variables:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...   # optional
```

## Build and run

Run commands from the root of this repository.

```bash
go run ./cmd/mpu-storm \
  --endpoint http://rgw.local:7480 \
  --region us-east-1 \
  --bucket mpu-test \
  --prefix-base mpu \
  --n 1000000 \
  --c 512 \
  --one-part-pct 10 \
  --part-mib 5 \
  --path-style=true \
  --out mpu_ids.csv
```

To build a binary:

```bash
go build ./cmd/mpu-storm
```

## Prefix format

A unique prefix is generated automatically for each run:

```text
<prefix-base>/<partMiB>mib-<count>-c<concurrency>-p<onePartPct>-<epoch_ms>/
```

Example:

```text
mpu/5mib-1000000-c512-p10-1719234567123/
```

The prefix is printed in startup, progress, and final logs.

## Flags

- `--endpoint` S3 endpoint URL, required
- `--region` AWS signing region, default `us-east-1`
- `--bucket` bucket name, required
- `--prefix-base` base path for generated prefixes, default `mpu`
- `--n` number of multipart uploads to create, default `1000`
- `--c` worker concurrency, default `256`
- `--one-part-pct` percentage of MPUs that upload one part, default `10`
- `--part-mib` part size in MiB, default `5`
- `--path-style` use path-style addressing, default `true`
- `--insecure` skip TLS certificate verification, default `false`
- `--create-bucket` create bucket if missing, default `true`
- `--out` CSV output file, default `mpu_ids.csv`
- `--rate` optional global request rate limit, default `0`

## Output CSV

The CSV file contains:

```text
key,upload_id,one_part
```

This can be used for inspection or correlation with lifecycle-driven cleanup behavior.

## Notes

- Bucket creation is done in a Ceph-compatible way without sending a location constraint.
- This tool is intended for QA/testing workflows.
- Multipart cleanup is expected to be handled by Ceph lifecycle policy.
- If these helper scripts grow significantly, moving them to a dedicated repository may be cleaner than keeping them under the Warp tree.
