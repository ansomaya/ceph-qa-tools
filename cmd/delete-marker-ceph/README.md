# delete-marker-ceph

Standalone QA helper for generating versioned objects and delete markers in a Ceph or other S3-compatible bucket.

## Overview

This tool performs one workflow only:

1. create the bucket if needed
2. enable bucket versioning
3. upload `n` objects under a unique auto-generated prefix
4. delete those objects
5. print a summary including the generated prefix

It is intentionally single-purpose.  
For later inspection or counting, use AWS CLI or equivalent tooling.

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
go run ./cmd/delete-marker-ceph \
  --endpoint https://rgw.example.com:443 \
  --region us-east-1 \
  --bucket delete-marker-test \
  --prefix-base dm \
  --n 10000 \
  --c 32 \
  --size 128 \
  --path-style=true
```

To build a binary:

```bash
go build ./cmd/delete-marker-ceph
```

## Prefix format

A unique prefix is generated automatically for each run:

```text
<prefix-base>/<size>b-<count>-c<concurrency>-<epoch_ms>/
```

Example:

```text
dm/128b-10000-c32-1719234567123/
```

The prefix is printed in the logs and final summary. Save it if you want to inspect the run later.

## Flags

- `--endpoint` S3 endpoint URL, required
- `--region` AWS signing region, default `us-east-1`
- `--bucket` bucket name, required
- `--prefix-base` base path for generated prefixes, default `dm`
- `--n` number of objects, default `1000`
- `--c` concurrency, default `64`
- `--size` object size in bytes, default `128`
- `--path-style` use path-style addressing, default `true`
- `--insecure` skip TLS certificate verification, default `false`
- `--create-bucket` create bucket if missing, default `true`

## Example summary

```text
using prefix "dm/128b-10000-c32-1719234567123/"
----- summary -----
bucket:           delete-marker-test
prefix:           dm/128b-10000-c32-1719234567123/
uploaded:         10000
delete requests:  10000
object versions:  10000
delete markers:   10000
expected markers: 10000
```

## Post-run verification with AWS CLI

Delete marker count:

```bash
aws s3api list-object-versions \
  --bucket delete-marker-test \
  --prefix 'dm/128b-10000-c32-1719234567123/' \
  --endpoint-url https://rgw.example.com:443 \
  --output json \
| jq '.DeleteMarkers | length'
```

Object version count:

```bash
aws s3api list-object-versions \
  --bucket delete-marker-test \
  --prefix 'dm/128b-10000-c32-1719234567123/' \
  --endpoint-url https://rgw.example.com:443 \
  --output json \
| jq '.Versions | length'
```

## Notes

- Bucket creation is done in a Ceph-compatible way without sending a location constraint.
- This tool is intended for QA/testing workflows.
- If these helper scripts grow significantly, moving them to a dedicated repository may be cleaner than keeping them under the Warp tree.
