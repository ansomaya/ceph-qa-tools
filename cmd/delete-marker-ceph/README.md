# delete-marker-ceph

Standalone QA helper for generating versioned objects and delete markers in a Ceph or other S3-compatible bucket.

## Overview

This tool performs one workflow only:

1. create the bucket if needed
2. enable bucket versioning
3. upload `n` objects under a unique auto-generated prefix
4. delete those objects using S3 multi-object delete batches
5. print a summary including the generated prefix and stage timings

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
  --delete-c 24 \
  --size 128 \
  --path-style=true
```

To build a binary:

```bash
go build ./cmd/delete-marker-ceph
```

## How deletes work

Object uploads are issued concurrently using `PutObject`.

Deletes are sent using `DeleteObjects` in batches of up to `1000` keys per request.  
Those delete batches are processed concurrently using `--delete-c`.

Defaults:
- upload concurrency (`--c`) = `64`
- delete batch concurrency (`--delete-c`) = `24`
- delete batch size = `1000`

This makes the tool better suited for large delete-marker generation runs on Ceph RGW and other S3-compatible systems.

## Prefix format

A unique prefix is generated automatically for each run:

```text
<prefix-base>/<size>b-<count>-c<upload_concurrency>-dc<delete_concurrency>-<epoch_ms>/
```

Example:

```text
dm/128b-10000-c32-dc24-1719234567123/
```

The prefix is printed in the logs and final summary. Save it if you want to inspect the run later.

## Flags

- `--endpoint` S3 endpoint URL, required
- `--region` AWS signing region, default `us-east-1`
- `--bucket` bucket name, required
- `--prefix-base` base path for generated prefixes, default `dm`
- `--n` number of objects, default `1000`
- `--c` upload concurrency, default `64`
- `--delete-c` delete batch concurrency, default `24`
- `--size` object size in bytes, default `128`
- `--path-style` use path-style addressing, default `true`
- `--insecure` skip TLS certificate verification, default `false`
- `--create-bucket` create bucket if missing, default `true`

## Example summary

```text
using prefix "dm/128b-100000-c128-dc24-1719234567123/"
----- summary -----
bucket:             delete-marker-test
prefix:             dm/128b-100000-c128-dc24-1719234567123/
uploaded:           100000
objects deleted:    100000
delete batches:     100
delete batch size:  1000
delete concurrency: 24
object versions:    100000
delete markers:     100000
expected markers:   100000
upload runtime:     3m45.724965854s
delete runtime:     3m31.281817102s
count runtime:      6.460215363s
total runtime:      7m23.467059089s
```

## Example performance note

In one 100k-object test run on a Ceph RGW environment:

- `--delete-c 1` took about `32m58s` total
- `--delete-c 24` took about `7m23s` total

Actual results will vary by RGW deployment, backend load, and network conditions, but parallel delete batching can significantly reduce total runtime.

## Post-run verification with AWS CLI

Delete marker count:

```bash
aws s3api list-object-versions \
  --bucket delete-marker-test \
  --prefix 'dm/128b-10000-c32-dc24-1719234567123/' \
  --endpoint-url https://rgw.example.com:443 \
  --output json \
| jq '.DeleteMarkers | length'
```

Object version count:

```bash
aws s3api list-object-versions \
  --bucket delete-marker-test \
  --prefix 'dm/128b-10000-c32-dc24-1719234567123/' \
  --endpoint-url https://rgw.example.com:443 \
  --output json \
| jq '.Versions | length'
```

## Notes

- Bucket creation is done in a Ceph-compatible way without sending a location constraint.
- This tool is intended for QA/testing workflows.
- `--delete-c` should be tuned for the target environment if needed.
- If these helper scripts grow significantly, moving them to a dedicated repository may be cleaner than keeping them under the Warp tree.
