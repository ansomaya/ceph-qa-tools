# ceph-qa-tools

Standalone QA utilities for Ceph and other S3-compatible object storage systems.

## Overview

This repository contains small standalone tools intended for QA and validation workflows.

Current tools:

- `cmd/delete-marker-ceph`  
  Generate versioned objects and delete markers under a unique prefix.

- `cmd/mpu-storm`  
  Create large numbers of incomplete multipart uploads under a unique prefix.

These tools are intentionally lightweight and focused on single-purpose workload generation.

## Intended use

These utilities are intended for:

- QA testing
- validation workflows
- controlled test environments
- Ceph or S3-compatible object storage testing

They are not designed as general-purpose benchmark framework features.

## Repository layout

```text
cmd/
  delete-marker-ceph/
    main.go
    README.md
  mpu-storm/
    main.go
    README.md
```

Each tool directory contains its own usage documentation.

## Build and run

Run commands from the root of this repository.

Example:

```bash
go run ./cmd/delete-marker-ceph --help
go run ./cmd/mpu-storm --help
```

Or build binaries:

```bash
go build ./cmd/delete-marker-ceph
go build ./cmd/mpu-storm
```

## Credentials

The tools use standard AWS-style environment variables:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...   # optional
```

## Notes

- These tools target Ceph and other S3-compatible endpoints.
- Bucket creation is handled in a Ceph-compatible way.
- Tool-specific details, examples, and caveats are documented in each tool directory.
