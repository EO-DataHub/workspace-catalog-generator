## eodhp-workspace-catalog-generator
### Repository to generate workspace STAC catalogs

This repository contains code to generate required STAC catalogs for EODHP workspaces, including `<workspace_name>.json` and `<workspace_name>/processing-results.json` which are required when loading ADES outputs to the user workspace. The generating process is triggered via Pulsar messaging. The new generated catalogs are uploaded to the specified S3 Bucket and a resulting Pulsar message is published to trigger the transformer and ingester to process these new catalogs.

## Installation

### Prerequisites

- [uv](https://docs.astral.sh/uv/) package manager

### Install via makefile

```commandline
make setup
```

This will install all dependencies using `uv sync` and set up `pre-commit` hooks.

It's safe and fast to run `make setup` repeatedly as it will only update these things if
they have changed.

After `make setup` you can run `make pre-commit` to run pre-commit checks on staged changes and
`make pre-commit-all` to run them on all files. This replicates the linter checks that
run from GitHub actions.

### Environment variables

```commandline
export PULSAR_URL=<pulsar_url>
export AWS_ACCESS_KEY=<aws_access_key>
export AWS_SECRET_ACCESS_KEY=<aws_secret_access_key>
```

### Run the service

```commandline
uv run python -m catalog_generator
```

## Building and testing

This component uses `pytest` tests and `ruff` for linting and formatting.

A number of `make` targets are defined:

- `make test`: run tests continuously (watch mode)
- `make testonce`: run tests once
- `make check`: run linting, formatting check, type checking, and pyproject validation
- `make format`: auto-fix lint issues and reformat code
- `make dockerbuild`: build a `latest` Docker image (use `make dockerbuild VERSION=1.2.3` for a release image)
- `make dockerpush`: push a `latest` Docker image (again, you can add `VERSION=1.2.3`) - normally this should be done
  only via the build system and its GitHub actions.
