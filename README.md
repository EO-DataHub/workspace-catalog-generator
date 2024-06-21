## eodhp-workspace-catalog-generator
### Repository to generate workspace STAC catalogs

This repository contains code to generate required STAC catalogs for EODHP workspaces, including `<workspace_name>.json` and `<workspace_name>/processing-results.json` which are required when loading ADES outputs to the user workspace. The generating process is triggered via Pulsar messaging. The new generated catalogs are uploaded to the specified S3 Bucket and a resulting Pulsar message is published to trigger the transformer and ingester to process these new catalogs.

## Installation

1. Follow the instructions [here](https://github.com/UKEODHP/template-python/blob/main/README.md) for installing a
   Python 3.11 environment.
2. Install dependencies:

```commandline
pip3 install -r requirements.txt
```

3. Set up environment variables

```commandline
export PULSAR_URL=<pulsar_url>
export AWS_ACCESS_KEY=<aws_access_key>
export AWS_SECRET_ACCESS_KEY=<aws_secret_access_key>
```

4. Run the service

```commandline
python -m create-workspace-catalogs
```

## Building and testing

This component uses `pytest` tests and the `ruff` and `black` linters. `black` will reformat your code in an
opinionated way.

A number of `make` targets are defined:

- `make test`: run tests continuously
- `make testonce`: run tests once
- `make lint`: lint and reformat
- `make dockerbuild`: build a `latest` Docker image (use `make dockerbuild `VERSION=1.2.3` for a release image)
- `make dockerpush`: push a `latest` Docker image (again, you can add `VERSION=1.2.3`) - normally this should be done
  only via the build system and its GitHub actions.
