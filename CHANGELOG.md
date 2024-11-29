# Changelog

## v0.1.3 (29-11-2024)
- Adding commercial-data catalog
- Adjusting unit tests to scale with input catalogs

## v0.1.2 (13-11-2024)
- Reformatting to support easier addition of catalogs
- Adding saved-data catalog

## v0.1.1 (09-07-2024)

- Adding unit tests
- Code clean-up to support more granular testing
  - Spltting up functions
  - Adding arguments

## v0.1.0 (21-06-2024)

- Initial commit for workspace catalog generator
  - Listens to Pulsar for new workspace message
  - Generates new STAC catalogs based on provided templates
  - Uploads/removes STAC catalog files to/from S3
  - Send Pulsar message to transformer to trigger ingest pipeline
