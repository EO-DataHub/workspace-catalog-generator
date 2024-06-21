# Changelog

## v0.1.0 (21-06-2024)

- Initial commit for workspace catalog generator
  - Listens to Pulsar for new workspace message
  - Generates new STAC catalogs based on provided templates
  - Uploads/removes STAC catalog files to/from S3
  - Send Pulsar message to transformer to trigger ingest pipeline
