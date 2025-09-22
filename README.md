[![DataPusher+ Testing Stress Run](https://github.com/dathere/datapusher-plus_testing/actions/workflows/runner.yml/badge.svg)](https://github.com/dathere/datapusher-plus_testing/actions/workflows/runner.yml)
[![DataPusher+ Testing Smoke Run](https://github.com/dathere/datapusher-plus_testing/actions/workflows/main.yml/badge.svg)](https://github.com/dathere/datapusher-plus_testing/actions/workflows/main.yml)
# DataPusher+ testing
Automated testing suite for DataPusher+ extension functionality. This repository provides a complete pipeline using GitHub Actions to validate data processing workflows across multiple file formats including CSV, TSV, Excel, and JSON files.

Key Features:
- Full CKAN 2.11 environment setup with PostgreSQL, Solr, and Redis services
- Multi-format file testing with both remote URLs and local repository files
- Real-time DataPusher job monitoring and status validation
- Automated datastore activation and data import verification
- Detailed test reporting with markdown summaries and downloadable artifacts
- Configurable test datasets and extensible file format support

Includes error handling, performance metrics, and detailed logging for troubleshooting extension issues.

