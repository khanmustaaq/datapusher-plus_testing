# datapusher_plus-testing
Automated testing suite for CKAN DataPusher Plus extension functionality. This repository provides a complete CI/CD pipeline using GitHub Actions to validate data processing workflows across multiple file formats including CSV, TSV, Excel, and JSON files.

Key Features:
- Full CKAN 2.11 environment setup with PostgreSQL, Solr, and Redis services
- Multi-format file testing with both remote URLs and local repository files
- Real-time DataPusher job monitoring and status validation
- Automated datastore activation and data import verification
- Detailed test reporting with markdown summaries and downloadable artifacts
- Configurable test datasets and extensible file format support

Built for CKAN developers, data engineers, and DevOps teams who need reliable automated validation of data ingestion pipelines. Includes comprehensive error handling, performance metrics, and detailed logging for troubleshooting extension issues.

Tech Stack: CKAN, Python, PostgreSQL, GitHub Actions, Docker
