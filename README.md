# Daily adsence ETL Pipeline using R - MSSQL

Automated ingestion, validation, transformation, and loading of NHS workforce absence data.

#Overview
This project replaces a fragile, manual workflow for producing the Trust’s daily absence report with a fully automated ETL pipeline built using R, SQL Server, and Windows Task Scheduler. It ingests ESR extracts and supplementary spreadsheets, validates their structure, transforms the data, and loads it into SQL staging tables for downstream reporting.

The pipeline reduces processing time from up to an hour to under five minutes, improves data quality, and strengthens auditability through structured logging.

#Key Features
Automated ingestion using scheduled execution

File‑checker to validate presence, naming, and schema

Multi‑stage validation for completeness, accuracy, and consistency

Modular R scripts for extraction, transformation, and loading

SQL Server integration with staging tables and stored procedures

Structured logging for auditability and troubleshooting

Scalable architecture ready for cloud orchestration in future
