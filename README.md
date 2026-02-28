# Daily adsence ETL Pipeline using R - MSSQL

Automated ingestion, validation, transformation, and loading of NHS workforce absence data.

# Overview
This project replaces a fragile, manual workflow for producing the Trust’s daily absence report with a fully automated ETL pipeline built using R, SQL Server, and Windows Task Scheduler. It ingests ESR extracts and supplementary spreadsheets, validates their structure, transforms the data, and loads it into SQL staging tables for downstream reporting.

The pipeline reduces processing time from up to an hour to under five minutes, improves data quality, and strengthens auditability through structured logging.

# Key Features
Automated ingestion using scheduled execution

File‑checker to validate presence, naming, and schema

Multi‑stage validation for completeness, accuracy, and consistency

Modular R scripts for extraction, transformation, and loading

SQL Server integration with staging tables and stored procedures

Structured logging for auditability and troubleshooting

Scalable architecture ready for cloud orchestration in future

# Architecture

Input Files → File‑Checker → Validation Layer → Transformation Layer → SQL Staging Load → Logging
## Components:
File‑Checker: Ensures all required files are present and valid

Validation Layer: Schema checks, row counts, data‑type enforcement

Transformation Layer: Cleans, standardises, and maps data

SQL Load: Writes to staging tables and triggers stored procedures

Logging: Captures run history, errors, and data quality metrics

# Technologies Used
  R – ingestion, validation, transformation
  
  SQL Server – storage, business logic
  
  Windows Task Scheduler – automation
  
  Power BI – downstream reporting (not part of this repo but supported by outputs)
