# Azure Data Engineering Pipeline (PySpark)

This project demonstrates an end-to-end data engineering pipeline using PySpark, Azure, and Terraform.

## Overview

This project simulates a production-style data pipeline with:

- Task-based orchestration
- Retry logic
- Structured logging
- Timestamp-based data versioning
- Azure cloud integration

## Features

- Data ingestion from CSV  
- Data transformation using PySpark  
- Aggregation of sales data  
- Output of analytics-ready dataset  
- Upload of processed data to Azure Blob Storage  
- Infrastructure deployed using Terraform (Infrastructure as Code)  
- Secure credential handling using environment variables  

## Tech Stack

- Python  
- PySpark  
- Azure Blob Storage  
- Terraform  
- Git  

## Pipeline Flow

1. Raw data is loaded from CSV  
2. Data is transformed using PySpark  
3. Output is saved locally  
4. Processed data is uploaded to Azure Blob Storage  

## Cloud Setup

- Azure Resource Group  
- Azure Storage Account  
- Deployed using Terraform  

## Security

- Sensitive credentials are handled using environment variables  
- No secrets are stored in the codebase  

## Purpose

This project simulates a real-world data engineering workflow similar to pipelines built in Azure-based data platforms (e.g., MS Fabric, Data Lake, Spark processing).

## Project Structure

```bash
data-pipeline/
│
├── data/
├── output/
├── pipeline.py
├── sql_step.py
├── upload_to_azure.py
├── run_pipeline.py
├── tasks.py
├── scheduler.py
├── logger.py
├── config.json
├── infra/
│   └── main.tf
├── README.md
```

## Pipeline Orchestration

The pipeline is structured using a task-based orchestration pattern:

- `tasks.py` defines reusable pipeline tasks with retry logic
- `run_pipeline.py` orchestrates execution of tasks
- `scheduler.py` enables automated recurring pipeline runs
- Each pipeline run is versioned using timestamp-based folders