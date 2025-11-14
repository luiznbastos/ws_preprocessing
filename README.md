# WhoScored Preprocessing Service

## Project Overview (Shared)
- Business objective: deliver actionable soccer performance insights from historical and near‑real‑time data, inspired by Soccermatics methods (https://soccermatics.readthedocs.io/en/latest/index.html).
- Pipeline (end‑to‑end):
  - ws_scrapping: acquire raw match/event data from web sources/APIs
  - ws_preprocessing: validate, normalize, and load to staging (S3→Parquet→Redshift when applicable)
  - ws_dbt: transform into bronze/silver/gold models for analytics (team/player/match)
  - ws_orchestrator: schedule and monitor flows (S3 state when applicable)
  - ws_streamlit: visualize KPIs and match insights
  - ws_infrastructure: IaC for compute, storage, security, CI/CD
- Data stores: AWS S3 and Redshift are primary; not all steps use both.
- Future (planned next year): add an xG/xT training job; extend the pipeline/dbt (Python models or external tasks) to load trained parameters, infer and persist xG/xT per event, and compute aggregates, using dbt tags to separate standard vs inference runs.

## Producer–Consumer Pattern
- Consumer of shared infrastructure produced by `ws_infrastructure`.
- Consumes from SSM:
  - ECR image URL: `/${project}/ecr/${job_name}/url`
  - Batch job queue ARN: `/${project}/batch/job-queue-arn`
  - Analytics bucket name: `/${project}/s3/analytics/name`
  - Database parameters: `/${project}/database/{host,username,password,database}`
- Produces back to SSM:
  - Batch job definition ARN: `/${project}/batch/jobs/${job_name}/arn` (for orchestration)

ETL service that transforms raw JSON data from S3 into structured relational tables in Redshift. Originally designed for PostgreSQL databases, this service processes scraped match event data and loads it into the bronze layer of the data warehouse.

## Overview

This preprocessing service is part of the WhoScored analytics pipeline. It reads raw JSON event data stored in S3 by the scraping service and transforms it into normalized database tables for analysis.

## Orchestration & Pipeline Context
- Cloud execution: this job is triggered by the orchestrator.
- Previous step: `ws_scrapping` writes raw artifacts to S3.
- Next step: `ws_dbt` models build silver/gold layers from these bronze tables.

### Key Features

- **Parallel Processing**: Multi-worker chunk-based processing for performance
- **Multiple Run Modes**: Support for full, incremental, date range, and orchestrated runs
- **Type-Safe Configuration**: Pydantic-based settings with environment variable support
- **AWS Native**: Designed to run as AWS Batch jobs with SSM parameter integration
- **Resource Efficient**: Memory-aware processing with cleanup mechanisms

## Architecture

### ETL Framework

The service uses a custom Python ETL framework built on:

- **pandas**: Data manipulation and transformation
- **SQLAlchemy**: Database connectivity and ORM
- **redshift-connector**: Redshift-specific database driver
- **boto3**: AWS S3 and SSM integration

### Processing Flow

```
1. ChunkManager
   ├─ Queries scrape_runs table based on run type
   ├─ Generates S3 keys for JSON files
   └─ Splits keys into parallel processing chunks

2. ETLTask (per chunk)
   ├─ Extracts: Fetches JSON from S3
   ├─ Transforms: Normalizes event data
   └─ Loads: Writes to temporary tables

3. DataLoader
   ├─ Merges temporary tables
   ├─ Loads to final bronze tables
   └─ Records processing metadata
```

## Run Types

The service supports four run modes controlled by `RUN_TYPE`:

### 1. FULL
Processes all matches in the database.
```bash
RUN_TYPE=FULL
```

### 2. INCREMENTAL
Processes only matches not yet loaded to bronze layer.
```bash
RUN_TYPE=INCREMENTAL
```

### 3. DATE_RANGE
Processes matches within a specific date range.
```bash
RUN_TYPE=DATE_RANGE
START_DATE=2025-09-01
END_DATE=2025-10-01
```

### 4. ORCHESTRATED
Processes matches from specific scrape run(s).
```bash
RUN_TYPE=ORCHESTRATED
SCRAPE_RUN_ID=705f0470
```

## Configuration

### Environment Variables

**Required:**
- `RUN_TYPE`: Processing mode (FULL, INCREMENTAL, DATE_RANGE, ORCHESTRATED)

**Optional (retrieved from SSM or env):**
- `RUN_ID`: Unique identifier for this run (auto-generated if not provided)
- `START_DATE`: Start date for DATE_RANGE mode (YYYY-MM-DD)
- `END_DATE`: End date for DATE_RANGE mode (YYYY-MM-DD)
- `SCRAPE_RUN_ID`: Scrape run ID(s) for ORCHESTRATED mode
- `MAX_KEYS_PER_UNIT`: Maximum S3 keys per processing chunk (default: 10)
- `MAX_WORKERS`: Number of parallel workers (default: CPU count)
- `CHUNK_SIZE`: Batch size for database operations (default: 1000)
- `BATCH_SIZE`: Batch size for transformations (default: 100)

**Database (from SSM):**
- `/ws-analytics/database/host`: Redshift endpoint
- `/ws-analytics/database/username`: Database user
- `/ws-analytics/database/password`: Database password
- `/ws-analytics/database/database`: Database name

**S3 (from SSM):**
- `/ws-analytics/s3/analytics/name`: S3 bucket for raw data

## Usage

### Local Development

1. Set up environment variables:
```bash
export RUN_TYPE=DATE_RANGE
export START_DATE=2025-09-01
export END_DATE=2025-10-01
export AWS_REGION=us-east-1
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the service:
```bash
python src/main.py
```

### AWS Batch Deployment

The service is containerized and runs as AWS Batch jobs:

1. Build Docker image:
```bash
docker build -t ws_preprocessing:latest .
```

2. Push to ECR:
```bash
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com
docker tag ws_preprocessing:latest <account-id>.dkr.ecr.us-east-1.amazonaws.com/ws_preprocessing:latest
docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/ws_preprocessing:latest
```

3. Submit job via AWS Batch:
```bash
aws batch submit-job \
  --job-name ws-transform-$(date +%s) \
  --job-definition ws-analytics-preprocessing \
  --job-queue ws-analytics-job-queue \
  --container-overrides '{
    "environment": [
      {"name": "RUN_TYPE", "value": "DATE_RANGE"},
      {"name": "START_DATE", "value": "2025-09-01"},
      {"name": "END_DATE", "value": "2025-10-01"}
    ]
  }'
```

## Database Schema

### Input: scrape_runs Table
Contains metadata about scraped matches:
- `scrape_run_id`: Unique scrape execution ID
- `match_id`: WhoScored match identifier
- `season_id`: Season identifier
- `date`: Match date (YYYYMM format)
- `tournaments`: Tournament name

### Output: Bronze Layer Tables

**event_main**: Core event data
- `match_id`, `event_id`, `minute`, `second`
- `x`, `y`: Event coordinates
- `team_id`, `player_id`
- `type_id`, `outcome_type`

**event_qualifiers**: Event attributes
- `event_id`, `qualifier_id`
- `value`: Qualifier value

**event_types**: Event type definitions
- `type_id`, `display_name`

**satisfied_event_types**: Type relationships
- `type_id`, `satisfied_type_id`

**bronze_runs**: Processing metadata
- `bronze_run_id`, `scrape_run_id`
- `is_loaded`, `created_ts`

## Performance Tuning

### Parallel Processing
Adjust worker count based on available CPU:
```bash
MAX_WORKERS=8  # Use 8 parallel workers
```

### Chunk Size
Control memory usage by adjusting chunk size:
```bash
MAX_KEYS_PER_UNIT=5  # Process 5 matches per chunk (lower = less memory)
```

### Batch Operations
Tune database write batch sizes:
```bash
CHUNK_SIZE=500   # Write 500 rows at a time
BATCH_SIZE=50    # Transform 50 records per batch
```

## Infrastructure

The service infrastructure is managed by Terraform in `terraform/`:

- **Job Definition**: AWS Batch job configuration
- **IAM Roles**: Permissions for S3, SSM, and CloudWatch
- **ECR Repository**: Docker image storage
- **SSM Parameters**: Configuration storage

Deploy infrastructure:
```bash
cd terraform
terraform init
terraform apply
```
