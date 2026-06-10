# Designing a Data Engineering Pipeline in AWS for Processing Stock Market Data

Serverless AWS data engineering pipeline for ingesting, validating and enriching stock market data using a Bronze-Silver-Gold medallion architecture.

## Overview

This repository contains the source code developed for the Final Degree Project **Designing a Data Engineering Pipeline in AWS for Processing Stock Market Data**.

The project implements a serverless data engineering pipeline on AWS for the automated processing of daily stock market data obtained from the Alpha Vantage API. The solution is designed around a Data Lake stored in Amazon S3 and organized using a medallion architecture with three progressive layers: **Bronze**, **Silver** and **Gold**.

The main goal of the project is not to predict stock prices or provide investment recommendations, but to design, implement and validate a reliable, traceable, idempotent and low-cost cloud-based data pipeline suitable for analytical use cases.

## Main Features

- Automated ingestion of daily OHLCV stock market data from Alpha Vantage.
- Data Lake organization in Amazon S3 using Bronze, Silver and Gold layers.
- Serverless processing with AWS Lambda.
- Scheduled and event-driven execution using Amazon EventBridge and Amazon S3 events.
- Monitoring and execution tracking through Amazon CloudWatch.
- Data quality controls over structure, types, null values, duplicates and consistency.
- Idempotent processing using deterministic output keys.
- Traceability between layers through execution metadata and source references.
- Analytical enrichment with financial indicators.
- Final dataset generation for descriptive analysis and visualization in Power BI.
- Cost-conscious design based on managed AWS services and controlled cloud resource consumption.

## Architecture

The pipeline follows a medallion architecture:

| Layer | Purpose |
|---|---|
| **Bronze** | Stores the original raw responses from the Alpha Vantage API together with execution metadata. |
| **Silver** | Transforms, cleans, types and validates the semi-structured data from Bronze. |
| **Gold** | Enriches the validated records with analytical metrics and financial indicators. |

The architecture uses:

- **Amazon S3** as the central Data Lake.
- **AWS Lambda** for ingestion, transformation and analytical enrichment.
- **Amazon EventBridge** for scheduled execution.
- **Amazon S3 events** to automate the transition between layers.
- **Amazon CloudWatch** for logs, monitoring and operational observability.
- **AWS IAM** to control permissions between services.

## Architecture Diagram

![AWS data pipeline architecture](docs/images/aws-pipeline-architecture.png)

## Repository Structure

```text
src/
├── lambdas/
│   ├── ingest_bronze/
│   │   └── handler.py
│   ├── transform_silver/
│   │   └── handler.py
│   └── transform_gold/
│       └── handler.py
│
├── core/
│   ├── api_client.py
│   ├── config.py
│   ├── indicators.py
│   ├── logging.py
│   └── s3_io.py
│
scripts/
└── Auxiliary scripts for validation, backfill, consolidation or dataset inspection
```

## Main Components

### Bronze Ingestion

The Bronze ingestion Lambda obtains daily stock market data from Alpha Vantage and stores the original API response in Amazon S3. This layer preserves the raw data and includes metadata that supports traceability and later reprocessing.

### Silver Transformation

The Silver transformation Lambda converts the semi-structured API response into clean, typed and validated records. This layer applies quality controls and separates valid processing outputs from invalid or unexpected responses.

### Gold Enrichment

The Gold transformation Lambda enriches the validated records with analytical metrics and financial indicators, producing a dataset prepared for descriptive analysis and visualization.

The Gold layer includes, among others:

- Daily, 5-session and 20-session returns.
- Rolling volatility metrics.
- ATR.
- MACD.
- RSI.
- OBV.
- Volume z-score.
- Distance from moving averages.

## Technologies Used

- Python
- AWS Lambda
- Amazon S3
- Amazon EventBridge
- Amazon CloudWatch
- AWS IAM
- boto3
- pandas
- pyarrow
- jq
- Alpha Vantage API
- Power BI

## Data Source

The project uses daily stock market data obtained from the Alpha Vantage API. The processed data corresponds to a selected set of technology-related tickers and is used for educational and descriptive analytical purposes.

## Dataset and Analysis

The final dataset generated in the Gold layer is designed for analytical consumption. It includes cleaned OHLCV data, calculated financial indicators and execution metadata that supports traceability across the pipeline.

The dataset was used to build a Power BI dashboard for exploring:

- Historical price evolution.
- Returns by ticker.
- Risk-return relationships.
- Abnormal volume sessions.
- Normalized performance comparisons.
- Aggregated market behavior across selected assets.

## Security Notice

This repository does **not** include:

- Alpha Vantage API keys.
- AWS access keys or secret keys.
- Real environment variable values.
- Sensitive AWS account information.
- Local virtual environments.
- Private configuration files.

Secrets are managed outside the source code through environment variables and excluded files.

A typical local configuration can be documented through an `.env.example` file using placeholder values only:

```env
ALPHA_VANTAGE_API_KEY=your_api_key_here
AWS_REGION=eu-west-1
S3_BUCKET_NAME=your_bucket_name_here
```

## Cost and Scope

The pipeline was designed for an academic project with a controlled data volume and a low-cost serverless architecture. AWS services were used under a limited execution scope, with attention to Free Tier usage, Amazon S3 requests, Lambda executions and CloudWatch logs.

This project is not intended to represent a large-scale production-grade data platform. Possible future improvements include infrastructure as code, a data catalog, Athena integration, additional data sources, larger historical coverage, multi-region strategies and predictive models built on top of the Gold layer.

## Academic Context

This project was developed as part of a Final Degree Project focused on:

- Cloud data engineering.
- Serverless architectures.
- Data Lake design.
- Medallion architecture.
- Data quality.
- Traceability.
- Idempotent processing.
- Analytical dataset generation.
- Cloud cost awareness.

## Disclaimer

The generated dataset and dashboard are intended for descriptive and educational analysis only. They should not be interpreted as financial advice, investment recommendations or trading signals.

## Author

**Ilarion Tsekot**

GitHub: [@ilariontsekot](https://github.com/ilariontsekot)

Repository: [Designing-a-data-engineering-pipeline-in-AWS-for-processing-stock-market-data](https://github.com/ilariontsekot/Designing-a-data-engineering-pipeline-in-AWS-for-processing-stock-market-data)
