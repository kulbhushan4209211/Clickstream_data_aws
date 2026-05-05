AWS Serverless Clickstream & CDC Pipeline
A production-grade, event-driven data pipeline that ingests real-time clickstream events and database changes (CDC), processing them through a serverless Medallion Architecture.

Architecture
Ingestion: Kinesis Data Streams & Firehose (JSON to Parquet conversion).

Storage: S3 Bronze (Raw) and S3 Gold (Curated) layers.

Orchestration: EventBridge + Step Functions (State Machine).

Processing: AWS Glue (PySpark) with Job Bookmarking.

Warehouse: Redshift Serverless.

 Key Technical Features
Sessionization: Solves the "Gaps and Islands" problem using Spark Window functions to group user activities into sessions.

SCD Type 1: Processes CDC logs to maintain the latest state of user profiles.

Infrastructure as Code (IaC): Entire stack is fully automated via Terraform.

Error Resilience: Implements a "Quiet Exit" strategy in Step Functions to handle concurrency limits without alert fatigue.
