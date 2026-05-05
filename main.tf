# 1. The Provider Block
# This tells Terraform: "Hey, I want to work with AWS in this specific region."

provider "aws" {
  region = "ap-south-1"
}
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# 2. Local Variables
# We use 'locals' to avoid typing the same names over and over. 
# It's like creating a variable in Python.
locals {
  project_name = "clickstream-project"
  env          = "dev"
}

# 3. Creating the S3 Buckets
# This is where your data will live.
resource "aws_s3_bucket" "bronze_layer" {
  bucket = "${local.project_name}-bronze-${local.env}"
}
resource "aws_s3_bucket" "silver_layer" {
  bucket = "${local.project_name}-silver-${local.env}"
}
resource "aws_s3_bucket" "gold" {
  bucket = "${local.project_name}-gold-${local.env}"
}
resource "aws_s3_bucket" "dlq_layer" {
  bucket = "${local.project_name}-dlq-${local.env}"
}

# 4. The $3 Budget Alarm
# This doesn't stop the services, but it screams at you via email if you spend money.
resource "aws_budgets_budget" "project_budget" {
  name              = "clickstream-monthly-budget"
  budget_type       = "COST"
  limit_amount      = "3"      # Your $3 limit
  limit_unit        = "USD"
  time_unit         = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80          # Alert at 80% ($2.40)
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = ["kulbhushansuresh99@gmail.com"] # CHANGE THIS
  }
}

# 5. The Ingestion Point (Kinesis Data Stream)
resource "aws_kinesis_stream" "main_stream" {
  name             = "${local.project_name}-stream"
  # shard_count      = 1
  retention_period = 24

  # This is the "On-Demand" mode which is cheaper for small tests
  # because you only pay for what you use, not a flat hourly rate for shards.
  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }
}

# 6. The IAM Role (The "Hat")
resource "aws_iam_role" "firehose_role" {
  name = "firehose_delivery_role"

  # The "Trust Policy" - Allows Firehose to use this role
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "firehose.amazonaws.com"
        }
      }
    ]
  })
}

# 7. The Permission Policy (The "Rules")
resource "aws_iam_role_policy" "firehose_s3_policy" {
  name = "firehose_s3_policy"
  role = aws_iam_role.firehose_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Allow Firehose to write to your buckets
        Action = [
          "s3:PutObject",
          "s3:GetBucketLocation",
          "s3:ListBucket"
        ]
        Effect   = "Allow"
        Resource = [
          aws_s3_bucket.bronze_layer.arn,
          "${aws_s3_bucket.bronze_layer.arn}/*",
          aws_s3_bucket.dlq_layer.arn,
          "${aws_s3_bucket.dlq_layer.arn}/*"
        ]
      },
      {
        # Allow Firehose to read from your Kinesis stream
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream"
        ]
        Effect   = "Allow"
        Resource = [aws_kinesis_stream.main_stream.arn]
      }
    ]
  })
}

# 8. The Delivery Truck (Kinesis Firehose)
resource "aws_kinesis_firehose_delivery_stream" "main_firehose" {
  name        = "${local.project_name}-firehose"
  destination = "extended_s3"

  # Step 1: Source (Kinesis Data Stream)
  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.main_stream.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }

  # Step 2: Destination & Logic (S3 + Lambda + Parquet)
  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose_role.arn
    bucket_arn = aws_s3_bucket.bronze_layer.arn

    prefix              = "data/"
    error_output_prefix = "errors/"
    # Buffer settings MUST be inside this block
    buffering_size     = 64   # 1MB
    buffering_interval = 60  # 60 seconds

    # A. In-Line Transformation Lambda (Schema Check & Cleansing)
    processing_configuration {
      enabled = "true"
      processors {
        type = "Lambda"
        parameters {
          parameter_name  = "LambdaArn"
          parameter_value = "${aws_lambda_function.transform_lambda.arn}:$LATEST"
        }
      }
    }

    # B. Native Format Conversion (JSON to Parquet)
    data_format_conversion_configuration {
      input_format_configuration {
        deserializer {
          open_x_json_ser_de {}
        }
      }
      output_format_configuration {
        serializer {
          parquet_ser_de {}
        }
      }
      schema_configuration {
        database_name = aws_glue_catalog_database.clickstream_db.name
        table_name    = aws_glue_catalog_table.clickstream_table.name
        role_arn      = aws_iam_role.firehose_role.arn
      }
    }
  }
}

resource "aws_glue_catalog_database" "clickstream_db" {
  name = "clickstream_db"
}

resource "aws_glue_catalog_table" "clickstream_table" {
  name          = "raw_clicks"
  database_name = aws_glue_catalog_database.clickstream_db.name

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.bronze_layer.bucket}/data/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # Column 1: Metadata
    # Unified Columns (Superset of Clicks and CDC)
    columns {
      name = "metadata"
      type = "struct<source:string,table:string,op:string,ts:double>"
    }
    columns {
      name = "data"
      type = "struct<user_id:int,name:string,email:string,country:string,url:string,event_id:string,platform:string,duration_sec:int>"
    }
  }
}


# A. Zip the Python code
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "transform_lambda.py"
  output_path = "lambda_function_payload.zip"
}

# B. Create the Lambda Function
resource "aws_lambda_function" "transform_lambda" {
  filename      = "lambda_function_payload.zip"
  function_name = "${local.project_name}-transform"
  role          = aws_iam_role.lambda_role.arn
  handler       = "transform_lambda.lambda_handler"
  runtime       = "python3.9"

  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
}

# C. IAM Role for Lambda
resource "aws_iam_role" "lambda_role" {
  name = "lambda_execution_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

# D. Update Firehose Role to include Lambda and Glue permissions
resource "aws_iam_role_policy" "firehose_extended_policy" {
  name = "firehose_extended_policy"
  role = aws_iam_role.firehose_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction", "lambda:GetFunctionConfiguration"]
        Resource = ["${aws_lambda_function.transform_lambda.arn}:*"]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetTable", "glue:GetDatabase"]
        Resource = [
          aws_glue_catalog_database.clickstream_db.arn,
          aws_glue_catalog_table.clickstream_table.arn,
          # Glue also requires access to the 'catalog' itself to look up the DB
          "arn:aws:glue:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:catalog"
        ]
      }
    ]
  })
}

# 11. The Notification Topic (SNS)
resource "aws_sns_topic" "pipeline_alerts" {
  name = "${local.project_name}-alerts"
}

# 12. Your Email Subscription
resource "aws_sns_topic_subscription" "email_target" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = "kulbhushansuresh99@gmail.com" # Your email
}

# 13. Enable EventBridge on the S3 Bucket
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket      = aws_s3_bucket.bronze_layer.id
  eventbridge = true
}

# 14. EventBridge Rule (The Listener)
resource "aws_cloudwatch_event_rule" "s3_upload_rule" {
  name        = "capture-s3-bronze-upload"
  description = "Triggers when Firehose drops a Parquet file in Bronze"

  # This JSON looks for "Object Created" events in your specific bucket
  event_pattern = jsonencode({
    "source": ["aws.s3"],
    "detail-type": ["Object Created"],
    "detail": {
      "bucket": {
        "name": [aws_s3_bucket.bronze_layer.id]
      }
    }
  })
}

# 15. IAM Role for EventBridge to trigger Step Functions
resource "aws_iam_role" "eventbridge_role" {
  name = "eventbridge_sfn_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "events.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_policy" {
  name = "eventbridge_policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action   = "states:StartExecution"
      Effect   = "Allow"
      Resource = ["*"] # We will narrow this down once the SFN is built
    }]
  })
}

# 16. IAM Role for Step Functions
resource "aws_iam_role" "sfn_role" {
  name = "${local.project_name}-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })
}

# 17. Permissions for the Conductor
resource "aws_iam_role_policy" "sfn_policy" {
  name = "sfn_policy"
  role = aws_iam_role.sfn_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"]
        Effect = "Allow"
        Resource = ["*"] # Narrow this down to your Glue Job ARN later
      },
      {
        Action = ["sns:Publish"]
        Effect = "Allow"
        Resource = [aws_sns_topic.pipeline_alerts.arn]
      }
    ]
  })
}

# 18. The Step Function (State Machine)
resource "aws_sfn_state_machine" "pipeline_orchestrator" {
  name     = "${local.project_name}-orchestrator"
  role_arn = aws_iam_role.sfn_role.arn

  definition = jsonencode({
    StartAt = "RunETLJob",
    States = {
      RunETLJob = {
        Type     = "Task",
        Resource = "arn:aws:states:::glue:startJobRun.sync", # .sync waits for completion
        Parameters = {
          JobName = "${local.project_name}-etl-job"
        },
        Next = "SuccessState",
        Catch = [{
            ErrorEquals = ["Glue.ConcurrentRunsExceededException"]
            Next        = "IgnoreConcurrency"
          },
          {
          ErrorEquals = ["States.ALL"],
          Next        = "NotifyFailure"
        }]
      },
      IgnoreConcurrency = {
        Type    = "Succeed"
        Comment = "Another job is already running and will process the files."
      },
      NotifyFailure = {
        Type     = "Task",
        Resource = "arn:aws:states:::sns:publish",
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_alerts.arn,
          Message  = "Alert: The Clickstream Glue ETL Job failed for project ${local.project_name}."
        },
        End = true
      },
      SuccessState = {
        Type = "Pass",
        End  = true
      }
    }
  })
}
# 19. Connect EventBridge to Step Function
resource "aws_cloudwatch_event_target" "sfn_target" {
  rule      = aws_cloudwatch_event_rule.s3_upload_rule.name
  target_id = "TriggerStepFunction"
  arn       = aws_sfn_state_machine.pipeline_orchestrator.arn
  role_arn  = aws_iam_role.eventbridge_role.arn # From Part 3
}

# 20. IAM Role for Glue
resource "aws_iam_role" "glue_role" {
  name = "${local.project_name}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

# 21. Permissions for Glue (S3 and Glue Catalog)
resource "aws_iam_role_policy" "glue_policy" {
  name = "glue_policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Block 1: S3 Permissions
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
        Effect   = "Allow"
        Resource = [
          aws_s3_bucket.bronze_layer.arn,
          "${aws_s3_bucket.bronze_layer.arn}/*",
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*"
        ]
      },
      {
        # Block 2: Glue Catalog Permissions
        # WE MOVED THE PERMISSION HERE
        Action   = [
          "glue:GetDatabase", 
          "glue:GetTable", 
          "glue:GetPartitions", 
          "glue:UpdateTable"
        ]
        Effect   = "Allow"
        Resource = ["*"]
      }
    ]
  })
}

# 22. The Glue Job
resource "aws_glue_job" "clickstream_etl" {
  name     = "${local.project_name}-etl-job"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://${aws_s3_bucket.bronze_layer.bucket}/scripts/etl_script.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option" = "job-bookmark-enable" # Saves money!
    "--enable-metrics"      = "true"
  }

  # Smallest capacity for testing
  max_capacity = 2.0 
  timeout      = 10 # Stop after 10 mins if stuck
}

# 23. IAM Role for Redshift
resource "aws_iam_role" "redshift_role" {
  name = "${local.project_name}-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
    }]
  })
}

# 24. Permission to read Gold S3 and use Spectrum
resource "aws_iam_role_policy" "redshift_s3_policy" {
  name = "redshift_s3_policy"
  role = aws_iam_role.redshift_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["s3:Get*", "s3:List*"]
        Effect   = "Allow"
        Resource = [
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.gold.arn}/*"
        ]
      },
      {
        Action   = ["glue:Get*", "athena:*"]
        Effect   = "Allow"
        Resource = ["*"]
      }
    ]
  })
}

# 25 
#  Redshift Serverless Namespace (The Data Container)
resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${local.project_name}-namespace"
  db_name             = "dev"
  admin_username      = "admin"
  admin_user_password = "YourStrongPassword123!" 
  iam_roles           = [aws_iam_role.redshift_role.arn]
}

# 24. Redshift Serverless Workgroup (The Compute Power)
resource "aws_redshiftserverless_workgroup" "main" {
  workgroup_name = "${local.project_name}-workgroup"
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  base_capacity  = 8 # Minimum capacity (RPU)
  
  # This makes it accessible from the internet for your tests
  publicly_accessible = true 
}