# =============================================================
# Payment Fraud Detection — AWS Infrastructure
# =============================================================
# Defines all AWS resources needed for the pipeline.
# Run: terraform init → terraform plan → terraform apply
# =============================================================

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.5.0"
}

provider "aws" {
  region = var.aws_region
}
data "archive_file" "lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/lambda_function.zip"
  source {
    content  = "def handler(event, context): print(event)"
    filename = "lambda_function.py"
  }
}









# =============================================================
# S3 BUCKETS — data lake bronze/silver/fraud layers
# =============================================================
resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-data-lake-${var.environment}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "data-lake"
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "bronze-lifecycle"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    # move bronze data to cheaper storage after 90 days
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    # move to glacier after 1 year
    transition {
      days          = 365
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "silver-lifecycle"
    status = "Enabled"

    filter {
      prefix = "silver/"
    }

    transition {
      days          = 180
      storage_class = "STANDARD_IA"
    }
  }
}

# S3 folders (prefixes)
resource "aws_s3_object" "bronze_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "bronze/payments/"
  content = ""
}

resource "aws_s3_object" "silver_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "silver/payments/"
  content = ""
}

resource "aws_s3_object" "fraud_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "silver/fraud/"
  content = ""
}

resource "aws_s3_object" "checkpoints_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "checkpoints/"
  content = ""
}

resource "aws_s3_object" "scripts_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "scripts/"
  content = ""
}


# =============================================================
# KINESIS DATA STREAM — real-time payment event ingestion
# =============================================================
resource "aws_kinesis_stream" "payment_stream" {
  name             = "${var.project_name}-payment-stream-${var.environment}"
  shard_count      = var.kinesis_shard_count
  retention_period = 24                         # hours

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}


# =============================================================
# IAM ROLE — EMR service role
# =============================================================
resource "aws_iam_role" "emr_service_role" {
  name = "${var.project_name}-emr-service-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

# IAM role for EMR EC2 instances
resource "aws_iam_role" "emr_ec2_role" {
  name = "${var.project_name}-emr-ec2-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_ec2_profile" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_iam_instance_profile" "emr_profile" {
  name = "${var.project_name}-emr-profile-${var.environment}"
  role = aws_iam_role.emr_ec2_role.name
}

# Custom policy — allow EMR to access S3 and Kinesis
resource "aws_iam_policy" "pipeline_policy" {
  name = "${var.project_name}-pipeline-policy-${var.environment}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:ListBucket", "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords", "kinesis:GetShardIterator",
          "kinesis:DescribeStream", "kinesis:ListStreams",
          "kinesis:ListShards"
        ]
        Resource = aws_kinesis_stream.payment_stream.arn
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetTable", "glue:GetPartitions",
          "glue:CreateTable", "glue:UpdateTable"
        ]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.fraud_alerts.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "pipeline_policy_emr" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = aws_iam_policy.pipeline_policy.arn
}


# =============================================================
# GLUE DATABASE + CRAWLER — metadata catalog
# =============================================================
resource "aws_glue_catalog_database" "payments" {
  name = "${var.project_name}_payments_${var.environment}"
}

resource "aws_glue_crawler" "silver_crawler" {
  name          = "${var.project_name}-silver-crawler-${var.environment}"
  role          = aws_iam_role.emr_ec2_role.arn
  database_name = aws_glue_catalog_database.payments.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/silver/payments/"
  }

  schedule = "cron(0 * * * ? *)"       # run every hour

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}


# =============================================================
# SNS TOPIC — real-time fraud alerts
# =============================================================
resource "aws_sns_topic" "fraud_alerts" {
  name = "${var.project_name}-fraud-alerts-${var.environment}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_sns_topic_subscription" "fraud_email" {
  topic_arn = aws_sns_topic.fraud_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}


# =============================================================
# LAMBDA — fraud alert trigger
# Triggered when new files land in S3 fraud prefix
# =============================================================
resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}-lambda-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_pipeline" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.pipeline_policy.arn
}

resource "aws_lambda_function" "fraud_alerter" {
  function_name = "${var.project_name}-fraud-alerter-${var.environment}"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.handler"
  runtime       = "python3.11"
  timeout       = 30

  filename         = "${path.module}/lambda_function.zip"

  environment {
    variables = {
      SNS_TOPIC_ARN = aws_sns_topic.fraud_alerts.arn
      ENVIRONMENT   = var.environment
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# trigger Lambda when fraud files land in S3
resource "aws_s3_bucket_notification" "fraud_trigger" {
  bucket = aws_s3_bucket.data_lake.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.fraud_alerter.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "silver/fraud/"
  }

  depends_on = [aws_lambda_permission.s3_invoke]
}

resource "aws_lambda_permission" "s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.fraud_alerter.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.data_lake.arn
}
