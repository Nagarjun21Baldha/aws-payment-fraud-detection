# =============================================================
# Outputs — values printed after terraform apply
# Use these to configure your pipeline scripts
# =============================================================

output "s3_bucket_name" {
  description = "S3 data lake bucket name"
  value       = aws_s3_bucket.data_lake.bucket
}

output "s3_bucket_arn" {
  description = "S3 data lake bucket ARN"
  value       = aws_s3_bucket.data_lake.arn
}

output "kinesis_stream_name" {
  description = "Kinesis stream name — use in payment_producer.py"
  value       = aws_kinesis_stream.payment_stream.name
}

output "kinesis_stream_arn" {
  description = "Kinesis stream ARN"
  value       = aws_kinesis_stream.payment_stream.arn
}

output "glue_database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.payments.name
}

output "sns_topic_arn" {
  description = "SNS fraud alerts topic ARN"
  value       = aws_sns_topic.fraud_alerts.arn
}

output "lambda_function_name" {
  description = "Lambda fraud alerter function name"
  value       = aws_lambda_function.fraud_alerter.function_name
}

output "emr_service_role_arn" {
  description = "EMR service role ARN — use when creating EMR cluster"
  value       = aws_iam_role.emr_service_role.arn
}

output "emr_instance_profile" {
  description = "EMR EC2 instance profile — use when creating EMR cluster"
  value       = aws_iam_instance_profile.emr_profile.name
}
