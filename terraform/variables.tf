# =============================================================
# Variables — Payment Fraud Detection Pipeline
# =============================================================
# Override defaults by creating a terraform.tfvars file:
#
#   project_name    = "payment-fraud"
#   environment     = "dev"
#   aws_region      = "us-east-1"
#   alert_email     = "your@email.com"
# =============================================================

variable "project_name" {
  description = "Project name — used as prefix for all AWS resources"
  type        = string
  default     = "payment-fraud"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "aws_region" {
  description = "AWS region to deploy all resources"
  type        = string
  default     = "us-east-1"
}

variable "kinesis_shard_count" {
  description = "Number of Kinesis shards (1 shard = 1MB/s ingest, 2MB/s read)"
  type        = number
  default     = 1

  validation {
    condition     = var.kinesis_shard_count >= 1 && var.kinesis_shard_count <= 10
    error_message = "Shard count must be between 1 and 10."
  }
}

variable "alert_email" {
  description = "Email address to receive fraud alerts via SNS"
  type        = string
  default     = "n.nagarjun.b@gmail.com"
}

variable "redshift_node_type" {
  description = "Redshift node type"
  type        = string
  default     = "dc2.large"
}

variable "redshift_cluster_size" {
  description = "Number of Redshift nodes"
  type        = number
  default     = 1
}
