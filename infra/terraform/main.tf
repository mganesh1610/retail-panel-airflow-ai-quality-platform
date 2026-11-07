terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
  }
}

variable "environment" {
  type    = string
  default = "production"
}

resource "aws_s3_bucket" "retail_panel_landing" {
  bucket = "retail-panel-bronze-landing-${var.environment}"
}

resource "aws_s3_bucket_versioning" "retail_panel_versioning" {
  bucket = aws_s3_bucket.retail_panel_landing.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "kubernetes_namespace" "data_quality_platform" {
  metadata {
    name = "retail-data-quality"
    labels = {
      managed_by  = "terraform"
      environment = var.environment
    }
  }
}
