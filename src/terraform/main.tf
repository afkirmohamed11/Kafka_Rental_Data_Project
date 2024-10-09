# main.tf
# Configure Terraform with required providers and version
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
  required_version = ">= 0.12"
}

# Define the AWS provider with access keys and region
provider "aws" {
  region     = var.location
  access_key = var.access_key
  secret_key = var.secret_key
}

# EC2 Instance Resource
resource "aws_instance" "ec2_example" {
  ami           = "ami-0ebfd941bbafe70c6"
  instance_type = var.instance_type
  user_data     = <<-EOF
    #!/bin/bash
    sudo yum install java-17-amazon-corretto -y
    wget https://archive.apache.org/dist/kafka/3.6.1/kafka_2.12-3.6.1.tgz
    tar -xvf kafka_2.12-3.6.1.tgz
  EOF
  tags = {
    Name = var.tag
  }
}

# S3 Bucket Resource
resource "aws_s3_bucket" "kafka_bucket" {
  bucket = var.bucket_name
  tags = {
    Name        = var.tag
    Environment = "Dev"
  }
}

# Glue Crawler Resource
resource "aws_glue_crawler" "kafka_crawler" {
  database_name = aws_glue_catalog_database.kafka_database.name
  name          = var.crawler_name
  role          = aws_iam_role.glue_crawler_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.kafka_bucket.bucket}"
  }

  description = var.crawler_description
}


# Glue Database Resource
resource "aws_glue_catalog_database" "kafka_database" {
  name        = var.database_name
  description = var.database_description
}

# IAM Role for Glue Crawler
resource "aws_iam_role" "glue_crawler_role" {
  name = "glue_crawler_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# Attach AdministratorAccess policy to the IAM role
resource "aws_iam_role_policy_attachment" "admin_policy_attachment" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}