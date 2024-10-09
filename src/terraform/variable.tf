# variable.tf
variable "instance_type" {
  type        = string
  description = "EC2 Instance Type"
  default     = "t2.micro"
}

variable "tag" {
  type        = string
  description = "A tag for the EC2 instance"
}

variable "location" {
  type        = string
  description = "The project region"
  default     = "us-east-1"
}

variable "bucket_name" {
  type        = string
  description = "The name of the S3 bucket"
}

variable "access_key" {
  type        = string
  description = "AWS access key"
}

variable "secret_key" {
  type        = string
  description = "AWS secret key"
}

variable "crawler_name" {
  type        = string
  description = "The name of the Glue crawler"
}

variable "crawler_description" {
  type        = string
  description = "Description of the Glue crawler"
}

variable "database_name" {
  type        = string
  description = "The name of the Glue database"
}

variable "database_description" {
  type        = string
  description = "Description of the Glue database"
}