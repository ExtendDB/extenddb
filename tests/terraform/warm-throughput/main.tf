# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
#
# The configuration from hashicorp/terraform-provider-aws#44344 (the table
# whose apply hung against an emulator that reported no WarmThroughput), plus
# a second table with a global secondary index, against the ExtendDB under
# test. Since provider 6.13.0 the aws_dynamodb_table resource waits for
# WarmThroughput.Status to read ACTIVE after every table and index create or
# update; run.sh asserts the apply completes and a second plan has no changes.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.70.0"
    }
  }
}

variable "endpoint" {
  description = "The ExtendDB endpoint, for example https://127.0.0.1:18443"
  type        = string
}

variable "access_key" {
  type      = string
  sensitive = true
}

variable "secret_key" {
  type      = string
  sensitive = true
}

variable "insecure" {
  description = "Accept the server's self-signed TLS certificate"
  type        = bool
  default     = true
}

provider "aws" {
  region     = "us-east-1"
  access_key = var.access_key
  secret_key = var.secret_key
  insecure   = var.insecure

  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    dynamodb = var.endpoint
  }
}

resource "aws_dynamodb_table" "example" {
  name         = "example"
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "PK"
  range_key = "SK"

  attribute {
    name = "PK"
    type = "S"
  }

  attribute {
    name = "SK"
    type = "S"
  }
}

resource "aws_dynamodb_table" "with_index" {
  name         = "example-with-index"
  billing_mode = "PAY_PER_REQUEST"

  hash_key = "PK"

  attribute {
    name = "PK"
    type = "S"
  }

  attribute {
    name = "G"
    type = "S"
  }

  global_secondary_index {
    name            = "gsi"
    hash_key        = "G"
    projection_type = "ALL"
  }
}
