# One-off AWS proof (Step 17): Sydney only, run and destroyed by the owner.
#
# Versions are pinned to releases at least 14 days old when chosen (2026-10-25): Terraform
# 1.16.5 (2026-10-02), hashicorp/aws 6.68.0 (2026-10-07), hashicorp/random 3.9.1 (2026-09-11).
# Credentials come from the shell (AWS_PROFILE, short-lived `aws login`), never from a file.

terraform {
  required_version = "~> 1.16.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 6.68.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "= 3.9.1"
    }
  }
}

provider "aws" {
  region = var.region

  # The teardown check (make aws-verify-teardown) searches for this tag.
  default_tags {
    tags = {
      project    = "deliverypulse-aws-proof"
      managed_by = "terraform"
    }
  }
}
