# One private bucket, one Glue database and table, one Athena workgroup and one least-privilege
# pipeline role. Everything here is deleted by `terraform destroy` (force_destroy empties the
# bucket and the workgroup's query history).

data "aws_caller_identity" "current" {}

# The IAM role behind the owner's `aws login` session; only it may assume the pipeline role.
data "aws_iam_session_context" "current" {
  arn = data.aws_caller_identity.current.arn
}

resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  bucket_name           = "${var.name_prefix}-${random_id.suffix.hex}"
  glue_database         = replace(var.name_prefix, "-", "_")
  glue_table            = "cleaned_orders"
  account_id            = data.aws_caller_identity.current.account_id
  cleaned_order_columns = jsondecode(file("${path.module}/tables/cleaned_orders.json"))
}

# --------------------------------------------------------------------------------- bucket ---

resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# SSE-S3: no KMS key, so no KMS charges.
resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Versioning stays off (the default), so deleted objects leave no billable old versions.

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-objects"
    status = "Enabled"
    filter {}
    expiration {
      days = var.object_retention_days
    }
  }

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = var.result_retention_days
    }
  }

  # Athena may leave incomplete multipart uploads after a failed or cancelled query.
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_policy" "lake" {
  bucket = aws_s3_bucket.lake.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.lake]
}

# ---------------------------------------------------------------------------------- glue ---

resource "aws_glue_catalog_database" "proof" {
  name = local.glue_database
}

# Partition projection: Athena computes the date partitions, so no crawler (which costs money)
# and no partition repair step is needed.
resource "aws_glue_catalog_table" "cleaned_orders" {
  name          = local.glue_table
  database_name = aws_glue_catalog_database.proof.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                        = "TRUE"
    classification                  = "parquet"
    "projection.enabled"            = "true"
    "projection.date.type"          = "date"
    "projection.date.format"        = "yyyy-MM-dd"
    "projection.date.range"         = var.projection_date_range
    "projection.date.interval"      = "1"
    "projection.date.interval.unit" = "DAYS"
    "storage.location.template"     = "s3://${aws_s3_bucket.lake.bucket}/cleaned/orders/date=$${date}/"
  }

  partition_keys {
    name = "date"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/cleaned/orders/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.cleaned_order_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

# -------------------------------------------------------------------------------- athena ---

resource "aws_athena_workgroup" "proof" {
  name          = var.name_prefix
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query     = var.per_query_scan_limit_bytes

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# ------------------------------------------------------------------------- pipeline role ---

resource "aws_iam_role" "pipeline" {
  name                 = "${var.name_prefix}-pipeline"
  max_session_duration = 3600
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { AWS = data.aws_iam_session_context.current.issuer_arn }
    }]
  })
}

# The policy text lives in a template so unit tests can check it allows exactly one bucket,
# one workgroup and one Glue database and table (tests/unit/test_aws_proof.py).
resource "aws_iam_role_policy" "pipeline" {
  name = "lake-and-athena"
  role = aws_iam_role.pipeline.id
  policy = templatefile("${path.module}/policies/pipeline.json.tftpl", {
    bucket_arn        = aws_s3_bucket.lake.arn
    workgroup_arn     = aws_athena_workgroup.proof.arn
    glue_catalog_arn  = "arn:aws:glue:${var.region}:${local.account_id}:catalog"
    glue_database_arn = "arn:aws:glue:${var.region}:${local.account_id}:database/${local.glue_database}"
    glue_table_arn    = "arn:aws:glue:${var.region}:${local.account_id}:table/${local.glue_database}/${local.glue_table}"
  })
}
