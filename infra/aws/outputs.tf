# Names only, except pipeline_role_arn (it contains the account ID): copy it into your own
# ~/.aws/config, never into a repository file.

output "region" {
  value = var.region
}

output "bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "athena_workgroup" {
  value = aws_athena_workgroup.proof.name
}

output "glue_database" {
  value = aws_glue_catalog_database.proof.name
}

output "glue_table" {
  value = aws_glue_catalog_table.cleaned_orders.name
}

output "pipeline_role_name" {
  value = aws_iam_role.pipeline.name
}

output "pipeline_role_arn" {
  value = aws_iam_role.pipeline.arn
}
