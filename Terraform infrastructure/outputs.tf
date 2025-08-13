output "bucket_name" {
  value = aws_s3_bucket.data.id
}
output "glue_job_name" {
  value = aws_glue_job.etl.name
}
output "athena_workgroup" {
  value = aws_athena_workgroup.wg.name
}
output "glue_db" {
  value = aws_glue_catalog_database.db.name
}
