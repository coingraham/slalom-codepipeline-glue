
# output "codepipeline_name" {
#   value = aws_codepipeline.codepipeline_glue_dp_jobs.name
# }

output "JE_datapipeline_name" {
  value = aws_datapipeline_pipeline.JE.name
}

output "JE_datapipeline_id" {
  value = aws_datapipeline_pipeline.JE.id
}