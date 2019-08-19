
# output "codepipeline_name" {
#   value = aws_codepipeline.codepipeline_glue_dp_jobs.name
# }

output "sqoop_landing_mstr1_datapipeline_id" {
  value = aws_datapipeline_pipeline.sqoop_landing_mstr1.id
}

output "sqoop_landing_mstr2_datapipeline_id" {
  value = aws_datapipeline_pipeline.sqoop_landing_mstr2.id
}

output "sqoop_s3_datapipeline_id" {
  value = aws_datapipeline_pipeline.sqoop_s3.id
}

output "sqoop_udus_datapipeline_id" {
  value = aws_datapipeline_pipeline.sqoop_udus.id
}

output "BP_datapipeline_id" {
  value = aws_datapipeline_pipeline.BP.id
}

output "PE_datapipeline_id" {
  value = aws_datapipeline_pipeline.PE.id
}

output "S2_datapipeline_id" {
  value = aws_datapipeline_pipeline.S2.id
}

output "S3_datapipeline_id" {
  value = aws_datapipeline_pipeline.S3.id
}

output "JD_datapipeline_id" {
  value = aws_datapipeline_pipeline.JD.id
}

output "J2_datapipeline_id" {
  value = aws_datapipeline_pipeline.J2.id
}

output "J4_datapipeline_id" {
  value = aws_datapipeline_pipeline.J4.id
}

output "NV_datapipeline_id" {
  value = aws_datapipeline_pipeline.NV.id
}

output "MDMS_Curate1_datapipeline_id" {
  value = aws_datapipeline_pipeline.MDMS_Curate1.id
}

output "MDMS_Curate2_datapipeline_id" {
  value = aws_datapipeline_pipeline.MDMS_Curate2.id
}

output "MDMS_Curate3_datapipeline_id" {
  value = aws_datapipeline_pipeline.MDMS_Curate3.id
}

output "MDMS_XRef1_datapipeline_id" {
  value = aws_datapipeline_pipeline.MDMS_XRef1.id
}

output "UD_datapipeline_id" {
  value = aws_datapipeline_pipeline.UD.id
}

output "MQ_datapipeline_id" {
  value = aws_datapipeline_pipeline.MQ.id
}

output "JE_datapipeline_id" {
  value = aws_datapipeline_pipeline.JE.id
}

