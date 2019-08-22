############
# Resources
############

# Glue job wrk_001_update_ctrl_system
resource "aws_glue_job" "wrk_001_update_ctrl_system" {
  name     = "wrk_001_update_ctrl_system_${var.environment}"
  role_arn = "${aws_iam_role.glue_role.arn}"
  max_capacity = 2

  default_arguments = {
    "--TempDir" = "s3://aws-glue-temporary-313644149065-us-east-1/wrk_001_update_ctrl_system"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--job-language" = "python"
  }

  command {
    script_location = "s3://${data.aws_s3_bucket.project_system_bucket.id}/Glue/Scripts/wrk_001_update_ctrl_system.py"
  }
}

resource "null_resource" "wrk_001_update_ctrl_system" {
  triggers = {
    # glue_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    glue_id = "STATIC"
  }

  # Manually set python 3 and glue 1.0
  provisioner "local-exec" {
    command = <<EOF
aws glue update-job \
--job-name wrk_001_update_ctrl_system_${var.environment} \
--job-update 'Command={ScriptLocation=s3://${data.aws_s3_bucket.project_system_bucket.id}/Glue/Scripts/wrk_001_update_ctrl_system.py,PythonVersion=2,Name=glueetl},GlueVersion=1.0,MaxCapacity=2,Role=${aws_iam_role.glue_role.arn},DefaultArguments={--TempDir=s3://aws-glue-temporary-313644149065-us-east-1/wrk_001_update_ctrl_system,--job-bookmark-option=job-bookmark-disable,--job-language=python}' \
EOF
  }
}

# Glue job wrk_01_ppm_ods_clear_glrev_mthly
resource "aws_glue_job" "wrk_01_ppm_ods_clear_glrev_mthly" {
  name     = "wrk_01_ppm_ods_clear_glrev_mthly_${var.environment}"
  role_arn = "${aws_iam_role.glue_role.arn}"
  max_capacity = 2

  default_arguments = {
    "--TempDir" = "s3://aws-glue-temporary-313644149065-us-east-1/wrk_01_ppm_ods_clear_glrev_mthly"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--job-language" = "python"
  }

  command {
    script_location = "s3://${data.aws_s3_bucket.project_system_bucket.id}/Glue/Scripts/wrk_01_ppm_ods_clear_glrev_mthly.py"
  }
}

resource "null_resource" "wrk_01_ppm_ods_clear_glrev_mthly" {
  triggers = {
    # glue_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    glue_id = "STATIC"
  }

  # Manually set python 3 and glue 1.0
  provisioner "local-exec" {
    command = <<EOF
aws glue update-job \
--job-name wrk_01_ppm_ods_clear_glrev_mthly_${var.environment} \
--job-update 'Command={ScriptLocation=s3://${data.aws_s3_bucket.project_system_bucket.id}/Glue/Scripts/wrk_01_ppm_ods_clear_glrev_mthly.py,PythonVersion=2,Name=glueetl},GlueVersion=1.0,MaxCapacity=2,Role=${aws_iam_role.glue_role.arn},DefaultArguments={--TempDir=s3://aws-glue-temporary-313644149065-us-east-1/wrk_01_ppm_ods_clear_glrev_mthly,--job-bookmark-option=job-bookmark-disable,--job-language=python}' \
EOF
  }
}

# Glue job wrk_03_audit_monthly_staged_data
resource "aws_glue_job" "wrk_03_audit_monthly_staged_data" {
  name     = "wrk_03_audit_monthly_staged_data_${var.environment}"
  role_arn = "${aws_iam_role.glue_role.arn}"
  max_capacity = 2

  default_arguments = {
    "--TempDir" = "s3://aws-glue-temporary-313644149065-us-east-1/wrk_03_audit_monthly_staged_data"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--job-language" = "python"
  }

  command {
    script_location = "s3://${data.aws_s3_bucket.project_system_bucket.id}/Glue/Scripts/wrk_03_audit_monthly_staged_data.py"
  }
}

resource "null_resource" "wrk_03_audit_monthly_staged_data" {
  triggers = {
    # glue_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    glue_id = "STATIC"
  }

  # Manually set python 3 and glue 1.0
  provisioner "local-exec" {
    command = <<EOF
aws glue update-job \
--job-name wrk_03_audit_monthly_staged_data_${var.environment} \
--job-update 'Command={ScriptLocation=s3://${data.aws_s3_bucket.project_system_bucket.id}/Glue/Scripts/wrk_03_audit_monthly_staged_data.py,PythonVersion=2,Name=glueetl},GlueVersion=1.0,MaxCapacity=2,Role=${aws_iam_role.glue_role.arn},DefaultArguments={--TempDir=s3://aws-glue-temporary-313644149065-us-east-1/wrk_03_audit_monthly_staged_data,--job-bookmark-option=job-bookmark-disable,--job-language=python}' \
EOF
  }
}

# Glue job wrk_04_ppm_clear_ods_mdm
resource "aws_glue_job" "wrk_04_ppm_clear_ods_mdm" {
  name     = "wrk_04_ppm_clear_ods_mdm_${var.environment}"
  role_arn = "${aws_iam_role.glue_role.arn}"
  max_capacity = 2

  default_arguments = {
    "--TempDir" = "s3://aws-glue-temporary-313644149065-us-east-1/wrk_04_ppm_clear_ods_mdm"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--job-language" = "python"
  }

  command {
    script_location = "s3://${data.aws_s3_bucket.project_system_bucket.id}/Glue/Scripts/wrk_04_ppm_clear_ods_mdm.py"
  }
}

resource "null_resource" "wrk_04_ppm_clear_ods_mdm" {
  triggers = {
    # glue_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    glue_id = "STATIC"
  }

  # Manually set python 3 and glue 1.0
  provisioner "local-exec" {
    command = <<EOF
aws glue update-job \
--job-name wrk_04_ppm_clear_ods_mdm_${var.environment} \
--job-update 'Command={ScriptLocation=s3://${data.aws_s3_bucket.project_system_bucket.id}/Glue/Scripts/wrk_04_ppm_clear_ods_mdm.py,PythonVersion=2,Name=glueetl},GlueVersion=1.0,MaxCapacity=2,Role=${aws_iam_role.glue_role.arn},DefaultArguments={--TempDir=s3://aws-glue-temporary-313644149065-us-east-1/wrk_04_ppm_clear_ods_mdm,--job-bookmark-option=job-bookmark-disable,--job-language=python}' \
EOF
  }
}

# Glue job wrk_ppm_hotfix_controller
resource "aws_glue_job" "wrk_ppm_hotfix_controller" {
  name     = "wrk_ppm_hotfix_controller_${var.environment}"
  role_arn = "${aws_iam_role.glue_role.arn}"
  max_capacity = 2

  default_arguments = {
    "--TempDir" = "s3://aws-glue-temporary-313644149065-us-east-1/wrk_ppm_hotfix_controller"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--job-language" = "python"
    "--config_path" = "Hotfix/config.yml"
    "--s3_src_bucket" = "${data.aws_s3_bucket.project_ingest_bucket.id}"
  }

  command {
    script_location = "s3://${data.aws_s3_bucket.project_system_bucket.id}/Glue/Scripts/hotfix_controller_glue.py"
  }
}

resource "aws_glue_catalog_database" "ods_database" {
  name = "wrk-${var.project}-ods-${var.environment}"
}

resource "aws_glue_crawler" "ods_transform" {
  database_name = "${aws_glue_catalog_database.ods_database.name}"
  name          = "wrk-${var.project}-ods-transform-${var.environment}"
  role          = "${aws_iam_role.glue_role.arn}"

  s3_target {
    # path = "s3://${aws_s3_bucket.project_datalake_bucket.bucket}/Transformed"
    path = "s3://wrk-datalake-ppm-dev/Transformed"
    exclusions = [
      "**/_SUCCESS"
    ]
  }

}

resource "aws_glue_crawler" "ctrl_tables" {
  database_name = "${aws_glue_catalog_database.ods_database.name}"
  name          = "wrk-${var.project}-ctrl-tables-${var.environment}"
  role          = "${aws_iam_role.glue_role.arn}"

  s3_target {
    # path = "s3://${aws_s3_bucket.project_datalake_bucket.bucket}/Logs/AuroraDB/PPMODS"
    path = "s3://wrk-system-ppm-dev/Logs/AuroraDB/PPMODS"
    exclusions = [
      "**/_SUCCESS"
    ]
  }
}