############
# Resources
############

resource "aws_datapipeline_pipeline" "sqoop_init_master" {
    name        = "ppm_sqoop_init_master_tf_${var.environment}"
}

data "template_file" "sqoop_init_master" {
  template = "${file("${path.module}/datapipelines/ppm_sqoop_init_master_definition.json")}"
  vars = {
    ppmrps_jdbc = "jdbc:oracle:thin"
    ppmrps_connection = "@ews-pgh1-ppmp4:5574"
    ppmrps_hostname = "ppmrps.ews_pgh1_ppmp4.meadwestvaco.com"
    ppmrps_username = aws_ssm_parameter.ppm_init_master_database_username.value
    ppmrps_password = aws_ssm_parameter.ppm_init_master_database_password.value
    ppmrps_database = "PPMRPS"
    # ppmrps_destination = "s3://wrk-ingest-${var.project}-dev"
    ppmrps_destination = "s3://wrk-ingest-poc-dev"
    ppmrps_folder = "Landing/Historical"
    ppmrps_number = "1"
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_emr_subnet = var.sqoop_emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "1"
  }
}

resource "null_resource" "update_sqoop_init_master_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-0355124ORILIUY9OKYX"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.sqoop_init_master.id} \
--pipeline-definition '${data.template_file.sqoop_init_master.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "sqoop_init_xref" {
    name        = "ppm_sqoop_init_xref_tf_${var.environment}"
}

data "template_file" "sqoop_init_xref" {
  template = "${file("${path.module}/datapipelines/ppm_sqoop_init_xref_definition.json")}"
  vars = {
    ppmrps_jdbc = "jdbc:oracle:thin"
    ppmrps_connection = "@ews-pgh1-ppmp3:5573"
    ppmrps_hostname = "ppmods.ews_pgh1_ppmp3.meadwestvaco.com"
    ppmrps_username = aws_ssm_parameter.ppm_init_xref_database_username.value
    ppmrps_password = aws_ssm_parameter.ppm_init_xref_database_password.value
    ppmrps_database = "PPMODS"
    # ppmrps_destination = "s3://wrk-ingest-${var.project}-dev"
    ppmrps_destination = "s3://wrk-ingest-poc-dev"
    ppmrps_folder = "Landing/Historical"
    ppmrps_number = "1"
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_emr_subnet = var.sqoop_emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "1"
  }
}

resource "null_resource" "update_sqoop_init_xref_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-046864136OVCRQLEAI0U"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.sqoop_init_xref.id} \
--pipeline-definition '${data.template_file.sqoop_init_xref.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "sqoop_landing_mstr1" {
    name        = "ppm_sqoop_landing_mstr1_tf_${var.environment}"
}

data "template_file" "sqoop_landing_mstr1" {
  template = "${file("${path.module}/datapipelines/ppm_sqoop_landing_mstr1_definition.json")}"
  vars = {
    ppmrps_jdbc = "jdbc:sqlserver"
    ppmrps_connection = "10.1.149.47:3221"
    ppmrps_hostname = "MDMSD"
    ppmrps_username = aws_ssm_parameter.ppm_mstr_database_username.value
    ppmrps_password = aws_ssm_parameter.ppm_mstr_database_password.value
    ppmrps_database = "dbo"
    # ppmrps_destination = "s3://wrk-ingest-${var.project}-dev"
    ppmrps_destination = "s3://wrk-ingest-poc-dev"
    ppmrps_folder = "Landing/Mstr"
    ppmrps_number = "1"
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_emr_subnet = var.sqoop_emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "1"
  }
}

resource "null_resource" "update_sqoop_landing_mstr1_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-07215721W4ATUGA5LR90"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.sqoop_landing_mstr1.id} \
--pipeline-definition '${data.template_file.sqoop_landing_mstr1.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "sqoop_landing_mstr2" {
    name        = "ppm_sqoop_landing_mstr2_tf_${var.environment}"
}

data "template_file" "sqoop_landing_mstr2" {
  template = "${file("${path.module}/datapipelines/ppm_sqoop_landing_mstr2_definition.json")}"
  vars = {
    ppmrps_jdbc = "jdbc:sqlserver"
    ppmrps_connection = "10.1.149.47:3221"
    ppmrps_hostname = "MDMSD"
    ppmrps_username = aws_ssm_parameter.ppm_mstr_database_username.value
    ppmrps_password = aws_ssm_parameter.ppm_mstr_database_password.value
    ppmrps_database = "dbo"
    # ppmrps_destination = "s3://wrk-ingest-${var.project}-dev"
    ppmrps_destination = "s3://wrk-ingest-poc-dev"
    ppmrps_folder = "Landing/Mstr"
    ppmrps_number = "1"
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_emr_subnet = var.sqoop_emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "1"
  }
}

resource "null_resource" "update_sqoop_landing_mstr2_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-09394738FOB8V6L6GWA"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.sqoop_landing_mstr2.id} \
--pipeline-definition '${data.template_file.sqoop_landing_mstr2.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "sqoop_landing_mstr3" {
    name        = "ppm_sqoop_landing_mstr3_tf_${var.environment}"
}

data "template_file" "sqoop_landing_mstr3" {
  template = "${file("${path.module}/datapipelines/ppm_sqoop_landing_mstr3_definition.json")}"
  vars = {
    ppmrps_jdbc = "jdbc:sqlserver"
    ppmrps_connection = "10.1.149.47:3221"
    ppmrps_hostname = "MDMSD"
    ppmrps_username = aws_ssm_parameter.ppm_mstr_database_username.value
    ppmrps_password = aws_ssm_parameter.ppm_mstr_database_password.value
    ppmrps_database = "dbo"
    # ppmrps_destination = "s3://wrk-ingest-${var.project}-dev"
    ppmrps_destination = "s3://wrk-ingest-poc-dev"
    ppmrps_folder = "Landing/Mstr"
    ppmrps_number = "1"
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_emr_subnet = var.sqoop_emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "1"
  }
}

resource "null_resource" "update_sqoop_landing_mstr3_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-0032992YD2CK16SKRSK"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.sqoop_landing_mstr3.id} \
--pipeline-definition '${data.template_file.sqoop_landing_mstr3.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "sqoop_landing_mstr4" {
    name        = "ppm_sqoop_landing_mstr4_tf_${var.environment}"
}

data "template_file" "sqoop_landing_mstr4" {
  template = "${file("${path.module}/datapipelines/ppm_sqoop_landing_mstr4_definition.json")}"
  vars = {
    ppmrps_jdbc = "jdbc:sqlserver"
    ppmrps_connection = "10.1.149.47:3221"
    ppmrps_hostname = "MDMSD"
    ppmrps_username = aws_ssm_parameter.ppm_mstr_database_username.value
    ppmrps_password = aws_ssm_parameter.ppm_mstr_database_password.value
    ppmrps_database = "dbo"
    # ppmrps_destination = "s3://wrk-ingest-${var.project}-dev"
    ppmrps_destination = "s3://wrk-ingest-poc-dev"
    ppmrps_folder = "Landing/Mstr"
    ppmrps_number = "1"
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_emr_subnet = var.sqoop_emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "1"
  }
}

resource "null_resource" "update_sqoop_landing_mstr4_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-074196015CC50A1M32P5"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.sqoop_landing_mstr4.id} \
--pipeline-definition '${data.template_file.sqoop_landing_mstr4.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "sqoop_s3" {
    name        = "ppm_sqoop_s3_tf_${var.environment}"
}

data "template_file" "sqoop_s3" {
  template = "${file("${path.module}/datapipelines/ppm_sqoop_s3_definition.json")}"
  vars = {
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_sqoop_script = "wkf_S3_DB2TABLE_FMS_S3_FLATFILE.sh"
    ppm_database_username = aws_ssm_parameter.ppm_s3_database_username.value
    ppm_database_password = aws_ssm_parameter.ppm_s3_database_password.value
    # ppm_destination_bucket = "s3://wrk-ingest-${var.project}-dev"
    ppm_destination_bucket = "wrk-ingest-poc-dev"
    ppm_filter_YEAR = "2019"
    ppm_filter_PERIOD = "07"
    ppm_emr_subnet = var.sqoop_emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "1"
  }
}

resource "null_resource" "update_sqoop_s3_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-06790522VBAI6UOBKEDM"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.sqoop_s3.id} \
--pipeline-definition '${data.template_file.sqoop_s3.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "sqoop_udus" {
    name        = "ppm_sqoop_udus_tf_${var.environment}"
}

data "template_file" "sqoop_udus" {
  template = "${file("${path.module}/datapipelines/ppm_sqoop_ud_definition.json")}"
  vars = {
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_sqoop_script = "wkf_UDUS_MSSQL_FMS_FLATFILE.sh"
    ppm_emr_subnet = var.sqoop_emr_subnet
    ppm_database_username = aws_ssm_parameter.ppm_ud_database_username.value
    ppm_database_password = aws_ssm_parameter.ppm_ud_database_password.value
    # ppm_destination_bucket = "s3://wrk-ingest-${var.project}-dev"
    ppm_destination_bucket = "wrk-ingest-poc-dev"
    ppm_datetime = "1900-01-01 00:00:00.000"
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "1"
  }
}

resource "null_resource" "update_sqoop_udus_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    # datapipeline_id = "df-07315523KL8R8Z3QFUP3"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.sqoop_udus.id} \
--pipeline-definition '${data.template_file.sqoop_udus.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "JE" {
    name        = "ppm_controller_JE_tf_${var.environment}"
}

data "template_file" "JE" {
  template = "${file("${path.module}/datapipelines/ppm_controller_JE_values.json")}"
  vars = {
    ppm_system = "JE"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_JE_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-04856052TJYGEFTH6D82"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.JE.id} \
--pipeline-definition file://datapipelines/ppm_controller_JE_definition.json \
--parameter-values-uri '${data.template_file.JE.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "MQ" {
    name        = "ppm_controller_MQ_tf_${var.environment}"
}

data "template_file" "MQ" {
  template = "${file("${path.module}/datapipelines/ppm_controller_MQ_values.json")}"
  vars = {
    ppm_system = "MQ"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_MQ_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-09398392563B96BNF8JR"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.MQ.id} \
--pipeline-definition file://datapipelines/ppm_controller_MQ_definition.json \
--parameter-values-uri '${data.template_file.MQ.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "MDMS_Curate1" {
    name        = "ppm_controller_MDMS_Curate1_tf_${var.environment}"
}

data "template_file" "MDMS_Curate1" {
  template = "${file("${path.module}/datapipelines/ppm_controller_MDMS_Curate1_values.json")}"
  vars = {
    ppm_system = "MDMS"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    # ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_ingest_bucket = "wrk-ingest-poc-dev"
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_MDMS_Curate1_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-0964971350WZ99DNE0T7"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.MDMS_Curate1.id} \
--pipeline-definition file://datapipelines/ppm_controller_MDMS_Curate1_definition.json \
--parameter-values-uri '${data.template_file.MDMS_Curate1.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "MDMS_Curate2" {
    name        = "ppm_controller_MDMS_Curate2_tf_${var.environment}"
}

data "template_file" "MDMS_Curate2" {
  template = "${file("${path.module}/datapipelines/ppm_controller_MDMS_Curate2_values.json")}"
  vars = {
    ppm_system = "MDMS"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    # ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_ingest_bucket = "wrk-ingest-poc-dev"
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_MDMS_Curate2_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-09936103K5UIWTZKQEJH"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.MDMS_Curate2.id} \
--pipeline-definition file://datapipelines/ppm_controller_MDMS_Curate2_definition.json \
--parameter-values-uri '${data.template_file.MDMS_Curate2.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "MDMS_Curate3" {
    name        = "ppm_controller_MDMS_Curate3_tf_${var.environment}"
}

data "template_file" "MDMS_Curate3" {
  template = "${file("${path.module}/datapipelines/ppm_controller_MDMS_Curate3_values.json")}"
  vars = {
    ppm_system = "MDMS"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    # ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_ingest_bucket = "wrk-ingest-poc-dev"
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_MDMS_Curate3_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-0689253314UJLOUFQ9H"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.MDMS_Curate3.id} \
--pipeline-definition file://datapipelines/ppm_controller_MDMS_Curate3_definition.json \
--parameter-values-uri '${data.template_file.MDMS_Curate3.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "MDMS_XRef1" {
    name        = "ppm_controller_MDMS_XRef1_tf_${var.environment}"
}

data "template_file" "MDMS_XRef1" {
  template = "${file("${path.module}/datapipelines/ppm_controller_MDMS_XRef1_values.json")}"
  vars = {
    ppm_system = "MDMS"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    # ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_ingest_bucket = "wrk-ingest-poc-dev"
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_MDMS_XRef1_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-08654641BZZHHNS6AFDA"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.MDMS_XRef1.id} \
--pipeline-definition file://datapipelines/ppm_controller_MDMS_XRef1_definition.json \
--parameter-values-uri '${data.template_file.MDMS_XRef1.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "NV" {
    name        = "ppm_controller_NV_tf_${var.environment}"
}

data "template_file" "NV" {
  template = "${file("${path.module}/datapipelines/ppm_controller_NV_values.json")}"
  vars = {
    ppm_system = "NV"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_NV_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-053770515ZVS8AXE8A5X"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.NV.id} \
--pipeline-definition file://datapipelines/ppm_controller_NV_definition.json \
--parameter-values-uri '${data.template_file.NV.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "PE" {
    name        = "ppm_controller_PE_tf_${var.environment}"
}

data "template_file" "PE" {
  template = "${file("${path.module}/datapipelines/ppm_controller_PE_values.json")}"
  vars = {
    ppm_system = "PE"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_PE_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-0434973348SFOVW2GPB2"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.PE.id} \
--pipeline-definition file://datapipelines/ppm_controller_PE_definition.json \
--parameter-values-uri '${data.template_file.PE.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "UDUS" {
    name        = "ppm_controller_UDUS_tf_${var.environment}"
}

data "template_file" "UDUS" {
  template = "${file("${path.module}/datapipelines/ppm_controller_UD_values.json")}"
  vars = {
    ppm_system = "UDUS"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_UDUS_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-03661043UMIPAHWDXZWZ"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.UDUS.id} \
--pipeline-definition file://datapipelines/ppm_controller_UD_definition.json \
--parameter-values-uri '${data.template_file.UDUS.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "BP" {
    name        = "ppm_controller_BP_tf_${var.environment}"
}

data "template_file" "BP" {
  template = "${file("${path.module}/datapipelines/ppm_controller_BP_values.json")}"
  vars = {
    ppm_system = "BP"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_BP_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-09219783RGETPX1NVIOU"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.BP.id} \
--pipeline-definition file://datapipelines/ppm_controller_BP_definition.json \
--parameter-values-uri '${data.template_file.BP.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "S2" {
    name        = "ppm_controller_S2_tf_${var.environment}"
}

data "template_file" "S2" {
  template = "${file("${path.module}/datapipelines/ppm_controller_S2_values.json")}"
  vars = {
    ppm_system = "S2"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_S2_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-05390893BFJ2PK2P7FN1"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.S2.id} \
--pipeline-definition file://datapipelines/ppm_controller_S2_definition.json \
--parameter-values-uri '${data.template_file.S2.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "S3" {
    name        = "ppm_controller_S3_tf_${var.environment}"
}

data "template_file" "S3" {
  template = "${file("${path.module}/datapipelines/ppm_controller_S3_values.json")}"
  vars = {
    ppm_system = "S3"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    # ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_ingest_bucket = "wrk-ingest-poc-dev"
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_S3_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-06475403L4VZP7ZY3D0H"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.S3.id} \
--pipeline-definition file://datapipelines/ppm_controller_S3_definition.json \
--parameter-values-uri '${data.template_file.S3.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "J2" {
    name        = "ppm_controller_J2_tf_${var.environment}"
}

data "template_file" "J2" {
  template = "${file("${path.module}/datapipelines/ppm_controller_J2_values.json")}"
  vars = {
    ppm_system = "J2"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_J2_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-020571213W5BI0F408IM"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.J2.id} \
--pipeline-definition file://datapipelines/ppm_controller_J2_definition.json \
--parameter-values-uri '${data.template_file.J2.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "J4" {
    name        = "ppm_controller_J4_tf_${var.environment}"
}

data "template_file" "J4" {
  template = "${file("${path.module}/datapipelines/ppm_controller_J4_values.json")}"
  vars = {
    ppm_system = "J4"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_J4_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-01523221IVZEYHGLHSQS"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.J4.id} \
--pipeline-definition file://datapipelines/ppm_controller_J4_definition.json \
--parameter-values-uri '${data.template_file.J4.rendered}' \
EOF
  }
}

resource "aws_datapipeline_pipeline" "JD" {
    name        = "ppm_controller_JD_tf_${var.environment}"
}

data "template_file" "JD" {
  template = "${file("${path.module}/datapipelines/ppm_controller_JD_values.json")}"
  vars = {
    ppm_system = "JD"
    ppm_emr_instance_type = "m5.2xlarge"
    ppm_emr_terminate_time = "1"
    ppm_emr_core_instance_count = "3"
    ppm_environment = var.environment
    # ppm_ingest_bucket = aws_s3_bucket.project_ingest_bucket.id
    # ppm_system_bucket = aws_s3_bucket.project_system_bucket.id
    # ppm_datalake_bucket = aws_s3_bucket.project_datalake_bucket.id
    ppm_ingest_bucket = data.aws_s3_bucket.project_ingest_bucket.id
    ppm_system_bucket = data.aws_s3_bucket.project_system_bucket.id
    ppm_datalake_bucket = data.aws_s3_bucket.project_datalake_bucket.id
    ppm_emr_subnet = var.emr_subnet
    ppm_emr_master_sg = var.emr_master_sg
    ppm_emr_slave_sg = var.emr_slave_sg
    ppm_emr_service_sg = var.emr_service_sg
    ppm_emr_role = aws_iam_role.datapipeline_emr_role.name
    ppm_emr_resource_role = aws_iam_role.datapipeline_emr_resource_role.name
    ppm_emr_region = data.aws_region.current.name
  }
}

resource "null_resource" "update_JD_datapipeline_definition" {
  triggers = {
    # Uncomment the below if you want this to run every time
    # datapipeline_id = "${uuid()}"

    # Uncomment the below and the definition update won't run every time
    datapipeline_id = "df-07268721L3230LDB69H0"
  }

  provisioner "local-exec" {
    command = <<EOF
aws datapipeline put-pipeline-definition \
--pipeline-id ${aws_datapipeline_pipeline.JD.id} \
--pipeline-definition file://datapipelines/ppm_controller_JD_definition.json \
--parameter-values-uri '${data.template_file.JD.rendered}' \
EOF
  }
}


