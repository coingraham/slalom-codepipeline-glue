# Setup Variables
region = "us-east-1"
environment = "dev"
project = "ppm"
role_arn = "arn:aws:iam::232943211143:role/_WRKSTS_techdeveloper"

# Datapipeline Variables
emr_subnet = "subnet-e386e6cc"
sqoop_emr_subnet = "subnet-e386e6cc"
emr_master_sg = "sg-096205745f33db931"
emr_slave_sg = "sg-0a82b9f5780744421"
emr_service_sg = "sg-045304204b8597b10"

# RDS Variables
aurora_instance_count = "1"
aurora_subnet_group = "default-vpc-31e6ea49"
aurora_security_groups = ["sg-06853dc1e9ae4045d"]
aurora_instance_class = "db.r5.large"
aurora_cluster_parameter_group_name = "default.aurora-postgresql10"
aurora_backup_retention_window = 7



