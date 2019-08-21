############
# Resources
############

resource "aws_ssm_parameter" "ppm_mstr_database_username" {
  name        = "/${var.environment}/ppm_mstr_database/username"
  description = "The ppm master data tables database SQL Server username"
  type        = "SecureString"
  value       = "stub"

  # Ignore changes to the stub value.  We'll update the password manually.
  lifecycle {
    ignore_changes = [
      value
    ]
  }

  tags = {
    environment = "${var.environment}"
  }
}

resource "aws_ssm_parameter" "ppm_mstr_database_password" {
  name        = "/${var.environment}/ppm_mstr_database/password"
  description = "The ppm master data tables database SQL Server password"
  type        = "SecureString"
  value       = "stub"

  # Ignore changes to the stub value.  We'll update the password manually.
  lifecycle {
    ignore_changes = [
      value
    ]
  }

  tags = {
    environment = "${var.environment}"
  }
}

resource "aws_ssm_parameter" "ppm_s3_database_username" {
  name        = "/${var.environment}/ppm_s3_database/username"
  description = "The ppm s3 data tables database DB2 username"
  type        = "SecureString"
  value       = "stub"

  # Ignore changes to the stub value.  We'll update the password manually.
  lifecycle {
    ignore_changes = [
      value
    ]
  }

  tags = {
    environment = "${var.environment}"
  }
}

resource "aws_ssm_parameter" "ppm_s3_database_password" {
  name        = "/${var.environment}/ppm_s3_database/password"
  description = "The ppm s3 data tables database DB2 password"
  type        = "SecureString"
  value       = "stub"

  # Ignore changes to the stub value.  We'll update the password manually.
  lifecycle {
    ignore_changes = [
      value
    ]
  }

  tags = {
    environment = "${var.environment}"
  }
}

resource "aws_ssm_parameter" "ppm_ud_database_username" {
  name        = "/${var.environment}/ppm_ud_database/username"
  description = "The ppm ud data tables database SQL Server username"
  type        = "SecureString"
  value       = "stub"

  # Ignore changes to the stub value.  We'll update the password manually.
  lifecycle {
    ignore_changes = [
      value
    ]
  }

  tags = {
    environment = "${var.environment}"
  }
}

resource "aws_ssm_parameter" "ppm_ud_database_password" {
  name        = "/${var.environment}/ppm_ud_database/password"
  description = "The ppm ud data tables database SQL Server password"
  type        = "SecureString"
  value       = "stub"

  # Ignore changes to the stub value.  We'll update the password manually.
  lifecycle {
    ignore_changes = [
      value
    ]
  }

  tags = {
    environment = "${var.environment}"
  }
}

resource "aws_ssm_parameter" "ppm_init_master_database_username" {
  name        = "/${var.environment}/ppm_init_master_database/username"
  description = "The ppm init_master data tables database Oracle username"
  type        = "SecureString"
  value       = "stub"

  # Ignore changes to the stub value.  We'll update the password manually.
  lifecycle {
    ignore_changes = [
      value
    ]
  }

  tags = {
    environment = "${var.environment}"
  }
}

resource "aws_ssm_parameter" "ppm_init_master_database_password" {
  name        = "/${var.environment}/ppm_init_master_database/password"
  description = "The ppm init_master data tables database SQL Server password"
  type        = "SecureString"
  value       = "stub"

  # Ignore changes to the stub value.  We'll update the password manually.
  lifecycle {
    ignore_changes = [
      value
    ]
  }

  tags = {
    environment = "${var.environment}"
  }
}

resource "aws_ssm_parameter" "ppm_init_xref_database_username" {
  name        = "/${var.environment}/ppm_init_xref_database/username"
  description = "The ppm init xref data tables database Oracle username"
  type        = "SecureString"
  value       = "stub"

  # Ignore changes to the stub value.  We'll update the password manually.
  lifecycle {
    ignore_changes = [
      value
    ]
  }

  tags = {
    environment = "${var.environment}"
  }
}

resource "aws_ssm_parameter" "ppm_init_xref_database_password" {
  name        = "/${var.environment}/ppm_init_xref_database/password"
  description = "The ppm init xref data tables database Oracle password"
  type        = "SecureString"
  value       = "stub"

  # Ignore changes to the stub value.  We'll update the password manually.
  lifecycle {
    ignore_changes = [
      value
    ]
  }

  tags = {
    environment = "${var.environment}"
  }
}

resource "aws_ssm_parameter" "ppm_aurora_database_password" {
  name        = "/${var.environment}/ppm_aurora_database/password"
  description = "The ppm aurora database Oracle password"
  type        = "SecureString"
  value       = "stub"

  # Ignore changes to the stub value.  We'll update the password manually.
  lifecycle {
    ignore_changes = [
      value
    ]
  }

  tags = {
    environment = "${var.environment}"
  }
}

