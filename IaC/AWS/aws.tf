################################################################################
# 1. Networking Layer (DB Subnet Group within Existing VPC)
################################################################################

# DB Subnet Group: Bundles subnets from the existing VPC for RDS
resource "aws_db_subnet_group" "rds" {
  name        = "${var.project_name}-${var.environment}-db-subnet-group"
  description = "Database subnet group using subnets from existing VPC ${data.aws_vpc.existing.id}"
  subnet_ids  = var.subnet_ids != null ? var.subnet_ids : data.aws_subnets.existing.ids

  tags = {
    Name        = "${var.project_name}-${var.environment}-db-subnet-group"
    Environment = var.environment
  }
}

################################################################################
# 2. Security Layer (Security Groups, Encryption Key, Secrets Manager)
################################################################################

# Security Group: Restricts traffic to PostgreSQL port 5432 within the existing VPC
resource "aws_security_group" "rds_sg" {
  name        = "${var.project_name}-${var.environment}-rds-sg"
  description = "Controls inbound and outbound traffic for Postgres RDS in ${data.aws_vpc.existing.id}"
  vpc_id      = data.aws_vpc.existing.id

  # Ingress rule: Allow Postgres TCP port 5432 from VPC CIDR and any extra allowed CIDRs
  ingress {
    description = "Allow inbound PostgreSQL traffic from VPC and authorized networks"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = length(var.db_allowed_cidr_blocks) > 0 ? var.db_allowed_cidr_blocks : [data.aws_vpc.existing.cidr_block]
  }

  # Egress rule: Default outbound traffic allowance
  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.project_name}-${var.environment}-rds-sg"
    Environment = var.environment
  }
}

# AWS KMS Customer Managed Key for encrypting RDS storage at rest and DB secrets
resource "aws_kms_key" "rds_key" {
  description             = "KMS Key for ${var.project_name}-${var.environment} RDS storage and secrets encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = {
    Name        = "${var.project_name}-${var.environment}-rds-kms-key"
    Environment = var.environment
  }
}

resource "aws_kms_alias" "rds_key_alias" {
  name          = "alias/${var.project_name}-${var.environment}-rds-key"
  target_key_id = aws_kms_key.rds_key.key_id
}

# Random password generation to avoid hardcoded credentials
resource "random_password" "db_master_password" {
  length           = 24
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

# Store database credentials securely in AWS Secrets Manager
resource "aws_secretsmanager_secret" "db_credentials" {
  name                    = "${var.project_name}-${var.environment}-rds-credentials"
  description             = "Master credentials and connection details for Postgres RDS"
  kms_key_id              = aws_kms_key.rds_key.arn
  recovery_window_in_days = 0

  tags = {
    Name        = "${var.project_name}-${var.environment}-rds-credentials"
    Environment = var.environment
  }
}

resource "aws_secretsmanager_secret_version" "db_credentials_val" {
  secret_id = aws_secretsmanager_secret.db_credentials.id
  secret_string = jsonencode({
    engine   = "postgres"
    host     = aws_db_instance.postgres.address
    port     = aws_db_instance.postgres.port
    username = var.db_username
    password = random_password.db_master_password.result
    database = var.db_name
  })
}

################################################################################
# 3. Database Parameter and Option Configuration
################################################################################

# Custom Parameter Group for PostgreSQL 16 (enables fine-tuning, e.g. logical decoding for CDC)
resource "aws_db_parameter_group" "postgres16" {
  name        = "${var.project_name}-${var.environment}-pg16-params"
  family      = "postgres16"
  description = "Custom parameter group for PostgreSQL 16"

  # Enforce TLS/SSL connections
  parameter {
    name  = "rds.force_ssl"
    value = "1"
  }

  # Enable logical replication (useful for CDC with Debezium/Kafka/Flink)
  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  tags = {
    Name        = "${var.project_name}-${var.environment}-pg16-params"
    Environment = var.environment
  }
}

################################################################################
# 4. PostgreSQL RDS Instance
################################################################################

resource "aws_db_instance" "postgres" {
  identifier = "${var.project_name}-${var.environment}-postgres"

  # Engine configuration
  engine               = "postgres"
  engine_version       = "16.4"
  instance_class       = var.db_instance_class
  parameter_group_name = aws_db_parameter_group.postgres16.name

  # Database settings
  db_name  = var.db_name
  username = var.db_username
  password = random_password.db_master_password.result

  # Storage configuration
  allocated_storage     = var.db_allocated_storage
  max_allocated_storage = var.db_max_allocated_storage
  storage_type          = "gp3"
  storage_encrypted     = true
  kms_key_id            = aws_kms_key.rds_key.arn

  # Network & Placement
  db_subnet_group_name   = aws_db_subnet_group.rds.name
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  publicly_accessible    = false

  # High Availability & Backup
  multi_az                = false # Set to true for production high availability
  backup_retention_period = 7
  backup_window           = "03:00-04:00"
  maintenance_window      = "Mon:04:30-Mon:05:30"

  # Upgrades & Protection
  auto_minor_version_upgrade = true
  allow_major_version_upgrade = false
  deletion_protection        = false # Set to true for production workloads
  skip_final_snapshot        = true  # Set to false for production to retain final snapshot

  # Performance Insights & Monitoring
  performance_insights_enabled          = true
  performance_insights_retention_period = 7
  performance_insights_kms_key_id       = aws_kms_key.rds_key.arn
  enabled_cloudwatch_logs_exports       = ["postgresql", "upgrade"]

  tags = {
    Name        = "${var.project_name}-${var.environment}-postgres"
    Environment = var.environment
    Project     = var.project_name
  }
}
