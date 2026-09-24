################################################################################
# Data Sources
################################################################################

# Discover available Availability Zones in the selected AWS region
data "aws_availability_zones" "available" {
  state = "available"
}

# Fetch existing VPC by ID (if provided) or by default/Name tag
data "aws_vpc" "existing" {
  id      = var.vpc_id != null ? var.vpc_id : null
  default = var.vpc_id == null && var.vpc_name_tag == null ? true : null

  dynamic "filter" {
    for_each = var.vpc_id == null && var.vpc_name_tag != null ? [var.vpc_name_tag] : []
    content {
      name   = "tag:Name"
      values = [filter.value]
    }
  }
}

# Fetch existing subnets in the target VPC if subnet_ids is not explicitly provided
data "aws_subnets" "existing" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.existing.id]
  }
}

# Fetch Confluent Cloud Connect egress IPs for managed connectors in the target AWS region
data "confluent_ip_addresses" "connectors" {
  filter {
    clouds        = ["AWS"]
    regions       = [coalesce(var.confluent_region, var.aws_region_primary)]
    services      = ["CONNECT", "KAFKA"]
    address_types = ["EGRESS"]
  }
}
