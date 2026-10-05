# Defining Confluent Cloud Resources with the Terraform


Terraform is organized under the [`IaC/`](IaC/) directory as **three independent root modules** (separate local state), each file-per-concern. This split lets the core Confluent Cloud stack run locally with **no AWS credentials**; AWS and the managed connector are opt-in cost paths.

For the steps described in this note we are concern by the [`IaC/ccloud/`](IaC/ccloud/): environment, Kafka cluster, Flink compute pool, Schema Registry, service account + Kafka/SR API keys. 

### Folder Structure

IaC/ccloud/ — core Confluent Cloud stack:

- provider.tf: dropped the aws provider and the unused random provider — now requires only confluent (verified via terraform providers).
- data.tf: removed the AWS Secrets Manager read + local.rds_*; kept env_mgr.
- flink.tf: added confluent_flink_compute_pool.pool (the file was empty).
- connector.tf: deleted (moved out).
- variables.tf: dropped rds_secret_arn/connector_name/kafka_topic_prefix; added flink_max_cfu.
- outputs.tf: replaced the connector outputs with flink_compute_pool_id.
- terraform.tfvars/.example: removed the AWS/connector entries.

### Pre-requisites

* Get Terraform cli
* Have CONFLUENT_API_KEY and SECRET to manage resources
* Run: `terraform init` under IaC/ccloud folder


Run with [`scripts/tf.sh`](scripts/tf.sh). 
