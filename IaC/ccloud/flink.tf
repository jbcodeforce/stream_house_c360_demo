# ------------------------------------------------------
# Flink Compute Pool
# Cloud/region mirror the Kafka cluster (see kafka.tf) so Flink runs
# alongside the cluster in the same environment.
# ------------------------------------------------------

resource "confluent_flink_compute_pool" "pool" {
  display_name = "${var.prefix}-flink-pool"
  cloud        = var.cloud_provider
  region       = var.aws_region_primary
  max_cfu      = var.flink_max_cfu

  environment {
    id = confluent_environment.env.id
  }

  depends_on = [
    confluent_kafka_cluster.kcl
  ]
}
