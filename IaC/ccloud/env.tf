# Confluent Cloud Environment and Environment Manager Service Account
# This is the base environment for j9r Flink applications

resource "confluent_environment" "env" {
  display_name = "${var.prefix}-env"

  stream_governance {
    package = "ESSENTIALS"
  }
}
