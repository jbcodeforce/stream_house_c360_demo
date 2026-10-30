# ------------------------------------------------------
# Kafka topics owned by Terraform
# ------------------------------------------------------
# agent_conversations — history of user interactions with the real-time
# context engine. The chat backend produces two Avro records per turn
# (type="request" / type="response", keyed by session_id, linked by turn_id);
# a downstream Flink agent consumes this stream for intent classification.
#
# The topic lifecycle is managed here; the value schema is registered by the
# backend at runtime (apps/backend/chat/conversation_producer.py).
# ------------------------------------------------------

resource "confluent_kafka_topic" "agent_conversations" {
  kafka_cluster {
    id = confluent_kafka_cluster.kcl.id
  }
  topic_name       = var.agent_conversations_topic
  partitions_count = var.agent_conversations_partitions
  rest_endpoint    = confluent_kafka_cluster.kcl.rest_endpoint

  credentials {
    key    = confluent_api_key.kcl-kafka-api-key.id
    secret = confluent_api_key.kcl-kafka-api-key.secret
  }

  depends_on = [
    confluent_api_key.kcl-kafka-api-key
  ]
}

output "agent_conversations_topic" {
  description = "Name of the agent-conversations history topic"
  value       = confluent_kafka_topic.agent_conversations.topic_name
}
