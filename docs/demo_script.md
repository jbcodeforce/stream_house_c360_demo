# Demonstration Script

Two approaches: 
1. local database, writing directly to Kafka topics defined within Confluent Cloud Kafka Cluster
2. Run Database on AWS, with Kafka Connector defined to get data from the database to Kafka topics. This approach has cost for the services on AWS.

## Get started locally

* Set .env under the apps/backend

* Start the database server
    ```sh
    cd apps/backend
    ./start_local_pg_server.sh
    ```