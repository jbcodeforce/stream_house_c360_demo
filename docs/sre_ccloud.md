# Defining Confluent Cloud Resources with the Consol

## SRE's tasks Walking Through

### Confluent Console Walk Through

We suppose the user has OrganizatonAdmin role to be able to create environment. 

Pre-requisites:

* Login to the Confluent Cloud console

#### Create Confluent Cloud Environment

1. From the home page, go to the environment page, and click on the `Add Cloud Environment` button on the top right part of the page.
1. Enter name and select one of the governance package. As the demonstration scope is not about governance, use Essential

    ![](./images/ccloud/cc-env-1.png)

#### Create Confluent Cloud Kafka Cluster

Next step is to create a KafKa Cluster
1. Enter name, and Cluster Type, which for demonstration will be standard.

    ![](./images/ccloud/cc-kafka-1.png)
1. Select a Cloud Provider and a region, select a 99.9% SLA
1. Launch the cluster using the right column 'Launch Cluster button`
1. Create API Key and Secrets

*As part of the essential governance, a schema registry is created once the Kafka Cluster is launched.*

#### Create Confluent Cloud Flink Compute Pool