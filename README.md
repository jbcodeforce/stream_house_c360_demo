# Streamhouse Customer 360


The purpose of this repo is to implement a Customer 360 streaming data pipeline integrating operational databases (PostgreSQL, DB2) with event streaming, Change Data Capture (CDC), and modern lakehouse analytical sinks.

## Infrastructure as code

### Pre-requisites

* Get Terraform
* Get aws CLI
* Login to AWS console, search for VPC to use
* Run: `terraform init` under IaC folder

### RDS Postgresql

The database has three main tables:

* Accounts
* Customers
* Transactions

### DB2 Community Edition EC2

### Private network

