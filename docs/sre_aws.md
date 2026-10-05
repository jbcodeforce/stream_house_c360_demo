# AWS Resources


Terraform is organized under the [`IaC/`](IaC/) directory as **three independent root modules** (separate local state), each file-per-concern. This split lets the core Confluent Cloud stack run locally with **no AWS credentials**; AWS and the managed connector are opt-in cost paths.


- [`IaC/connector/`](IaC/connector/) — **managed Debezium Postgres CDC connector + AWS dependency**: reads the core stack's outputs from `../ccloud/terraform.tfstate` via `terraform_remote_state`, and the RDS credentials from AWS Secrets Manager. Run with [`scripts/tf_connector.sh`](scripts/tf_connector.sh) (needs AWS creds). Optional — in local mode the backend app writes Debezium-shaped events directly to `cdc.public.*` topics instead.
- [`IaC/AWS/`](IaC/AWS/) — **AWS stack**: PostgreSQL RDS 17 (CDC-ready), VPC/subnet lookups, security group (allowlists Confluent egress IPs), KMS, and the Secrets Manager secret consumed by `IaC/connector/`. Files: `provider.tf`, `variables.tf`, `data.tf`, `aws.tf`, `outputs.tf`.

**Apply order:** `IaC/ccloud` (always) → `IaC/AWS` + `IaC/connector` (only for the full CDC-over-AWS path).


We will not detail how to use the AWS Console to create RDS Postgresql instance and access VPC information. The [terraform section](#aws-resources-rds-postgresql) below describres how to automate the creation of those resources.

### Pre-requisites

* Get Terraform cli
* Get aws CLI
* Get psql client to query Postgresql instance:
    ```
    brew install libpq
    ```
* Login to AWS console, search for the VPC to use
    ```sh
    aws sso login --profile your-profile-name
    export AWS_PROFILE="your-profile-name"
    ```
* Find you the public IP address of your machine: [https://checkip.amazonaws.com](https://checkip.amazonaws.com)
* Modify terraform environment variables `terraform.tfvars` with:
    ```sh
    db_allowed_cidr_blocks
    my_ip_addr
    ```



### Notes

It is important to get the following information to be able to create the Kafka Connector for change data capture.

| Resources | |
|-----| ---- |
| Region | us-west-2 |
| RDS host name | e.g. c360-j9r-postgres.c.....us-west-2.rds.amazonaws.com |
| secrets_manager_secret_arn | Resource ref for AWS Secrets Manager |
| Database password  | Coud be created by the admin or retrieve from the secret |
| Public certificates | a .pem file to download |

![](./docs/images/aws/aws-rds-info.png)

*To retrieve password from secret, knowing the secret arn do:*
```sh
aws secretsmanager get-secret-value --secret-id arn:aws:secretsmanager:us-west-2:.....:secret:c360-j9r-rds-credentials-9x --query SecretString --output text |jq -r .password
```

It is possible to retrieve the secret ARN from secrets manager:

![](./docs/images/aws/aws-secrets.png)
