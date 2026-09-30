"""
db_utils.py
───────────
Shared database connection and CLI argument parsing helpers for C360 DB scripts.
"""

import argparse
import json
import logging
import os
import sys

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)


def add_db_arguments(parser: argparse.ArgumentParser) -> None:
    """Add standard database connection arguments to an ArgumentParser."""
    # Secrets Manager (preferred)
    parser.add_argument("--secret-arn", help="AWS Secrets Manager secret ARN (preferred)")
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    # Manual overrides
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="c360db")
    parser.add_argument("--username", default="dbadmin")
    parser.add_argument("--password", default=None)
    parser.add_argument("--sslmode", default="require")
    parser.add_argument(
        "--ssl-root-cert",
        default=os.path.expanduser("~/.ssh/global-bundle.pem"),
        help="Path to the CA bundle for SSL verification "
             "(default: ~/.ssh/global-bundle.pem)",
    )


def fetch_secret(secret_arn: str, region: str) -> dict:
    """Retrieve and parse a JSON secret from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region)
    try:
        resp = client.get_secret_value(SecretId=secret_arn)
    except ClientError as exc:
        log.error("Failed to retrieve secret %s: %s", secret_arn, exc)
        sys.exit(1)
    return json.loads(resp["SecretString"])


def build_conn_params(args: argparse.Namespace) -> dict:
    """Return psycopg2 connection kwargs from CLI args or Secrets Manager."""
    if args.secret_arn:
        log.info("Loading connection details from Secrets Manager: %s", args.secret_arn)
        secret = fetch_secret(args.secret_arn, args.region)
        params = {
            "host": secret["host"],
            "port": int(secret.get("port", 5432)),
            "dbname": secret["database"],
            "user": secret["username"],
            "password": secret["password"],
            "sslmode": "verify-full",
            "sslrootcert": args.ssl_root_cert,
        }
        return params

    params = {
        "host": args.host,
        "port": args.port,
        "dbname": args.dbname,
        "user": args.username,
        "password": args.password,
        "sslmode": args.sslmode,
    }
    if args.sslmode not in ("disable", "allow"):
        params["sslrootcert"] = args.ssl_root_cert
    return params


def validate_db_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Validate that either --secret-arn or --password was provided."""
    if not args.secret_arn and args.password is None:
        parser.error("Provide --secret-arn (recommended) or --password for manual connection.")
