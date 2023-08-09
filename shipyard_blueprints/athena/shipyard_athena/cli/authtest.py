import argparse
import os
import sys

import boto3
from shipyard_blueprints import AthenaClient


def get_args():
    return {
        "aws_access_key": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "region": os.getenv("AWS_DEFAULT_REGION"),
    }


def main():
    athena = AthenaClient(
        os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    try:
        client = boto3.client(
            "sts",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        response = client.get_caller_identity()
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            athena.logger.info("Successfully connected to AWS Athena")
            sys.exit(0)
    except Exception as e:
        athena.logger.error("Could not connect to the AWS Athena")
        sys.exit(1)

    else:
        athena.logger.error("Could not connect to the AWS Athena")
        sys.exit(1)


if __name__ == "__main__":
    main()
