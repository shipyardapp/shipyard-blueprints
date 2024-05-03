import os
import sys

from shipyard_templates import ShipyardLogger

from shipyard_s3 import S3Client

logger = ShipyardLogger.get_logger()


def main():
    sys.exit(
        S3Client(
            aws_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region=os.getenv("AWS_DEFAULT_REGION"),
        ).connect()
    )


if __name__ == "__main__":
    main()
