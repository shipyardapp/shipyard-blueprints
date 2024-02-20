import os
import sys
from shipyard_s3 import S3Client
from shipyard_templates import ShipyardLogger
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

logger = ShipyardLogger.get_logger()


def main():
    try:
        s3 = S3Client(
            aws_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        ).connect()
    except Exception as e:
        logger.error("Could not connect to S3 with the provided credentials")
        logger.debug(f"Response from the server: {str(e)}")
    else:
        logger.info("Successfully connected to S3")
        logger.debug(f"Response from the server: {str(s3)}")


if __name__ == "__main__":
    main()
