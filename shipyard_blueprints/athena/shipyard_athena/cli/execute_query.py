import argparse
import os
import sys
from shipyard_athena import AthenaClient
from shipyard_templates import ShipyardLogger, ExitCodeException, Database

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--aws-access-key-id", dest="aws_access_key_id", required=True)
    parser.add_argument(
        "--aws-secret-access-key", dest="aws_secret_access_key", required=False
    )
    parser.add_argument(
        "--aws-default-region", dest="aws_default_region", required=True
    )
    parser.add_argument("--bucket-name", dest="bucket_name", required=True)
    parser.add_argument("--log-folder", dest="log_folder", required=False)
    parser.add_argument("--database", dest="database", required=False)
    parser.add_argument("--query", dest="query", required=True)
    args = parser.parse_args()
    return args


def main():
    try:
        args = get_args()
        access_key = args.aws_access_key_id
        secret_key = args.aws_secret_access_key
        region_name = args.aws_default_region
        database = args.database if args.database else None
        bucket = args.bucket_name.strip("/")
        log_folder = args.log_folder if args.log_folder else None
        query = args.query

        client = AthenaClient(
            aws_access_key=access_key,
            aws_secret_key=secret_key,
            region=region_name,
            bucket=bucket,
        )

        state = client.execute_query(
            query=query, database=database, log_folder=log_folder
        )
        logger.debug(f"Query State is {state}")
        if state == "SUCCEEDED":
            logger.info("Successfully executed query")

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(
            f"An unexpected error occurred when attempting to execute query. Message from AWS: {e}"
        )
        sys.exit(Database.EXIT_CODE_UNKNOWN)


if __name__ == "__main__":
    main()
