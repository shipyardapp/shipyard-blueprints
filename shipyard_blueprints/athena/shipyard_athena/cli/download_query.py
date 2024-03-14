import os
import argparse
import shipyard_bp_utils as shipyard
import sys
from shipyard_athena import AthenaClient
from shipyard_templates import ShipyardLogger, Database, ExitCodeException
from shipyard_bp_utils.artifacts import Artifact

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
    parser.add_argument("--bucket-name", dest="bucket", required=True)
    parser.add_argument("--log-folder", dest="log_folder", default="", required=False)
    parser.add_argument("--database", dest="database", required=True)
    parser.add_argument("--query", dest="query", required=True)
    parser.add_argument(
        "--destination-file-name",
        dest="destination_file_name",
        default="output.csv",
        required=False,
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    args = parser.parse_args()
    return args


def main():
    try:
        args = get_args()
        access_key = args.aws_access_key_id
        secret_key = args.aws_secret_access_key
        region_name = args.aws_default_region if args.aws_default_region else None
        database = args.database if args.database else None
        bucket = args.bucket.strip("/")
        log_folder = args.log_folder if args.log_folder else None
        query = args.query
        target_file = args.destination_file_name
        target_dir = args.destination_folder_name
        target_path = shipyard.files.combine_folder_and_file_name(
            folder_name=target_dir, file_name=target_file
        )
        if target_dir:
            shipyard.files.create_folder_if_dne(target_dir)

        client = AthenaClient(
            aws_access_key=access_key,
            aws_secret_key=secret_key,
            region=region_name,
            bucket=bucket,
        )

        response = client.fetch(
            query=query, dest_path=target_path, log_folder=log_folder, database=database
        )
        logger.info(f"Successfully downloaded query results to {target_path}")
        # create the artifacts if the response is not None
        if response:
            Artifact("athena").responses.write_json("athena_response.json", response)

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(
            f"An unexpected error occurred when attempting to download query results. Message from AWS: {e}"
        )
        sys.exit(Database.EXIT_CODE_UNKNOWN)


if __name__ == "__main__":
    main()
