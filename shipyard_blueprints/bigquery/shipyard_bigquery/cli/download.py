import os
import json
import tempfile
import argparse
import sys
import shipyard_bp_utils as shipyard

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound

from shipyard_templates import ShipyardLogger, ExitCodeException
from shipyard_bigquery import BigQueryClient


logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", dest="query", required=True)
    parser.add_argument("--service-account", dest="service_account", required=True)
    parser.add_argument(
        "--destination-file-name",
        dest="destination_file_name",
        default="output.csv",
        required=True,
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
        target_file = args.destination_file_name
        target_folder = args.destination_folder_name
        target_path = shipyard.files.combine_folder_and_file_name(
            folder_name=target_folder, file_name=target_file
        )
        query = args.query

        if not os.path.exists(target_folder) and (target_folder != ""):
            os.makedirs(target_folder)

        client = BigQueryClient(args.service_account)
        client.connect()

        logger.debug(f"Query is {query}")
        df = client.fetch(query)
        logger.debug(f"Shape of the data is {df.shape}")
        df.to_csv(target_path, index=False)
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(
            f"Error in fetching query results and writing results to {target_path}: {str(e)}"
        )
        sys.exit(client.EXIT_CODE_NO_RESULTS)

    else:
        logger.info(f"Successfully wrote query results to {target_path}")


if __name__ == "__main__":
    main()
