import sys
import os
import argparse
from shipyard_templates import ExitCodeException, ShipyardLogger
import shipyard_utils as shipyard
from shipyard_domo.utils import exceptions as errs
from shipyard_domo import DomoClient

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--client-id", dest="client_id", required=True)
    parser.add_argument("--secret-key", dest="secret_key", required=True)
    parser.add_argument("--dataset-id", dest="dataset_id", required=True)
    parser.add_argument("--destination-file-name", dest="dest_file_name", required=True)
    parser.add_argument(
        "--destination-folder-name", dest="dest_folder_name", required=False
    )
    args = parser.parse_args()
    return args


def main():
    try:
        args = get_args()
        dest_file = args.dest_file_name
        dest_folder = args.dest_folder_name or os.getcwd()
        client = DomoClient(client_id=args.client_id, secret_key=args.secret_key)
        df = client.download_dataset(args.dataset_id)
        logger.info("Successfully downloaded dataset")
        # write the dataframe to a file
        if args.dest_folder_name:
            shipyard.files.create_folder_if_dne(dest_folder)
        dest_path = shipyard.files.combine_folder_and_file_name(dest_folder, dest_file)
        df.to_csv(dest_path, index=False)
        logger.info(f"Successfully stored results to {dest_path}")

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(
            f"Error in Downloading dataset from Domo. Error message reads: {str(e)}"
        )
        sys.exit(errs.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
