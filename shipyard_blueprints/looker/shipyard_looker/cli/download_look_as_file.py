import argparse
import sys
import shipyard_bp_utils as shipyard

from shipyard_templates import ShipyardLogger, ExitCodeException, DataVisualization
from shipyard_looker import LookerClient

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", dest="base_url", required=True)
    parser.add_argument("--client-id", dest="client_id", required=True)
    parser.add_argument("--client-secret", dest="client_secret", required=True)
    parser.add_argument("--look-id", dest="look_id", required=True)
    parser.add_argument("--destination-file-name", dest="dest_file_name", required=True)
    parser.add_argument(
        "--destination-folder-name", dest="dest_folder_name", required=False, default=""
    )
    parser.add_argument(
        "--file-type",
        dest="file_type",
        choices=[
            "json",
            "txt",
            "csv",
            "json_detail",
            "md",
            "xlsx",
            "sql",
            "png",
            "jpg",
        ],
        type=str.lower,
        required=True,
    )
    args = parser.parse_args()
    return args


def main():
    try:
        args = get_args()
        base_url = args.base_url
        client_id = args.client_id
        client_secret = args.client_secret
        file_type = args.file_type
        look_id = args.look_id
        target_file = args.dest_file_name
        target_dir = args.dest_folder_name

        if target_dir:
            shipyard.files.create_folder_if_dne(target_dir)

        target_path = shipyard.files.combine_folder_and_file_name(
            target_dir, target_file
        )
        # generate SDK
        looker = LookerClient(
            base_url=base_url, client_id=client_id, client_secret=client_secret
        )

        looker.download_look(look_id, target_path, file_format=file_type)

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(
            f"An unexpected error occurred when attempting to download the Look: {e}"
        )
        sys.exit(DataVisualization.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
