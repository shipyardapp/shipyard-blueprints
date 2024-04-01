import argparse
import sys
import shipyard_bp_utils as utils
from shipyard_api import ShipyardClient
from shipyard_templates import ShipyardLogger, ExitCodeException
from shipyard_api.errors import EXIT_CODE_UNKNOWN_ERROR

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--organization-id", dest="org_id", required=True)
    parser.add_argument("--api-key", dest="api_key", required=True)
    parser.add_argument("--file-name", dest="file_name", required=True)
    parser.add_argument("--folder-name", dest="folder_name", required=False, default="")
    return parser.parse_args()


def main():
    try:
        args = get_args()
        org_id = args.org_id
        api_key = args.api_key
        file_name = args.file_name
        folder_name = args.folder_name
        if folder_name:
            utils.files.create_folder_if_dne(folder_name)
        target_path = utils.files.combine_folder_and_file_name(folder_name, file_name)
        shipyard = ShipyardClient(org_id=org_id, api_key=api_key)
        shipyard.export_voyages(target_path)
        logger.info("Successfully exported fleet runs to ")
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
