import argparse
import sys

from shipyard_bp_utils import files as shipyard
from shipyard_templates import ShipyardLogger, ExitCodeException, CloudStorage

from shipyard_microsoft_sharepoint import SharePointClient, utils

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--access-token", required=False, help="Access token to use for authentication"
    )
    parser.add_argument(
        "--client-id", required=False, help="Client ID for OAuth authentication"
    )
    parser.add_argument(
        "--client-secret", required=False, help="Client secret for OAuth authentication"
    )
    parser.add_argument(
        "--tenant", required=False, help="Tenant for OAuth authentication"
    )
    parser.add_argument(
        "--site-name",
        required=True,
        dest="site_name",
        help="Site name to upload the file to",
    )
    parser.add_argument("--file-name", required=True, help="Name of the file to upload")
    parser.add_argument("--directory", default="", required=False)
    parser.add_argument(
        "--sharepoint-file-name",
        required=False,
        default="",
        help="Name of the file in OneDrive",
    )
    parser.add_argument(
        "--sharepoint-directory",
        required=False,
        default="",
        help="Directory in OneDrive to upload the file to",
    )
    parser.add_argument(
        "--match-type",
        required=False,
        default="exact_match",
        choices=["exact_match", "regex_match"],
        help="Type of matching to use when finding the file in the directory",
    )
    return parser.parse_args()


def main():
    try:
        args = get_args()
        credentials = utils.get_credential_group(args)
        sharepoint = SharePointClient(**credentials, site_name=args.site_name)

        target_dir = args.sharepoint_directory
        if target_dir:
            folder_id = sharepoint.get_folder_id(target_dir)
            if not folder_id:
                logger.info(f"Creating folder {target_dir}...")
                sharepoint.create_folder(target_dir)

        matching_files = shipyard.file_match(
            search_term=args.file_name,
            source_directory=args.directory,
            destination_directory=args.sharepoint_directory,
            match_type=args.match_type,
            destination_filename=args.sharepoint_file_name or args.file_name,
            files=shipyard.fetch_file_paths_from_directory(args.directory or "."),
        )
        logger.info(f"{len(matching_files)} file(s) found. Preparing to upload...")

        for file in matching_files:
            sharepoint.upload(file["source_path"], file["destination_filename"])

    except FileNotFoundError as e:
        logger.error(e)
        sys.exit(CloudStorage.EXIT_CODE_FILE_NOT_FOUND)
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
