import argparse
import sys

import shipyard_bp_utils as shipyard
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
    parser.add_argument("--src-file", required=True, help="Name of the file to upload")
    parser.add_argument("--src-dir", default="", required=False)
    parser.add_argument(
        "--dest-file",
        required=False,
        default="",
        help="Name of the moved file in SharePoint",
    )
    parser.add_argument(
        "--dest-dir",
        required=False,
        default="",
        help="Name of the directory to move the file into",
    )
    parser.add_argument(
        "--match-type",
        required=False,
        default="exact_match",
        choices=["exact_match", "regex_match"],
        help="Type of match to use when moving the files",
    )
    return parser.parse_args()


def main():
    try:
        args = get_args()

        src_file = args.src_file
        src_dir = args.src_dir
        dest_file = args.dest_file
        dest_dir = args.dest_dir

        if not dest_file:
            dest_file = src_file
        credentials = utils.get_credential_group(args)

        sharepoint = SharePointClient(**credentials, site_name=args.site_name)

        if args.match_type == "exact_match":
            sharepoint.move(
                src_name=src_file,
                src_dir=src_dir,
                target_name=dest_file,
                target_dir=dest_dir,
            )
        elif args.match_type == "regex_match":
            matches = sharepoint.get_file_matches(src_dir, src_file)
            if (n_matches := len(matches)) == 0:
                logger.error(f"No files found matching '{src_file}'")
                sys.exit(CloudStorage.EXIT_CODE_FILE_NOT_FOUND)
            logger.info(f"{n_matches} files found, preparing to download...")
            file_names = [file["name"] for file in matches]
            for index, file in enumerate(file_names, start=1):
                src_path = shipyard.files.combine_folder_and_file_name(src_dir, file)
                file_name = shipyard.files.determine_destination_file_name(
                    source_full_path=src_path,
                    destination_file_name=dest_file,
                    file_number=index if args.dest_file else None,
                )
                sharepoint.move(
                    src_name=file,
                    src_dir=src_dir,
                    target_name=file_name,
                    target_dir=dest_dir,
                )
            logger.info("Successfully moved all files")

    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
