import argparse
import os
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
    parser.add_argument(
        "--file-name", required=False, help="The name of the file once it is downloaded"
    )
    parser.add_argument("--directory", default="", required=False)
    parser.add_argument(
        "--sharepoint-file-name",
        required=True,
        default="",
    )
    parser.add_argument(
        "--sharepoint-directory",
        required=False,
        default="",
    )
    parser.add_argument(
        "--match-type",
        required=False,
        default="exact_match",
        choices=["exact_match", "regex_match"],
        help="Type of match to use when downloading the files",
    )
    return parser.parse_args()


def main():
    try:
        args = get_args()
        credentials = utils.get_credential_group(args)

        src_file = args.sharepoint_file_name
        src_dir = args.sharepoint_directory
        src_path = shipyard.files.combine_folder_and_file_name(src_dir, src_file)

        target_file = args.file_name or src_file
        target_dir = args.directory
        target_path = shipyard.files.combine_folder_and_file_name(
            target_dir, target_file
        )

        if target_dir:
            shipyard.files.create_folder_if_dne(target_dir)

        sharepoint = SharePointClient(**credentials, site_name=args.site_name)

        if args.match_type == "exact_match":
            sharepoint.download(target_path, src_path)
            logger.info(f"Successfully downloaded file to {target_path}")
        elif args.match_type == "regex_match":
            matches = sharepoint.get_file_matches(src_dir, src_file)
            if (n_matches := len(matches)) == 0:
                logger.error(f"No files found matching '{src_file}'")
                sys.exit(CloudStorage.EXIT_CODE_FILE_NOT_FOUND)

            logger.info(f"{n_matches} files found, preparing to download...")
            download_urls = list(
                map(
                    lambda x: {
                        "name": x["name"],
                        "url": x["@microsoft.graph.downloadUrl"],
                    },
                    matches,
                )
            )
            logger.debug(f"Download URLs: {download_urls}")
            for index, file in enumerate(download_urls, start=1):
                name = file["name"]
                download_url = file["url"]
                file_ext = os.path.splitext(name)[1]
                dest_path = shipyard.files.determine_destination_full_path(
                    destination_folder_name=target_dir,
                    destination_file_name=target_file,
                    source_full_path=file,
                    file_number=index if target_file else None,
                )
                dest_path += file_ext
                sharepoint.download_from_graph_url(download_url, dest_path)
            logger.info("Successfully downloaded all files")

    except ExitCodeException as ec:
        logger.error(ec)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
