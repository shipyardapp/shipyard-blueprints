import argparse
import sys

from shipyard_bp_utils import files as shipyard
from shipyard_templates import ShipyardLogger, ExitCodeException, CloudStorage

from shipyard_ftp.ftp import FtpClient

logger = ShipyardLogger().get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        choices={"exact_match", "regex_match"},
        required=True,
    )
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    parser.add_argument(
        "--destination-file-name",
        dest="destination_file_name",
        default=None,
        required=False,
    )
    parser.add_argument("--host", dest="host", default=None, required=True)
    parser.add_argument("--port", dest="port", default=21, required=True)
    parser.add_argument("--username", dest="username", default=None, required=False)
    parser.add_argument("--password", dest="password", default=None, required=False)
    return parser.parse_args()


def main():
    try:
        args = get_args()

        source_file_name = args.source_file_name
        source_folder_name = args.source_folder_name
        destination_filename = args.destination_file_name or source_file_name
        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name
        )

        ftp_client = FtpClient(
            host=args.host, port=args.port, user=args.username, pwd=args.password
        )
        files = shipyard.file_match(
            search_term=source_file_name,
            files=shipyard.fetch_file_paths_from_directory(source_folder_name),
            source_directory=source_folder_name,
            destination_directory=destination_folder_name,
            destination_filename=destination_filename,
            match_type=args.source_file_name_match_type,
        )

        for file in files:
            source_file = file["source_path"]
            destination_full_path = file["destination_filename"]
            if len(destination_full_path.split("/")) > 1:
                path, file_name = destination_full_path.rsplit("/", 1)
                ftp_client.create_new_folders(path)
            ftp_client.upload(source_file, destination_full_path)

    except ExitCodeException as e:
        logger.error(e.message)
        sys.exit(e.exit_code)
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(CloudStorage.EXIT_CODE_FILE_MATCH_ERROR)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
