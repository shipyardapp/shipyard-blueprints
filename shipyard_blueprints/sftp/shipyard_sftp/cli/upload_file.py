import argparse
import os
import sys
import tempfile

from shipyard_bp_utils import files as shipyard
from shipyard_templates import CloudStorage, ExitCodeException
from shipyard_templates.shipyard_logger import ShipyardLogger

from shipyard_sftp.sftp import SftpClient

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
    parser.add_argument("--key", dest="key", default=None, required=False)
    return parser.parse_args()


def main():
    key_path = None
    try:
        args = get_args()
        if not args.password and not args.key:
            raise ExitCodeException(
                "Must specify a password or a RSA key",
                CloudStorage.EXIT_CODE_INVALID_CREDENTIALS,
            )

        connection_args = {"host": args.host, "port": args.port, "user": args.username}

        if args.password:
            connection_args["pwd"] = args.password

        if key := args.key:
            if not os.path.isfile(key):
                fd, key_path = tempfile.mkstemp()
                logger.info(f"Storing RSA temporarily at {key_path}")
                with os.fdopen(fd, "w") as tmp:
                    tmp.write(key)
                connection_args["key"] = key_path
        sftp = SftpClient(**connection_args)

        source_file_name = args.source_file_name
        source_folder_name = args.source_folder_name
        source_full_path = os.path.join(
            os.getcwd(), source_folder_name, source_file_name
        )

        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name
        )
        source_file_name_match_type = args.source_file_name_match_type

        if source_file_name_match_type == "exact_match":
            destination_full_path = shipyard.determine_destination_full_path(
                destination_folder_name=destination_folder_name,
                destination_file_name=args.destination_file_name,
                source_full_path=source_full_path,
            )
            if not os.path.isfile(source_full_path):
                raise ExitCodeException(
                    f"{source_full_path} is not a file", sftp.EXIT_CODE_FILE_MATCH_ERROR
                )
            sftp.upload(source_full_path, destination_full_path)

        elif source_file_name_match_type == "regex_match":
            file_names = shipyard.find_all_file_matches(
                sftp.list_files_recursive(source_folder_name or "."),
                args.source_file_name,
            )
            logger.info(f"{len(file_names)} files found. Preparing to upload...")

            for index, key_name in enumerate(file_names):
                destination_full_path = shipyard.determine_destination_full_path(
                    destination_folder_name=destination_folder_name,
                    destination_file_name=args.destination_file_name,
                    source_full_path=key_name,
                    file_number=index + 1,
                )

                logger.info(f"Uploading file {index + 1} of {len(file_names)}")
                if not os.path.isfile(key_name):
                    logger.warning(f"{key_name} is not a file")
                    continue
                sftp.upload(key_name, destination_full_path)

    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(e)
        sys.exit(CloudStorage.EXIT_CODE_UNKNOWN_ERROR)
    finally:
        if key_path:
            logger.info(f"Removing temporary RSA Key file {key_path}")
            os.remove(key_path)


if __name__ == "__main__":
    main()
