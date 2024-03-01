import argparse
import os
import re
import sys
import tempfile
import time

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
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument(
        "--destination-file-name",
        dest="destination_file_name",
        default=None,
        required=False,
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
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

        errors = []
        source_file_name = args.source_file_name
        source_folder_name = shipyard.clean_folder_name(args.source_folder_name)
        source_full_path = shipyard.combine_folder_and_file_name(
            folder_name=source_folder_name, file_name=source_file_name
        )
        source_file_name_match_type = args.source_file_name_match_type

        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name
        )
        shipyard.create_folder_if_dne(destination_folder_name)

        if source_file_name_match_type == "exact_match":
            destination_name = shipyard.determine_destination_full_path(
                destination_folder_name=destination_folder_name,
                destination_file_name=args.destination_file_name,
                source_full_path=source_full_path,
            )
            sftp.download(source_full_path, destination_name)

            try:
                destination_name = shipyard.determine_destination_full_path(
                    destination_folder_name=destination_folder_name,
                    destination_file_name=args.destination_file_name,
                    source_full_path=source_full_path,
                )
                sftp.download(source_full_path, destination_name)

            except Exception as e:
                logger.error(f"Failed to download {source_full_path} due to {e}")
                errors.append({"filename": source_full_path, "error": e})
            finally:
                sftp.close()

            if errors:
                logger.error(f"Failed to download {len(errors)} file(s): {errors}")
                sys.exit(CloudStorage.EXIT_CODE_DOWNLOAD_ERROR)

        elif source_file_name_match_type == "regex_match":
            files = sftp.list_files_recursive(source_folder_name or ".")
            matching_file_names = shipyard.find_all_file_matches(
                files, re.compile(source_file_name)
            )
            if matching_file_names:
                logger.info(
                    f"{len(matching_file_names)} files found. Preparing to download..."
                )
            else:
                raise ExitCodeException(
                    f"No files matching {source_file_name} found",
                    sftp.EXIT_CODE_FILE_MATCH_ERROR,
                )
            for index, file_name in enumerate(matching_file_names):
                destination_name = shipyard.determine_destination_full_path(
                    destination_folder_name=destination_folder_name,
                    destination_file_name=args.destination_file_name,
                    source_full_path=file_name,
                    file_number=index + 1,
                )

                logger.info(
                    f"Downloading file {index + 1} of {len(matching_file_names)}"
                )

                try:
                    sftp.download(file_name, destination_name)

                except Exception as e:
                    errors.append({"filename": file_name, "error": e})

                    logger.error(
                        f"Failed to download {file_name} due to {e}... Skipping"
                    )
                    logger.info("Closing current SFTP session and reconnecting..")
                    sftp.client.close()
                    time.sleep(1)
            sftp.close()
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
