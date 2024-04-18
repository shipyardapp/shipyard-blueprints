import argparse
import shipyard_bp_utils as shipyard
import sys
from shipyard_templates import ShipyardLogger, ExitCodeException, Database
from shipyard_motherduck import MotherDuckClient

logger = ShipyardLogger().get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True)
    parser.add_argument("--table-name", required=True)
    parser.add_argument("--database", required=False, default="")
    parser.add_argument("--file-name", required=True)
    parser.add_argument("--directory", required=False, default=None)
    parser.add_argument(
        "--insert-method",
        required=False,
        default="replace",
        choices={"replace", "append"},
    )
    parser.add_argument(
        "--match-type",
        dest="match_type",
        required=False,
        default="exact_match",
        choices={"glob_match", "exact_match", "regex_match"},
    )

    return parser.parse_args()


def main():
    try:
        args = get_args()
        database = args.database if args.database != "" else None
        client = MotherDuckClient(args.token, database=database)
        table_name = args.table_name
        file_name = args.file_name
        directory = args.directory
        file_path = file_name
        insert_method = args.insert_method

        if directory:
            file_path = shipyard.files.combine_folder_and_file_name(
                directory, file_name
            )

        if args.match_type in ["glob_match", "regex_match"]:
            file_matches = shipyard.files.find_matching_files(
                file_name, directory, match_type=args.match_type
            )

            if n_matches := (len(file_matches)) == 0:
                logger.error(f"No file matches found for pattern {file_name}")
                sys.exit(Database.EXIT_CODE_FILE_NOT_FOUND)
            logger.info(f"Found {n_matches} file matches, preparing to upload...")
            for file in file_matches:
                logger.debug(f"Uploading {file}")
                client.upload(table_name, file, insert_method=insert_method)
                if insert_method == "replace":
                    insert_method = "append"
            logger.info("Successfully uploaded all files")
        elif args.match_type == "exact_match":
            client.upload(table_name, file_path, insert_method=insert_method)
        else:
            logger.error(
                "Invalid match type, select either 'glob_match', 'exact_match', or 'regex_match'"
            )
            sys.exit(Database.EXIT_CODE_INVALID_ARGUMENTS)
    except FileNotFoundError:
        logger.error(f"File {file_path} not found")
        sys.exit(Database.EXIT_CODE_FILE_NOT_FOUND)
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(
            f"An unexpected error occurred when attempting to upload to MotherDuck: {e}"
        )
        sys.exit(Database.EXIT_CODE_UNKNOWN)


if __name__ == "__main__":
    main()
