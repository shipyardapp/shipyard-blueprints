import argparse
import shipyard_bp_utils as shipyard
import sys
from shipyard_templates import ShipyardLogger, ExitCodeException, Database
from shipyard_motherduck import MotherDuckClient

logger = ShipyardLogger().get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--database", required=False, dest="database", default="")
    parser.add_argument("--file-name", required=True)
    parser.add_argument("--directory", required=False, default=None)
    parser.add_argument(
        "--file-type", required=False, default="csv", choices={"csv", "parquet"}
    )

    return parser.parse_args()


def main():
    try:
        args = get_args()
        query = args.query
        target_file = args.file_name
        target_dir = args.directory
        database = args.database if args.database != "" else None
        client = MotherDuckClient(args.token, database=database)
        if target_dir:
            shipyard.files.create_folder_if_dne(target_dir)
            target_file = shipyard.files.combine_folder_and_file_name(
                target_dir, target_file
            )
        result = client.fetch(query)
        if args.file_type == "csv":
            result.to_csv(target_file, sep=",", header=True)
        elif args.file_type == "parquet":
            result.to_parquet(target_file)

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(
            f"An unexpected error occurred when attempting to fetch query results from MotherDuck: {e}"
        )
        sys.exit(Database.EXIT_CODE_UNKNOWN)
    else:
        logger.info(f"Successfully downloaded query results to {target_file}")


if __name__ == "__main__":
    main()
