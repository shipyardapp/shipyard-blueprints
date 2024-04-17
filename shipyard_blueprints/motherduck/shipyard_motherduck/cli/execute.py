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
    return parser.parse_args()


def main():
    try:
        args = get_args()
        query = args.query
        database = args.database if args.database != "" else None
        client = MotherDuckClient(args.token, database=database)
        client.execute_query(query)

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error(
            f"An unexpected error occurred when attempting to execute query against MotherDuck: {e}"
        )
        sys.exit(Database.EXIT_CODE_UNKNOWN)


if __name__ == "__main__":
    main()
