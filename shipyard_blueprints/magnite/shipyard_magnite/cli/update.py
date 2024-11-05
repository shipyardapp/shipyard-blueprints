import argparse
import sys
import os

from shipyard_magnite import MagniteClient
from shipyard_templates import ShipyardLogger
from shipyard_templates import errors, ExitCodeException

logger = ShipyardLogger.get_logger()

EXIT_CODE_INVALID_ARGS = 200


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--id", required=False, default="")
    parser.add_argument("--file", required=False, default="")
    parser.add_argument(
        "--budget-value", dest="budget_value", required=False, default=""
    )
    parser.add_argument(
        "--budget-period", dest="budget_period", required=False, default=""
    )
    parser.add_argument(
        "--budget-pacing", dest="budget_pacing", required=False, default=""
    )
    return parser.parse_args()


def main():
    try:
        args = get_args()
        if not args.id or args.file:
            logger.error(
                "Either a file or an ID must be provided. For single updates, provide an ID. For bulk updates, provide a file"
            )
            sys.exit(EXIT_CODE_INVALID_ARGS)

        if args.id and not args.budget_value:
            logger.error("A budget value is necessary to update a single ID")
            sys.exit(EXIT_CODE_INVALID_ARGS)

        client = MagniteClient(args.username, args.password)
        client.connect()

        if args.file:
            pass
        elif args.id:
            client.update_single(
                endpoint=args.endpoint, id=args.id, budget_value=args.budget_value
            )

        else:
            logger.error("Must provide a file or id")
            sys.exit(EXIT_CODE_INVALID_ARGS)

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error("Error in reading data from api")
        logger.error(f"Message reads: {e}")


if __name__ == "__main__":
    main()
