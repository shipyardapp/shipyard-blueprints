import argparse
import sys

from shipyard_magnite import MagniteClient
from shipyard_magnite.errs import (
    EXIT_CODE_INVALID_ARGS,
    EXIT_CODE_PARTIAL_FAILURE,
    EXIT_CODE_FILE_NOT_FOUND,
)
from shipyard_templates import ShipyardLogger
from shipyard_templates import ExitCodeException

logger = ShipyardLogger.get_logger()


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
        id = args.id
        file = args.file
        budget_value = args.budget_value
        budget_period = args.budget_period
        budget_pacing = args.budget_pacing
        username = args.username
        password = args.password
        endpoint = args.endpoint
        if not (id or file):
            logger.error(
                "Either a file or an ID must be provided. For single updates, provide an ID. For bulk updates, provide a file"
            )
            sys.exit(EXIT_CODE_INVALID_ARGS)

        if id and not budget_value:
            logger.error("A budget value is necessary to update a single ID")
            sys.exit(EXIT_CODE_INVALID_ARGS)

        client = MagniteClient(username, password)
        client.connect()
        res = client.update(
            endpoint=endpoint, id=id, budget_value=budget_value, file=file
        )
        if all(res):
            logger.info("All updates were executed successfully")
            sys.exit(0)
        else:
            n_entries = len(res)
            failed = list(filter(lambda x: not x, res))
            logger.error(
                f"{len(failed)} out of {n_entries} failed to update successfully"
            )
            sys.exit(EXIT_CODE_PARTIAL_FAILURE)

    except FileNotFoundError as fe:
        logger.error(fe)
        sys.exit(EXIT_CODE_FILE_NOT_FOUND)
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except Exception as e:
        logger.error("Error in reading data from api")
        logger.error(f"Message reads: {e}")


if __name__ == "__main__":
    main()
