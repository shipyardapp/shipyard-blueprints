import argparse
import sys

from shipyard_templates import ShipyardLogger, ExitCodeException
from shipyard_magnite import MagniteClient
from shipyard_magnite.errs import InvalidArgs, PartialFailure


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
    parser.add_argument(
        "--budget-metric", dest="budget_metric", required=False, default=""
    )

    return parser.parse_args()


def main():
    try:
        args = get_args()
        upload_args = validate_args(args)
        client = MagniteClient(args.username, args.password)
        results = client.update(**upload_args)
        if all(results):
            logger.info("All updates were executed successfully")
        else:
            n_entries = len(results)
            failed = list(filter(lambda x: not x, results))
            raise PartialFailure(
                f"{len(failed)} out of {n_entries} failed to update successfully"
            )

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except FileNotFoundError as fe:
        logger.error(f"File not found: {fe}")
        sys.exit(FileNotFoundError)
    except Exception as e:
        logger.error("An Unexpected Error in reading data from api \nError: {e}")
        sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)


def validate_args(args: argparse.Namespace) -> dict:
    """
    Validate the command line arguments.

    Args:
        args: Parsed command line arguments.

    Returns:
        dict: A dictionary of validated arguments to be used for the upload function.

    Raises:
        InvalidArgs: If neither 'id' nor 'file' is provided, or if 'id' is provided without 'budget_value'.
    """
    if not (args.id or args.file):
        raise InvalidArgs(
            "Either a file or an ID must be provided. For single updates, provide an ID. For bulk updates, provide a file"
        )

    if args.id and not args.budget_value:
        raise InvalidArgs("A budget value is necessary to update a single ID")

    _args = {
        "id": args.id,
        "file": args.file,
        "budget_value": args.budget_value,
        "endpoint": args.endpoint,
        "budget_metric": args.budget_metric,
    }
    validated_args = {k: v for k, v in _args.items() if v}
    return validated_args


if __name__ == "__main__":
    main()
