import argparse
import sys

from shipyard_templates import ShipyardLogger, ExitCodeException, InvalidCredentialError
from shipyard_templates.errors import EXIT_CODE_UNKNOWN_ERROR
from shipyard_magnite import MagniteClient, utils, errs
from shipyard_magnite.errs import PartialFailure


logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--file", required=True)
    parser.add_argument("--update-method", required=False, default="UPSERT")
    return parser.parse_args()


def main():
    try:
        errors = []
        args = get_args()
        client = MagniteClient(username=args.username, password=args.password)

        file_data = utils.open_csv(args.file)
        campaigns = utils.group_budget_data_by_campaign(file_data)
        for campaign_id in campaigns:
            try:
                client.update_campaign_budgets(
                    campaign_id=campaign_id,
                    budget_data=file_data,
                    update_method=args.update_method,
                )
            except InvalidCredentialError:
                raise
            except Exception as e:
                errors.append({"campaign_id": campaign_id, "error": str(e)})
            else:
                logger.info(f"Successfully updated campaign {campaign_id}")

        if errors:
            logger.error(
                f"{len(errors)} out of {len(campaigns)} campaigns failed to update"
            )
            for error in errors:
                logger.error(
                    f"Error updating campaign {error['campaign_id']}: {error['error']}"
                )
            if len(errors) == len(campaigns):
                raise errs.UpdateError("All campaigns failed to update")
            else:
                raise PartialFailure(
                    f"Errors occurred while updating budgets: {errors}"
                )
    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except FileNotFoundError as fe:
        logger.error(f"File not found: {fe}")
        sys.exit(FileNotFoundError)
    except Exception as e:
        logger.error(f"An Unexpected Error in reading data from api \nError: {e}")
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
