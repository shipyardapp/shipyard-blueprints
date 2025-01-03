import argparse
import sys

from shipyard_templates import ShipyardLogger, ExitCodeException
from shipyard_templates.errors import EXIT_CODE_UNKNOWN_ERROR
from shipyard_magnite import MagniteClient, utils
from shipyard_magnite.errs import (
    EXIT_CODE_UPDATE_ERROR,
    EXIT_CODE_FILE_NOT_FOUND,
    EXIT_CODE_PARTIAL_FAILURE,
)

from shipyard_magnite.dataclasses.budget_item import BudgetItem
from shipyard_magnite.dataclasses.budgets import Budgets
from shipyard_magnite.dataclasses.targeting_spend_profile import TargetingSpendProfile
from shipyard_magnite.dataclasses.validation_results import ValidationResult


logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser(
        description="Update campaign budgets using Magnite API"
    )
    parser.add_argument("--username", required=True, help="Magnite API username")
    parser.add_argument("--password", required=True, help="Magnite API password")
    parser.add_argument(
        "--file", required=True, help="Path to the CSV file containing budget data"
    )
    return parser.parse_args()


def main():
    skipped = []
    errored = []
    successful = []
    try:
        args = get_args()
        client = MagniteClient(username=args.username, password=args.password)

        file_data = utils.open_csv(args.file)
        logger.debug(f"Loaded file data: {file_data}")
        grouped_data = utils.group_budget_data_by_campaign(file_data)
        logger.debug(f"Grouped data: {grouped_data}")

        for campaign in grouped_data:
            budget_items = grouped_data[campaign]
            update = {k: v for k, v in budget_items[0].items() if v not in ("", None)}
            if len(budget_items) > 1:
                logger.warning(
                    f"Multiple budget items found for campaign {campaign}. Continuing with the first item only. {update}"
                )
                for skipped_record in budget_items[1:]:
                    skipped_record["campaign_id"] = campaign
                    skipped.append(
                        ValidationResult(
                            status="Skipped",
                            message="Multiple budget lines found for campaign.",
                            details=skipped_record,
                        )
                    )
            try:
                campaign_details = client.get_campaign_by_id(campaign)
                profile_id = campaign_details["targeting_spend_profile"]["id"]
                current_budget = {
                    k: v
                    for k, v in campaign_details["targeting_spend_profile"]["budgets"][
                        0
                    ].items()
                    if k in BudgetItem.SUPPORTED_FIELDS
                }

            except Exception as e:
                errored.append(
                    ValidationResult(
                        status="Error",
                        message="Failed to retrieve current budget. {e}",
                        details={"campaign_id": campaign},
                    )
                )
                continue

            budget = Budgets(items=[BudgetItem(**(current_budget | update))])
            valid_budget = budget.validate()
            budget._validation_results[0].details["campaign_id"] = campaign

            if not len(valid_budget.items):
                errored.append(budget._validation_results[0])
                continue
            profile = TargetingSpendProfile(id=profile_id, budgets=valid_budget)
            try:
                client.update_campaign_budgets(campaign, profile)
            except Exception as e:
                errored.append(
                    ValidationResult(
                        status="Error",
                        message="Failed to update budget. {e}",
                        details={"campaign_id": campaign},
                    )
                )
            else:

                successful.append(
                    ValidationResult(
                        status="Success",
                        message="Budget updated successfully",
                        details=budget._validation_results[0].details,
                    )
                )

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except FileNotFoundError:
        logger.error(f"File not found: {args.file}")
        sys.exit(EXIT_CODE_FILE_NOT_FOUND)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)
    if skipped:
        logger.warning(
            f"======= Skipped records: {len(skipped)} ======= \n {ValidationResult.report(skipped)}"
        )
    if errored:
        logger.error(
            f"======= Errors: {len(errored)} ======= \n {ValidationResult.report(errored)}"
        )

    if successful:
        logger.info(
            f"======= Successful updates: {len(successful)} ======= \n {ValidationResult.report(successful)}"
        )

    if (errored or skipped) and successful:
        sys.exit(EXIT_CODE_PARTIAL_FAILURE)
    elif (errored or skipped) and not successful:
        sys.exit(EXIT_CODE_UPDATE_ERROR)


if __name__ == "__main__":
    main()
