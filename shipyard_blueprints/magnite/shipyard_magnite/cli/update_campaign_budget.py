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
from shipyard_magnite.dataclasses.targeting_spend_profile import TargetingSpendProfile

"""
This script updates a campaign with multiple budgets based on the data provided in a CSV file.
The CSV file is treated as the source of truth for budget information. If the CSV specifies one budget for a campaign (e.g., campaign "abc") but the campaign currently has two budgets, the script will update the budget from the CSV and delete the other.
This ensures that the campaign's budgets exactly match those defined in the CSV. 

Note: The script does not add budgets to existing ones; it replaces them entirely with the data from the CSV file.
"""

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


def process_campaign(client, campaign_id, budget_data):
    """
    Process a single campaign's budget.
    """
    try:
        logger.info(f"Processing campaign ID: {campaign_id}...")

        budgets = utils.transform_data_to_budgets(budget_data)
        valid_budgets = budgets.validate()
        report = budgets.report_validation_results()

        if not valid_budgets.items:
            logger.error(f"No valid budgets found for campaign ID: {campaign_id}")
            return {"errors": [campaign_id], "report": {campaign_id: report}}

        if len(valid_budgets.items) < len(budgets.items):
            logger.warning(
                f"Found invalid budgets for campaign ID: {campaign_id}. "
                f"Proceeding with the following valid budgets:\n"
                f"{chr(10).join(str(budget) for budget in valid_budgets.items)}"
            )

        profile_id = client.get_campaign_by_id(campaign_id)["targeting_spend_profile"][
            "id"
        ]
        spend_profile = TargetingSpendProfile(id=profile_id, budgets=valid_budgets)
        client.update_campaign_budgets(campaign_id, spend_profile)

        if len(valid_budgets.items) == len(budgets.items):
            logger.info(f"Successfully updated budgets for campaign ID: {campaign_id}")
        else:
            logger.warning(
                f"Partial success: Updated budgets for campaign ID: {campaign_id}. \n"
                "Note: Some budget items were invalid and excluded from the update."
            )

        return {
            "errors": (
                [] if len(valid_budgets.items) == len(budgets.items) else [campaign_id]
            ),
            "report": {campaign_id: report},
        }

    except Exception as e:
        logger.warning(f"Error processing campaign ID: {campaign_id}: {e}")
        return {"errors": [campaign_id], "report": {campaign_id: f"Error: {str(e)}"}}


def main():
    try:
        args = get_args()
        client = MagniteClient(username=args.username, password=args.password)

        file_data = utils.open_csv(args.file)
        logger.debug(f"Loaded file data: {file_data}")

        campaigns = utils.group_budget_data_by_campaign(file_data)
        logger.info(f"Found {len(campaigns)} campaign(s) in the {args.file} file.")

        errors = []
        reports = []

        for campaign_id, budget_data in campaigns.items():
            result = process_campaign(client, campaign_id, budget_data)
            errors.extend(result["errors"])
            reports.append(result["report"])

        utils.log_campaign_report(reports, errors, "Campaign")
        if errors:
            if len(errors) == len(campaigns):
                logger.error(
                    "All campaigns encountered issues — inspect error details to address failures."
                )
                sys.exit(EXIT_CODE_UPDATE_ERROR)
            else:
                sys.exit(EXIT_CODE_PARTIAL_FAILURE)
        else:
            logger.info("All campaigns updated successfully.")

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except FileNotFoundError:
        logger.error(f"File not found: {args.file}")
        sys.exit(EXIT_CODE_FILE_NOT_FOUND)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
