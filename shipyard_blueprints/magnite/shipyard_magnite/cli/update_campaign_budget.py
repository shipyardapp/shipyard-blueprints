import argparse
import sys

from shipyard_templates import ShipyardLogger, ExitCodeException
from shipyard_templates.errors import EXIT_CODE_UNKNOWN_ERROR
from shipyard_magnite import MagniteClient, utils
from shipyard_magnite.errs import EXIT_CODE_UPDATE_ERROR, EXIT_CODE_FILE_NOT_FOUND
from shipyard_magnite.dataclasses.targeting_spend_profile import TargetingSpendProfile


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
    """Process a single campaign's budget."""
    try:
        logger.debug(f"Processing campaign ID: {campaign_id}...")
        budgets = utils.transform_data_to_budgets(budget_data)

        valid_budgets = budgets.validate()
        report = budgets.report_validation_results()

        if len(valid_budgets.items) == 0:
            logger.error(f"No valid budgets found for campaign ID: {campaign_id}")
            return {"errors": [campaign_id], "report": {campaign_id: report}}

        if len(valid_budgets.items) != len(budgets.items):
            logger.warning(
                f"Some budgets are invalid for campaign ID: {campaign_id}. "
                f"Continuing with the following valid budgets:\n"
                f"{chr(10).join(str(budget) for budget in valid_budgets.items)}"
            )

        profile_id = client.get_campaign_by_id(campaign_id)["targeting_spend_profile"][
            "id"
        ]
        spend_profile = TargetingSpendProfile(id=profile_id, budgets=valid_budgets)

        client.update_campaign_budgets(campaign_id, spend_profile)
        logger.info(f"Successfully updated budgets for campaign ID: {campaign_id}")
        return {"errors": [], "report": {campaign_id: report}}

    except Exception as e:
        logger.warning(f"Error processing campaign ID: {campaign_id}\nError: {e}")
        return {"errors": [campaign_id], "report": {campaign_id: f"Error: {e}"}}


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

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)
    except FileNotFoundError:
        logger.error(f"File not found: {args.file}")
        sys.exit(EXIT_CODE_FILE_NOT_FOUND)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)

    logger.info("======= Reports for Successful campaigns =======")
    for report in reports:
        for campaign_id, campaign_report in report.items():
            if campaign_id not in errors:
                logger.info(f"Campaign ID: {campaign_id}\n{campaign_report}")
    if errors:
        logger.error(f"Error(s) occurred for the following campaign(s): {errors}")
        logger.info("======= Reports for Errored Campaigns =======")
        for report in reports:
            for campaign_id, campaign_report in report.items():
                if campaign_id in errors:
                    logger.info(f"Campaign ID: {campaign_id}\n{campaign_report}")
        sys.exit(EXIT_CODE_UPDATE_ERROR)
    else:
        logger.info("All campaigns updated successfully.")


if __name__ == "__main__":
    main()
