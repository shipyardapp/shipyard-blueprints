from shipyard_templates import ShipyardLogger
from collections import defaultdict
from shipyard_magnite import errs
import csv
from shipyard_magnite.dataclasses.budget_item import BudgetItem
from shipyard_magnite.dataclasses.budgets import Budgets

from typing import Dict, Any, Union, List

logger = ShipyardLogger.get_logger()


def open_csv(file_path: str) -> List[Dict]:
    """
    Opens a CSV file and returns the data as a list of dictionaries.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        List[Dict]: The data from the CSV file.
    """
    with open(file_path, "r") as file:
        data = [{k: v for k, v in row.items()} for row in csv.DictReader(file)]
    return data


def group_budget_data_by_campaign(budget_data):
    """useful because it groups the budget data by campaign_id, which is needed to update a campaigns budget at once"""
    logger.debug(f"Grouping budget data by campaign...")
    campaigns = defaultdict(list)
    for budget_line in budget_data:
        logger.debug(f"Processing line: {budget_line}")
        try:
            campaign_id = budget_line.pop("campaign_id")
            logger.debug(f"Adding line to campaign {campaign_id}")
            campaigns[campaign_id].append(budget_line)
        except KeyError:
            logger.error(f"Missing 'campaign_id' in line: {budget_line}")

    results = dict(campaigns)
    logger.debug(f"Grouped campaigns: {results}")
    return results


def transform_data_to_budget_item(raw_data):
    """
    Transforms a dictionary of data into a BudgetItem object.

    Args:
        data (Dict): A dictionary of data.

    Returns:
        BudgetItem: A BudgetItem object.
    """
    return BudgetItem(
        id=raw_data.get("id") or raw_data.get("budget_item_id"),
        budget_value=raw_data.get("budget_value"),
        budget_period=raw_data.get("budget_period"),
        budget_pacing=raw_data.get("budget_pacing"),
        budget_metric=raw_data.get("budget_metric"),
        vast_caching_adjustment=raw_data.get("vast_caching_adjustment"),
    )


def transform_data_to_budgets(raw_data):
    """
    Transforms a list of dictionaries into a Budgets object.

    Args:
        data (List[Dict]): A list of dictionaries.

    Returns:
        Budgets: A Budgets object.
    """
    return Budgets(items=[transform_data_to_budget_item(item) for item in raw_data])


def log_campaign_report(reports, errors, description):

    if len(reports) - len(errors) > 0:
        logger.info(f"======= Reports for Successful {description}(s) =======")
        for report in reports:
            for key, report in report.items():
                if key not in errors:
                    logger.info(f"{description}: {key}\n{report}")
    if errors:
        logger.error(f"Error(s) occurred for the following {description}(s): {errors}")
        logger.info(f"======= Reports for Errored {description} =======")
        for report in reports:
            for key, report in report.items():
                if key in errors:
                    logger.info(f"{description}: {key}\n{report}")

    pass
