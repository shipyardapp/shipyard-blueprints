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


# The below functions were used prior to using dataclasses for validation and organization. Keeping since it could prove useful in the future if we want to handle the raw data directly.
VALID_BUDGET_METRICS = {
    "net_cost",
    "gross_cost",
    "requests",
    "revenue",
    "impressions",
    "completes",
    "clicks",
}
VALID_BUDGET_PERIODS = {"day", "lifetime", "month", "week", "hour"}
VALID_BUDGET_PACINGS = {"asap", "smooth", "front_loaded", "even"}


def validate_budget_data(budget_data: List[Dict]) -> List[Dict]:
    """
    Validates the budget data to ensure that it contains the necessary fields.

    Args:
        budget_data (List[Dict]): Budget data to validate.

    Returns:
        List[Dict]: List of valid budget items.
    """

    def _is_valid_budget_metric(metric: str) -> bool:
        return metric in VALID_BUDGET_METRICS

    def _is_valid_budget_period(period: str) -> bool:
        return period in VALID_BUDGET_PERIODS

    def _is_valid_budget_pacing(pacing: str) -> bool:
        return pacing in VALID_BUDGET_PACINGS

    def _is_valid_budget_value(value) -> bool:
        try:
            float(value)
            return True
        except ValueError:
            return False

    def _check_unique_budget_period(budgets: List[Dict]) -> bool:
        seen_budget_periods = set()
        duplicates = set()
        for budget in budgets:
            budget_period = budget.get("budget_period")
            if budget_period:
                if budget_period in seen_budget_periods:
                    duplicates.add(budget_period)
                else:
                    seen_budget_periods.add(budget_period)
        if duplicates:
            return False
        return True

    def _check_budget_period_pacing(budget_item: Dict) -> bool:
        return not (
            budget_item.get("budget_period") == "lifetime"
            and budget_item.get("budget_pacing") not in {"asap", "smooth"}
        )

    def _validate_vast_caching_adjustment(budget_item: Dict) -> bool:
        metric = budget_item.get("budget_metric")
        adjustment = budget_item.get("vast_caching_adjustment")
        if metric in {"revenue", "requests"} and adjustment:
            logger.error(
                f"Budget item {budget_item.get('id')} has an invalid Ad caching adjustment. Skipping..."
            )
            return False
        return True

    def remove_empty_id(budget_item: Dict) -> Dict:
        """This is needed because the API does not accept empty strings for IDs. Particullary when using UPSERT method."""
        if budget_item.get("id") == "":
            del budget_item["id"]
        return budget_item

    if not _check_unique_budget_period(budget_data):
        errs.InvalidBudgetPayload("Budget data has conflicting budget periods.")
        return []

    valid_keys = {
        "id",
        "budget_value",
        "budget_period",
        "budget_pacing",
        "budget_metric",
        "vast_caching_adjustment",
    }
    valid_budgets = []
    item_results = []
    for budget_item in budget_data:
        result = {
            "status": "Validation Incomplete",
            "campaign_id": budget_item.get("campaign_id"),
            "budget_value": budget_item.get("budget_value"),
            "budget_period": budget_item.get("budget_period"),
            "budget_pacing": budget_item.get("budget_pacing"),
            "budget_metric": budget_item.get("budget_metric"),
            "vast_caching_adjustment": budget_item.get("vast_caching_adjustment"),
        }

        invalid_keys = [key for key in budget_item if key not in valid_keys]
        if invalid_keys:
            result["status"] = "error: invalid keys"
            logger.error(
                f"Budget item {budget_item} contains invalid keys: {invalid_keys}. Skipping..."
            )
            item_results.append(result)
            continue

        if not _is_valid_budget_metric(budget_item.get("budget_metric")):
            result["status"] = "error: invalid budget metric"
            logger.error(
                f"Invalid budget metric in item {budget_item.get('id')}. Skipping..."
            )
            item_results.append(result)
            continue

        if not _is_valid_budget_period(budget_item.get("budget_period")):
            result["status"] = "error: invalid budget period"
            logger.error(
                f"Invalid budget period in item {budget_item.get('id')}. Skipping..."
            )
            item_results.append(result)
            continue

        if not _is_valid_budget_pacing(budget_item.get("budget_pacing")):
            result["status"] = "error: invalid budget pacing"
            logger.error(f"Invalid budget pacing in item {budget_item}. Skipping...")
            item_results.append(result)
            continue

        if not _is_valid_budget_value(budget_item.get("budget_value")):
            result["status"] = "error: invalid budget value"
            logger.error(f"Invalid budget value in item {budget_item} Skipping...")
            item_results.append(result)
            continue

        if not _validate_vast_caching_adjustment(budget_item):
            result["status"] = "error: invalid vast caching adjustment"
            has_errors = True
            item_results.append(result)
            continue

        if not _check_budget_period_pacing(budget_item):
            result["status"] = "error: invalid budget period pacing"
            logger.error(
                f"Invalid pacing for lifetime budget in item {budget_item.get('id')}. Skipping..."
            )
            item_results.append(result)
            continue

        if budget_item.get("budget_pacing") == "asap":
            # In Magnite, "asap" pacing is an option in the UI but is represented as an empty string in the payload
            budget_item["budget_pacing"] = ""

        budget_item = remove_empty_id(budget_item)
        valid_budgets.append(budget_item)
        result["status"] = "success"
        item_results.append(result)

    return valid_budgets, item_results


def make_campaign_budget_payload_from_campaign_data(
    campaign_data: Dict,
    budget_data: List[Dict],
    update_method: str = "UPSERT",
) -> Union[List[Dict]]:
    """
    Prepares a campaign budget payload based on the update method.

    Args:
        campaign_data (Dict): Existing campaign data containing targeting spend profiles.
        budget_data (List[Dict]): New budget data to be processed.
        update_method (str): Update method - one of "UPSERT", "OVERWRITE", or "INSERT".
        **kwargs: Additional optional arguments.

    Returns:
        List[Dict] or None: Processed budget data based on the update method.
    """
    valid_methods = ["UPSERT", "REPLACE", "INSERT"]
    validated_budget_data, has_errors = validate_budget_data(budget_data)
    if update_method not in valid_methods:
        raise ValueError(f"Invalid update method. Valid methods are {valid_methods}")

    if update_method == "REPLACE":
        return validated_budget_data, has_errors

    campaign_budget_data = campaign_data.get("targeting_spend_profile", {}).get(
        "budgets", []
    )
    campaign_budget_lookup = {
        budget.get("id"): budget for budget in campaign_budget_data
    }

    formatted_budget_data = (
        [] if update_method == "INSERT" else campaign_budget_data.copy()
    )

    for budget_item in validated_budget_data:
        budget_id = budget_item.get("id")

        if not budget_id:
            formatted_budget_data.append(budget_item)
        elif budget_id in campaign_budget_lookup:
            if update_method == "INSERT":
                logger.warning(
                    f"Budget item with ID {budget_id} already exists in campaign data. Skipping..."
                )
            elif update_method == "UPSERT":
                campaign_budget_lookup[budget_id].update(budget_item)
        else:
            formatted_budget_data.append(budget_item)

    if update_method == "UPSERT":
        formatted_budget_data.extend(campaign_budget_lookup.values())

    return formatted_budget_data, has_errors


def make_campaign_budget_payload(
    budget_value: Union[str, float, int],
    campaign_data: Dict[str, Any],
    **kwargs,
):
    """

    Args:
        budget_value:
        **kwargs:

    Returns:

    """
    data = {
        "targeting_spend_profile": {
            "id": campaign_data.get("targeting_spend_profile").get("id"),
            "budgets": [
                {
                    "id": campaign_data.get("targeting_spend_profile")
                    .get("budgets")[0]
                    .get("id"),
                    "budget_value": budget_value,
                }
            ],
        }
    }
    if budget_pacing := kwargs.get("budget_pacing"):
        data["targeting_spend_profile"]["budgets"][0]["budget_pacing"] = budget_pacing
    if budget_period := kwargs.get("budget_period"):
        data["targeting_spend_profile"]["budgets"][0]["budget_period"] = budget_period
    if budget_metric := kwargs.get("budget_metric"):
        data["targeting_spend_profile"]["budgets"][0]["budget_period"] = budget_metric

    return data
