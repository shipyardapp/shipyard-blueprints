from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Tuple
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()

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
SUPPORTED_FIELDS = (
    "id",
    "budget_value",
    "budget_period",
    "budget_pacing",
    "budget_metric",
    "vast_caching_adjustment",
    "budget_item_id",
)


@dataclass
class BudgetItem:
    id: Optional[str] = None
    budget_value: float = 0.0
    budget_period: str = None
    budget_pacing: str = None
    budget_metric: str = None
    vast_caching_adjustment: Optional[float] = None
    # TODO: Support budget flight dates(should be a new dataclass)
    # Missing fields: budget_flight_dates, budget_flight_dates_timezone, budget_flight_dates_pacing not needed for V1

    def validate(self) -> Tuple[bool, str]:
        """Validate the individual budget item."""
        if self.budget_metric not in VALID_BUDGET_METRICS:
            return False, f"Invalid budget metric: {self.budget_metric}"

        if self.budget_period not in VALID_BUDGET_PERIODS:
            return False, f"Invalid budget period: {self.budget_period}"

        if self.budget_pacing not in VALID_BUDGET_PACINGS:
            return False, f"Invalid budget pacing: {self.budget_pacing}"

        if self.budget_period == "lifetime" and self.budget_pacing not in {
            "asap",
            "smooth",
        }:
            return False, f"Invalid pacing for lifetime budget: {self.budget_pacing}"

        try:
            float(self.budget_value)
        except ValueError:
            return False, f"Invalid budget value: {self.budget_value}"

        if (
            self.budget_metric in {"revenue", "requests"}
            and self.vast_caching_adjustment is not None
        ):
            return False, "Invalid vast caching adjustment for budget metric"

        return True, "Valid"

    def to_payload(self) -> Dict:
        """Convert the budget item to a payload for the API.

        Notes:
        1. For 'asap' pacing, the API expects an empty string instead of the literal 'asap'.
        2. If no ID is provided, the API interprets an empty string as an attempt to overwrite the ID with an empty value.
        To avoid this, IDs should be removed from the payload if not being used.
        """
        payload = asdict(self)
        if self.budget_pacing == "asap":
            payload["budget_pacing"] = ""
        if self.id == "":
            del payload["id"]
        # Remove keys with None values to avoid unintended overwrites
        payload = {key: value for key, value in payload.items() if value is not None}
        return payload
