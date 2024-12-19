from dataclasses import dataclass, field, asdict
from typing import List, Dict, Tuple
from shipyard_templates import ShipyardLogger
from shipyard_magnite.dataclasses.budgets import Budgets
from shipyard_magnite.dataclasses.validation_results import ValidationResult
from shipyard_magnite.dataclasses.budget_item import BudgetItem

logger = ShipyardLogger.get_logger()


@dataclass
class TargetingSpendProfile:
    id: int
    budgets: Budgets
    # Missing fields: "budget_timezone", "created_at" not needed for V1

    def validate(self) -> Tuple[List[BudgetItem], List[ValidationResult]]:
        """
        Validate all budgets in the spend profile and ensure they are valid.
        """
        logger.info(f"Validating budgets for TargetingSpendProfile ID {self.id}")
        valid_budgets, validation_results = self.budgets.validate()
        if not valid_budgets:
            logger.warning(
                f"No valid budgets found for TargetingSpendProfile ID {self.id}"
            )
        return valid_budgets, validation_results

    def report_validation_results(self) -> str:
        """
        Generate a formatted report of validation results for the spend profile.
        """
        logger.info(
            f"Generating validation report for TargetingSpendProfile ID {self.id}"
        )
        return self.budgets.report_validation_results()

    def to_payload(self) -> Dict:
        """
        Convert the spend profile and its budgets into a payload for the API.
        """
        logger.debug(f"Converting TargetingSpendProfile ID {self.id} to payload")
        payload = {"id": self.id, **self.budgets.to_payload()}

        logger.debug(f"Payload for TargetingSpendProfile ID {self.id}: {payload}")
        return {"targeting_spend_profile": payload}
