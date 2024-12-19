from dataclasses import dataclass, field, asdict
from typing import List, Dict, Tuple
from shipyard_templates import ShipyardLogger
from shipyard_magnite.dataclasses.budget_item import BudgetItem
from shipyard_magnite.dataclasses.validation_results import ValidationResult


logger = ShipyardLogger.get_logger()


@dataclass
class Budgets:
    items: List[BudgetItem] = field(default_factory=list)
    _validation_results: List[ValidationResult] = field(
        default_factory=list, init=False
    )

    def validate(self) -> Tuple[List[BudgetItem], List[ValidationResult]]:
        """Validate all budget items and ensure there are no duplicate periods."""
        valid_budgets = []
        self._validation_results = []
        seen_periods = set()

        for item in self.items:
            if item.budget_period in seen_periods:
                self._validation_results.append(
                    ValidationResult(
                        status="ERROR",
                        message=f"Duplicate budget period: {item.budget_period}",
                        details=asdict(item),
                    )
                )
                logger.error(f"Duplicate budget period: {item.budget_period}")
                continue
            seen_periods.add(item.budget_period)

            is_valid, message = item.validate()
            if not is_valid:
                self._validation_results.append(
                    ValidationResult(
                        status="ERROR",
                        message=message,
                        details=asdict(item),
                    )
                )
                logger.error(message)
                continue

            valid_budgets.append(item)
            self._validation_results.append(
                ValidationResult(
                    status="OK",
                    message="Valid budget item.",
                    details=asdict(item),
                )
            )

        return Budgets(items=valid_budgets)

    def report_validation_results(self) -> str:
        """Generate a formatted report of validation results."""
        if not self._validation_results:
            self.validate()
        return ValidationResult.report(self._validation_results)

    def to_payload(self) -> List[Dict]:
        """Convert the budget items to a payload for the API."""
        payload = []
        for item in self.items:
            payload.append(item.to_payload())
        return {"budgets": payload}
