import pytest
from shipyard_magnite.dataclasses.budget_item import BudgetItem
from shipyard_magnite.dataclasses.budgets import Budgets
from shipyard_magnite.dataclasses.validation_results import ValidationResult


@pytest.fixture
def valid_daily_budget():
    return BudgetItem(
        id="1",
        budget_value=100.0,
        budget_period="day",
        budget_pacing="smooth",
        budget_metric="clicks",
    )


@pytest.fixture
def valid_weekly_budget():
    return BudgetItem(
        id="2",
        budget_value=200.0,
        budget_period="week",
        budget_pacing="asap",
        budget_metric="impressions",
    )


@pytest.fixture
def invalid_metric_budget():
    return BudgetItem(
        id="3",
        budget_value=150.0,
        budget_period="month",
        budget_pacing="asap",
        budget_metric="invalid",
    )


def test_validate_with_valid_budgets(valid_daily_budget, valid_weekly_budget):
    """Test validation with all valid budgets."""
    budgets = Budgets(items=[valid_daily_budget, valid_weekly_budget])

    valid_budgets = budgets.validate()
    validation_results = budgets._validation_results

    assert len(valid_budgets.items) == 2
    assert all(result.status == "OK" for result in validation_results)


def test_validate_with_invalid_metric_budget(valid_daily_budget, invalid_metric_budget):
    """Test validation with a budget having an invalid metric."""
    budgets = Budgets(items=[valid_daily_budget, invalid_metric_budget])

    valid_budgets = budgets.validate()
    validation_results = budgets._validation_results

    # Assert only the valid budget is returned
    assert len(valid_budgets.items) == 1
    assert len(validation_results) == 2
    assert validation_results[1].status == "ERROR"
    assert "Invalid budget metric" in validation_results[1].message


def test_validate_with_duplicate_period_budget(valid_daily_budget, valid_weekly_budget):
    """Test validation with a duplicate budget period."""
    budgets = Budgets(
        items=[valid_daily_budget, valid_weekly_budget, valid_daily_budget]
    )
    valid_budgets = budgets.validate()
    validation_results = budgets._validation_results

    # Assert only the unique budgets are returned
    assert len(valid_budgets.items) == 2
    assert len(validation_results) == 3

    # Check for duplicate period error
    assert validation_results[2].status == "ERROR"
    assert "Duplicate budget period" in validation_results[2].message


def test_report_validation_results(
    valid_daily_budget, valid_weekly_budget, invalid_metric_budget
):
    """Test report generation for validation results."""
    budgets = Budgets(
        items=[
            valid_daily_budget,
            valid_weekly_budget,
            invalid_metric_budget,
            valid_weekly_budget,
        ]
    )
    report = budgets.report_validation_results()

    # Assert report contains key validation messages
    assert "Duplicate budget period" in report
    assert "Invalid budget metric" in report
    assert "Valid budget item" in report
