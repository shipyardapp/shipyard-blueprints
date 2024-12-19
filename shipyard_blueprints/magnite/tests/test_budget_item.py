import pytest
from dataclasses import asdict
from shipyard_magnite.dataclasses.budget_item import BudgetItem


def test_validate_valid_budget_item():
    """Test validation of a valid budget item."""
    budget_item = BudgetItem(
        id="1",
        budget_value=100.0,
        budget_period="day",
        budget_pacing="smooth",
        budget_metric="clicks",
    )
    is_valid, message = budget_item.validate()
    assert is_valid
    assert message == "Valid"


def test_validate_invalid_metric():
    """Test validation fails for an invalid budget metric."""
    budget_item = BudgetItem(
        id="1",
        budget_value=100.0,
        budget_period="day",
        budget_pacing="smooth",
        budget_metric="invalid_metric",
    )
    is_valid, message = budget_item.validate()
    assert not is_valid
    assert message == "Invalid budget metric: invalid_metric"


def test_validate_invalid_period():
    """Test validation fails for an invalid budget period."""
    budget_item = BudgetItem(
        id="1",
        budget_value=100.0,
        budget_period="yearly",
        budget_pacing="smooth",
        budget_metric="clicks",
    )
    is_valid, message = budget_item.validate()
    assert not is_valid
    assert message == "Invalid budget period: yearly"


def test_validate_invalid_pacing():
    """Test validation fails for an invalid budget pacing."""
    budget_item = BudgetItem(
        id="1",
        budget_value=100.0,
        budget_period="day",
        budget_pacing="fast",
        budget_metric="clicks",
    )
    is_valid, message = budget_item.validate()
    assert not is_valid
    assert message == "Invalid budget pacing: fast"


def test_validate_invalid_lifetime_pacing():
    """Test validation fails for an invalid pacing with a lifetime budget period."""
    budget_item = BudgetItem(
        id="1",
        budget_value=100.0,
        budget_period="lifetime",
        budget_pacing="fast",
        budget_metric="clicks",
    )
    is_valid, message = budget_item.validate()
    assert not is_valid
    assert message == "Invalid budget pacing: fast"


def test_validate_invalid_budget_value():
    """Test validation fails for an invalid budget value."""
    budget_item = BudgetItem(
        id="1",
        budget_value="invalid_value",
        budget_period="day",
        budget_pacing="smooth",
        budget_metric="clicks",
    )
    is_valid, message = budget_item.validate()
    assert not is_valid
    assert message == "Invalid budget value: invalid_value"


def test_validate_invalid_vast_caching_adjustment():
    """Test validation fails for an invalid vast caching adjustment."""
    budget_item = BudgetItem(
        id="1",
        budget_value=100.0,
        budget_period="day",
        budget_pacing="smooth",
        budget_metric="revenue",
        vast_caching_adjustment=True,
    )
    is_valid, message = budget_item.validate()
    assert not is_valid
    assert message == "Invalid vast caching adjustment for budget metric"


# Test cases for BudgetItem Payload prep


def test_to_payload_pacing_asap():
    """Test cleaning changes 'asap' pacing to an empty string."""
    budget_item = BudgetItem(
        id="1",
        budget_value=100.0,
        budget_period="day",
        budget_pacing="asap",
        budget_metric="clicks",
    )
    payload = budget_item.to_payload()
    assert payload["budget_pacing"] == ""
    assert payload == {
        "id": "1",
        "budget_value": 100.0,
        "budget_period": "day",
        "budget_pacing": "",
        "budget_metric": "clicks",
    }


def test_to_payload_empty_id():
    """Test removing an empty string ID from the payload."""
    budget_item = BudgetItem(
        id="",
        budget_value=100.0,
        budget_period="day",
        budget_pacing="smooth",
        budget_metric="clicks",
    )
    payload = budget_item.to_payload()
    assert "id" not in payload


def test_to_payload_None_id_removed():
    """Test removing a None ID from the payload."""
    budget_item = BudgetItem(
        id=None,
        budget_value=100.0,
        budget_period="day",
        budget_pacing="smooth",
        budget_metric="clicks",
    )
    payload = budget_item.to_payload()
    assert "id" not in payload
