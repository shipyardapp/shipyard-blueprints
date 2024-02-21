import pytest
from shipyard_bp_utils import args


@pytest.mark.parametrize(
    "input_string,expected_result",
    [
        ("TRUE", True),
        ("FALSE", False),
        (None, False),
        ("", False),
        ("true", True),
        ("false", False),
        ("True", True),
        ("False", False),
        ("tRuE", True),
        ("fAlSe", False),
        ("true ", True),
        (" false", False),
        (" true", True),
        (" false", False),
        (" true ", True),
    ],
)
def test_convert_to_boolean(input_string, expected_result):
    assert (
        args.convert_to_boolean(input_string) == expected_result
    ), f"Failed for input: {input_string}"


@pytest.mark.parametrize(
    "input_string,default,expected_result",
    [
        (None, True, True),
        ("", False, False),
        ("", True, True),
        ("This is not a boolean", False, False),
        ("This is not a boolean", True, True),
    ],
)
def test_convert_to_boolean_with_default(input_string, default, expected_result):
    assert (
        args.convert_to_boolean(input_string, default) == expected_result
    ), f"Failed for input: {input_string} with default: {default}"
