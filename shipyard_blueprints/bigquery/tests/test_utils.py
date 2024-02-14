from shipyard_bigquery import BigQueryClient
from shipyard_bigquery.utils import utils

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


def test_legacy_schema_correct():
    example = [
        ["string_col", "string"],
        ["char_col", "string"],
        ["int_col", "INT64"],
        ["float_col", "FLOAT64"],
        ["bool_col", "bool"],
        ["date_col", "date"],
        ["datetime_col", "datetime"],
    ]
    res = utils.validate_data_types(example)
    assert res is True


def test_legacy_schema_incorrect():
    example = [
        ["string_col", "string"],
        ["char_col", "Char"],
        ["int_col", "INT64"],
        ["float_col", "FLOAT64"],
        ["bool_col", "bool"],
        ["date_col", "date"],
        ["datetime_col", "datetime"],
    ]
    res = utils.validate_data_types(example)
    assert res is False


def test_new_schema_correct():
    example = [
        {"name": "string_col", "type": "string"},
        {"name": "char_col", "type": "string"},
        {"name": "int_col", "type": "Int64"},
        {"name": "float_col", "type": "Float64"},
        {"name": "bool_col", "type": "Bool"},
        {"name": "date_col", "type": "Date"},
        {"name": "datetime_col", "type": "Datetime"},
    ]
    res = utils.validate_data_types(example)
    assert res is True


def test_new_schema_incorrect():
    example = [
        {"name": "string_col", "type": "string"},
        {"name": "char_col", "type": "char"},
        {"name": "int_col", "type": "Int64"},
        {"name": "float_col", "type": "Float64"},
        {"name": "bool_col", "type": "Bool"},
        {"name": "date_col", "type": "Date"},
        {"name": "datetime_col", "type": "Datetime"},
    ]
    res = utils.validate_data_types(example)
    assert res is False
