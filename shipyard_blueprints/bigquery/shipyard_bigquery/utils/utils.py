from typing import Dict


def validate_data_types(data_types: Dict[str, str]) -> bool:
    BIGQUERY_DATA_TYPES = {
        "STRING",
        "BYTES",
        "INT64",
        "FLOAT64",
        "BOOLEAN",
        "TIMESTAMP",
        "DATE",
        "TIME",
        "DATETIME",
        "GEOGRAPHY",
        "JSON",
        "INTERVAL",
        "NUMERIC",
        "BIGNUMERIC",
        "STRUCT",
        "TIME",
    }
    """ Helper function to validate whether the provided data types are real BigQuery data types

    Args:
        data_types: The inputted data types to be used in the upload

    Returns: True if valid, False otherwise
        
    """
    for v in data_types.values():
        if v not in BIGQUERY_DATA_TYPES:
            return False
    return True
