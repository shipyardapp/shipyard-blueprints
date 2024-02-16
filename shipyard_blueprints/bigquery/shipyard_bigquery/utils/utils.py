from typing import Dict, Union, List
from shipyard_templates import ShipyardLogger


logger = ShipyardLogger.get_logger()


def validate_data_types(
    data_types: Union[List[List[str]], List[Dict[str, str]]]
) -> bool:
    BIGQUERY_DATA_TYPES = {
        "STRING",
        "BYTES",
        "INT64",
        "FLOAT64",
        "BOOL",
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
        ##NOTE: the following below should be supported too
        "INT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "TINYINT",
        "BYTEINT",
    }
    """ Helper function to validate whether the provided data types are real BigQuery data types

    Args:
        data_types: The inputted data types to be used in the upload

    Returns: True if valid, False otherwise
        
    """
    for item in data_types:
        if isinstance(item, list):
            if str(item[1]).upper() not in BIGQUERY_DATA_TYPES:
                logger.debug(f"Type {item[1]} is not a valid BigQuery data type")
                return False
        elif isinstance(item, dict):
            if str(item["type"]).upper() not in BIGQUERY_DATA_TYPES:
                logger.debug(f"Type {item['type']} is not a valid BigQuery data type")
                return False
    else:
        return True
