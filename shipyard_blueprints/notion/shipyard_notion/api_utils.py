import requests
import pandas as pd
from typing import Dict, Any, List


def mapper(pandas_dtype: str) -> str:
    """Helper function to map a pandas datatype to a notion datatype

    Args:
        pandas_dtype: The datatype of the pandas column

    Returns: The notion datatype (in form a string)

    """
    if pandas_dtype == "bool":
        return "checkbox"
    if pandas_dtype in ("int64", "float64"):
        return "number"

    if "datetime64" in pandas_dtype:
        return "datetime"
    return "text"


def convert_pandas_to_notion(df: pd.DataFrame) -> Dict[str, str]:
    """Helper function to map the pandas datatypes to the equivalent Notion types. This will output a dictionary
    with the Column name as the keys and the Notion datatype as the values

    Args:
        df: The pandas dataframe

    Returns: Dictionary of the mapped types

    """
    dtypes = df.dtypes
    cols = df.columns
    mapped_dtypes = {}

    for c, d in zip(cols, dtypes):
        mapped_dtypes[c] = mapper(d)

    return mapped_dtypes

def create_properties_payload(df: pd.DataFrame, name: str) -> List[Dict[Any, Any]]:
    """Creates a list of payloads to create notion pages based off of the data provided from a pandas dataframe

    Args:
        data: The pandas dataframe

    Returns: The list of payload objects, where the first element is the payload for the first row, and so on.

    """
    columns = df.columns
    # set the fields for the properties
    all_data = []
    dtypes = df.dtypes
    for index, row in df.iterrows():
        data = {}
        for column in columns:
            notion_dtype = mapper(str(dtypes[column]))
            value = row[column]
            if notion_dtype == "text":
                data[str(column)] = {"title": [{"text": {"content": value}}]}
            if notion_dtype == "number":
                data[str(column)] = {"number": value, "number_format": "number"}
            if notion_dtype == "datetime":
                # check to see if value is date or datetime
                if type(value) is pd.Timestamp:
                    # handle datetime
                    data[str(column)] = {
                        "date": {"start": value, "end": None, "include_time": True}
                    }
                else:
                    # handle dates
                    data[str(column)] = {
                        "date": {"start": value, "end": None, "include_time": False}
                    }
            if notion_dtype == "checkbox":
                data[str(column)] = {"checkbox": {"checked": True if value else False}}
            all_data.append(data)

    return all_data
