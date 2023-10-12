import requests
import pandas as pd
from typing import Dict, Any, List
from shipyard_notion.notion_utils import convert_pandas_to_notion, mapper


def create_properties_payload(df: pd.DataFrame) -> List[Dict[Any, Any]]:
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
