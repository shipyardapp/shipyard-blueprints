import math
import re
from numpy import float64, int64
import pandas as pd
import json
from typing import List, Dict, Any, Union
from dataclasses import dataclass

from shipyard_templates import ExitCodeException


# These dataclasses are helpful for dealing with rowise operations, since Notion inserts data 1 row at a time
@dataclass
class DataType:
    name: str
    notion_type: str
    pandas_type: str
    values: Dict[Any, Any]  # this is the payload to send


@dataclass
class Properties:
    payload: Dict[Any, Any]


@dataclass
class DataRow:
    row: int
    # dtypes: List[List[DataType]]
    dtypes: Properties


def form_property_payload(row_payload: List[DataRow]):
    d = {}
    # go through each
    for row in row_payload:
        pass


def guess_type_by_values(values_str: List[str]) -> str:
    unique_values = set(filter(None, values_str))

    match_map = {
        "text": is_empty,
        "checkbox": is_checkbox,
        "number": is_number,
        "url": is_url,
        "email": is_email,
    }

    matches = (
        value_type
        for value_type, match_func in match_map.items()
        if all(map(match_func, unique_values))
    )

    return next(matches, "text")


def is_number(s: str) -> bool:
    try:
        return not math.isnan(float(s))
    except ValueError:
        return False


def is_url(s: str) -> bool:
    return re.match("^https?://", s) is not None


def is_email(s: str) -> bool:
    return re.match(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$", s) is not None


def is_checkbox(s: str) -> bool:
    return s in {"true", "false"}


def is_empty(s: str) -> bool:
    return not s.strip()


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


def create_property_payload(notion: str, value: Any) -> Dict[Any, Any]:
    """Creates the payload for a single value

    Args:
        notion: The notion datatype
        value:  The value from the pandas dataframe

    Returns: The converted payload to load to notion

    """
    if notion == "text":
        return {
            "title": {"text": {"content": value}}
        }  # NOTE: Removed the brackets for the `text` obejct

    if notion == "number":
        return {"number": f"{value}", "number_format": "number"}
    if notion == "datetime":
        # check to see if value is date or datetime
        if type(value) is pd.Timestamp:
            # handle datetime
            return {"date": {"start": value, "end": None, "include_time": True}}
        else:
            # handle dates
            return {"date": {"start": value, "end": None, "include_time": False}}
    if notion == "checkbox":
        return {"checkbox": {"checked": True if value else False}}
    return {
        "title": {"text": {"content": value}}
    }  # NOTE: Removed the brackets for the `text` obejct


def form_row_payload(
    col_name: str, db_properties: Dict[Any, Any], value: Any
) -> Dict[Any, Any]:
    dtype = db_properties.get(col_name).get("type")
    payload = {}
    # go through each case and form the payload accordingly
    if dtype == "rich_text":
        body = {}
        inner = {}
        body["type"] = "rich_text"
        inner["type"] = "text"
        inner["text"] = {"content": value}
        inner["plain_text"] = value
        # ignoring the annotations section
        # payload['rich_text'] = [inner]
        body["rich_text"] = [inner]
        payload[col_name] = body

    elif dtype == "title":
        body = {}
        inner = {}
        body["type"] = "title"
        inner["type"] = "text"
        inner["text"] = {"content": value}
        inner["plain_text"] = value
        # ignoring the annotations section
        # payload['title'] = [inner]
        body["title"] = [inner]
        payload[col_name] = body

    elif dtype == "checkbox":
        inner = {}
        inner["type"] = "checkbox"
        if bool(value):
            inner["checkbox"] = True
        inner["checkbox"] = False
        payload[col_name] = inner

    elif dtype == "number":
        inner = {}
        inner["type"] = "number"
        if type(value) is int64:
            inner["number"] = int(value)  # need to put this as a string
        elif type(value) is float64:
            inner["number"] = float(value)
        payload[col_name] = inner

    elif dtype in ("date", "datetime"):
        body = {}
        inner = {}
        body["type"] = "date"
        inner["start"] = value
        body["date"] = inner
        payload[col_name] = body

    else:
        raise ExitCodeException(
            f"Unsupported data type {value}. Please update the schema in Notion to be either a Title, Rich Text, Number, Checkbox, or Date",
            1,
        )
    return payload


def create_row_payload(
    df: pd.DataFrame, db_properties: Dict[Any, Any]
) -> List[DataRow]:
    """Creates a list of DataRow structs which will be used to load into Notion

    Args:
        df: The dataframe in which to construct the list

    Returns: List of DataRow structs

    """
    datatypes_list = []  # this will be a list of the DataType structs
    columns = df.columns
    dtypes = df.dtypes.to_dict()
    for index in range(len(df)):
        property_payload = {}
        row_list = []  # this will be the size of the number of columns
        for column in columns:
            row_value = df[column].iloc[index]
            pd_dtype = dtypes.get(column)
            notion_type = mapper(str(pd_dtype))
            # NOTE: using the form function instead here
            # payload = create_property_payload(notion_type, row_value)
            payload = form_row_payload(column, db_properties, row_value)
            property_payload = property_payload | payload

            # dt = DataType(name = column,notion_type= notion_type, pandas_type = pd_dtype, values = payload)
            # row_list.append(dt)
        prop = Properties(property_payload)
        # datarow = DataRow(index, row_list)
        datarow = DataRow(index, prop)
        datatypes_list.append(datarow)
    return datatypes_list


def create_column_mapping(properties: Dict[Any, Any]) -> Dict[str, Dict[str, str]]:
    """Returns a mapping of of the column name and notion datatype. An example of the properties object from notion is:
    "properties": {
    "datetime_col": {
        "id": "GhnZ",
        "name": "datetime_col",
        "type": "date",
        "date": {}
    },

    Args:
        properties: The properties object of the database

    Returns: Dictionary with column name as the key, and type as the value

    """
    d = {}
    for property in properties:
        inner = {}
        name = properties[property]["name"]
        dtype = properties[property]["type"]
        inner["type"] = dtype
        d[name] = inner
    return d


def get_type(name: str, db_properties: Dict[Any, Any]) -> Union[str, None]:
    for k in db_properties:
        if k == name:
            return db_properties[k]["type"]
    return None
