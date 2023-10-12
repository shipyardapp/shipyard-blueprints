import math
import re
import pandas as pd
from typing import List, Dict, Any
from dataclasses import dataclass


# These dataclasses are helpful for dealing with rowise operations, since Notion inserts data 1 row at a time
@dataclass
class DataType:
    name: str
    notion_type: str
    pandas_type: str
    values: Dict[Any,Any] # this is the payload to send

@dataclass 
class DataRow:
    row: int
    dtypes: List[List[DataType]]



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

def create_property_payload(notion:str, value:Any) -> Dict[Any,Any]:
    """ Creates the payload for a single value

    Args:
        notion: The notion datatype 
        value:  The value from the pandas dataframe

    Returns: The converted payload to load to notion 
        
    """
    if notion == "text":
        return {"title": [{"text": {"content": value}}]}
    if notion == "number":
        return {"number": value, "number_format": "number"}
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
    return {"title": [{"text": {"content": value}}]}

def create_row_payload(df: pd.DataFrame) -> List[DataRow]:
    datatypes_list = [] # this will be a list of the DataType structs
    columns = df.columns
    dtypes = df.dtypes.to_dict()
    for index in range(len(df)):
        row_list = [] # this will be the size of the number of columns
        for column in columns:
            row_value = df[column].iloc[index]
            pd_dtype = dtypes.get(column)
            notion_type = mapper(str(pd_dtype))
            payload = create_property_payload(notion_type, row_value)

            dt = DataType(name = column,notion_type= notion_type, pandas_type = pd_dtype, values = payload)
            row_list.append(dt)
        datarow = DataRow(index, row_list) 
        datatypes_list.append(datarow)
    return datatypes_list





