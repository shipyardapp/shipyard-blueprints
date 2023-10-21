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
class Properties:
    payload: Dict[Any, Any]


@dataclass
class DataRow:
    row: int
    dtypes: Properties


def flatten_json(json_data: List[Dict[Any, Any]]) -> Dict[str, List[Any]]:
    """Helper function to get data returned from NotionClient.fetch() to be in a readable format.
    Format will result in {Column1: [Values],
                           Column2: [Values]}

    Args:
        json_data: The json response data from the Notion API for the contents of the database
    """
    master = {}
    for dictionary in json_data:
        d = {}
        try:
            for results in dictionary:
                properties = results["properties"]
                for (
                    property
                ) in properties:  # this will grab the key (column) for each property
                    nested = properties[property]
                    column_type = nested["type"]
                    # initialize the values of the return dictionary to an empty list
                    if not d.get(property):
                        d[property] = []
                    if column_type == "date":
                        d[property].append(nested.get("date").get("start"))
                    if column_type == "number":
                        d[property].append(nested.get("number"))
                    if column_type == "rich_text":
                        inner_array = nested.get("rich_text")
                        if len(inner_array) > 0:
                            d[property].append(
                                inner_array[0].get("text").get("content")
                            )
                        else:
                            d[property].append(
                                None
                            )  # need to add None so that all the lists will be the same length

                    if column_type == "checkbox":
                        d[property].append(nested.get("checkbox"))
                    if column_type == "title":
                        inner_array = nested.get("title")
                        if len(inner_array) > 0:
                            d[property].append(
                                inner_array[0].get("text").get("content")
                            )
                        else:
                            d[property].append(
                                None
                            )  # need to add None so that all the lists will be the same length

                    if column_type == "multi_select":
                        vals = [x.get("name") for x in nested.get("multi_select")]
                        d[property].append(vals)
                    if column_type == "select":
                        d[property].append(nested.get("select").get("name"))
                    if column_type == "url":
                        d[property].append(nested.get("url"))
                    if column_type == "files":
                        d[property].append(nested.get("name"))
                    if column_type == "email":
                        d[property].append(nested.get("email"))

                    if column_type == "status":
                        d[property].append(nested.get("status").get("name"))

                    if column_type == "people":
                        vals = [
                            x.get("person").get("email") for x in nested.get("people")
                        ]
                        d[property].append(vals)

                    if column_type == "formula":
                        d[property].append(nested.get("formula").get("string"))

                    if column_type == "phone_number":
                        d[property].append(nested.get("phone_number"))
        except Exception as e:
            raise ExitCodeException(str(e), 1)
        else:
            for d_key, d_val in d.items():
                if master.get(d_key):
                    master[d_key].extend(d_val)
                else:
                    master[d_key] = d_val
    return master


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
    pandas_dtype = str(pandas_dtype)
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
        # need to parse the numpy floats/ints in order to serialize them
        if type(value) is int64:
            inner["number"] = int(value)
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

    elif dtype == "multi_select":
        inner = {}
        inner["type"] = "multi_select"
        multi_list = []
        list_values = eval(value)  # need to parse the list of selected values
        if len(list_values) > 0:
            for val in list_values:
                select_values = {}
                select_values["name"] = val
                multi_list.append(select_values)
        inner["multi_select"] = list_values
        payload[col_name] = inner

    elif dtype == "select":
        inner = {}
        inner["type"] = "select"
        select_dict = {}
        select_dict["name"] = value
        inner["select"] = select_dict
        payload[col_name] = inner

    elif dtype == "url":
        inner = {}
        inner["type"] = "url"
        inner["url"] = value
        payload[col_name] = inner

    elif dtype == "email":
        inner = {}
        inner["type"] = "email"
        inner["email"] = value
        payload[col_name] = inner

    elif dtype == "status":
        inner = {}
        inner["type"] = "status"
        status_dict = {}
        status_dict["name"] = value
        inner["status"] = status_dict
        payload[col_name] = inner

    elif dtype == "phone_number":
        inner = {}
        inner["type"] = "phone_number"
        inner["phone_number"] = value
        payload[col_name] = inner

    else:
        raise ExitCodeException(
            f"Unsupported data type {dtype}. At this time, the following types are not supported for upload: Files, Rollup, Relation, People",
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
            payload = form_row_payload(column, db_properties, row_value)
            property_payload = property_payload | payload

        prop = Properties(property_payload)
        datarow = DataRow(index, prop)
        datatypes_list.append(datarow)
    return datatypes_list
