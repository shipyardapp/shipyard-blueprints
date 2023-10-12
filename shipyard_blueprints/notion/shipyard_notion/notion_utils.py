import math
import re
import pandas as pd
from typing import List, Dict

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

