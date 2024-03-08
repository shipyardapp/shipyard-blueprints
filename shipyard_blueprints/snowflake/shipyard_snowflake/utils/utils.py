import re
import psutil
import os
import pandas as pd
from datetime import datetime
from dask import dataframe as dd
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization.ssh import serialize_ssh_public_key
from typing import Dict, List, Optional, Tuple, Union
from copy import deepcopy
from random import random, randrange
from itertools import islice
from io import StringIO
from math import exp, log, floor, ceil


def _get_file_size(file: str) -> int:
    """Helper function to get the size of a given file

    Args:
        file: The desired file to grab the size of

    Returns: The size in bytes
    """
    return os.path.getsize(file)


def _get_memory() -> int:
    """Helper function to get the available memory

    Returns: The amount of memory in bytes

    """
    return psutil.virtual_memory().total


def _file_fits_in_memory(file: str) -> bool:
    """Helper function to show if a file can be read into memory

    Args:
        file: The desired file to be read in

    Returns: True if the file can fit in memory and False otherwise

    """
    if _get_file_size(file) < _get_memory():
        return True
    return False


def _decode_rsa(rsa_key: str):
    try:
        with open(rsa_key, "rb") as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"].encode(),
                backend=default_backend(),
            )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        return pkb

    except Exception as e:
        return None


def map_snowflake_to_pandas(
    snowflake_data_types: Optional[Union[List[List], Dict[str, str]]]
) -> Union[Dict, None]:
    # TODO: modify this to accept a list of lists (old way) and a JSON representation of datatypes (new way)
    """Helper function to map a snowflake data type to the associated pandas data type

    Args:
        List

    Returns:
        dict | None: Dict where the key is the field name and the value is the pandas data type
    """
    if snowflake_data_types is None:
        return None
    snowflake_to_pandas = {
        "BOOLEAN": "bool",
        "TINYINT": "int8",
        "SMALLINT": "int16",
        "INTEGER": "int32",
        "INT": "int32",
        "BIGINT": "int64",
        "FLOAT": "float32",
        "DOUBLE": "float64",
        "DECIMAL": "float64",
        "NUMERIC": "float64",
        "NUMBER": "float64",
        "REAL": "float32",
        "DATE": "datetime64[ns]",
        "TIME": "datetime64[ns]",
        "DATETIME": "datetime64[ns]",
        "TIMESTAMP": "datetime64[ns]",
        "VARCHAR": "object",
        "NVARCHAR": "object",
        "CHAR": "object",
        "NCHAR": "object",
        "BINARY": "object",
        "VARBINARY": "object",
        "STRING": "object",
    }

    pandas_dtypes = {}
    if isinstance(snowflake_data_types, list):
        for item in snowflake_data_types:
            field = item[0]
            dtype = item[1]
            try:
                converted = snowflake_to_pandas[str(dtype).upper()]
                if converted is None:
                    raise Exception(
                        f"Invalid datatypes: the datatype {dtype} is not a recognized snowflake datatype"
                    )

                pandas_dtypes[field] = converted
            except KeyError as e:
                raise Exception(
                    f"Invalid datatype: the datatype {dtype} is not a recognized snowflake datatype"
                )
    elif isinstance(snowflake_data_types, dict):
        for field, dtype in snowflake_data_types.items():
            try:
                converted = snowflake_to_pandas[str(dtype).upper()]
                if converted is None:
                    raise Exception(
                        f"Invalid datatypes: the datatype {dtype} is not a recognized snowflake datatype"
                    )

                pandas_dtypes[field] = converted
            except KeyError as e:
                raise Exception(
                    f"Invalid datatype: the datatype {dtype} is not a recognized snowflake datatype"
                )
    else:
        raise Exception(
            "Unsupported format. Supplied datatypes should be either an array of arrays, or JSON "
        )

    return pandas_dtypes


def get_pandas_dates(pandas_datatypes: dict) -> tuple:
    dates = []
    new_dict = deepcopy(pandas_datatypes)
    for k, v in pandas_datatypes.items():
        if v in ["datetime64[ns]", "datetime64"]:
            dates.append(k)
            del new_dict[k]
    return (dates, new_dict) if dates else (None, new_dict)


def read_file(
    file: str, snowflake_dtypes: Union[List, None] = None, file_type: str = "csv"
) -> pd.DataFrame:
    """Helper function to read in a file to a pandas dataframe. This will be build out in the future to allow for more file types like parquet, arrow, tsv, etc.
    Args:
        file (str): The file to be read in as a dataframe
        pandas_dtypes (Union[Dict,None]): The optional dictionary of pandas data types to be used when reading in the file. Defaults to None
        file_type (str): The type of file to be read in. Defaults to 'csv'

    Returns:
        pd.DataFrame: The dataframe output of the file
    """
    if snowflake_dtypes:
        pandas_dtypes = map_snowflake_to_pandas(snowflake_dtypes)
        dates, pandas_dtypes = get_pandas_dates(
            pandas_dtypes
        )  # get the dates to be parsed

    else:
        pandas_dtypes = None
        dates = None
    if _file_fits_in_memory(file):
        # if it fits in memory
        if file_type == "csv":  # alwys true for now, will be augmented in the future
            if dates:
                df = pd.read_csv(
                    file,
                    dtype=pandas_dtypes,
                    parse_dates=dates,
                    date_parser=lambda x: pd.to_datetime(x).tz_localize("UTC"),
                )
            else:
                df = pd.read_csv(file, dtype=pandas_dtypes)
    else:
        # if the file is larger than memory
        if dates:
            df = dd.read_csv(
                file,
                dtype=pandas_dtypes,
                parse_dates=dates,
                date_parser=lambda x: pd.to_datetime(x).tz_localize("UTC"),
            )
        else:
            df = dd.read_csv("file", dtype=pandas_dtypes)
    return df


def reservoir_sample(iterable, k=1):
    """Select k items uniformly from iterable.
    Returns the whole population if there are k or fewer items

    """
    iterator = iter(iterable)
    values = list(islice(iterator, k))

    W = exp(log(random()) / k)
    while True:
        # skip is geometrically distributed
        skip = floor(log(random()) / log(1 - W))
        selection = list(islice(iterator, skip, skip + 1))
        if selection:
            values[randrange(k)] = selection[0]
            W *= exp(log(random()) / k)
        else:
            return values


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        # Check if the column contains date-like values
        if df[col].apply(lambda x: bool(re.match(r"\d{4}-\d{2}-\d{2}", str(x)))).all():
            df[col] = pd.to_datetime(df[col])

    return df


def infer_schema(file_name: str, k=10000) -> Dict[str, str]:
    """Randomly samples a csv file and infers the schema based off of the k rows sampled

    Args:
        file_name (str): The file to be sampled
        k (int, optional): The number of rows to sample. Defaults to 10000.

    Returns:
        dict: The dictionary of inferred pandas datatypes data types
    """
    if isinstance(file_name, list):
        dataframes = []
        n_files = len(file_name)
        rows_per_file = ceil(k / n_files)
        for f in file_name:
            file_path = f
            with open(file_path, "r") as f:
                header = next(f)
                result = [header] + reservoir_sample(f, rows_per_file)
            df = pd.read_csv(StringIO("".join(result)))
            dataframes.append(df)
        merged = pd.concat(dataframes, axis=0, ignore_index=True)
        return merged.dtypes.to_dict()
    else:
        with open(file_name, "r") as f:
            header = next(f)
            result = [header] + reservoir_sample(f, k)
        df = pd.read_csv(StringIO("".join(result)), parse_dates=True)
        df = _parse_dates(df)  # parse the date and datetime fields
        return {k: str(v) for k, v in df.dtypes.to_dict().items()}


def map_pandas_to_snowflake(data_type_dict: Dict[str, str]) -> Dict[str, str]:
    """Helper function to map the the pandas data types to the snowflake data types

    Args:
        pandas_dtypes (dict): Dictionary of the pandas data types

    Returns:
        dict: Dictionary of the mapped snowflake data types
    """
    snowflake_data_types = {
        "int64": "NUMBER",
        "float64": "FLOAT",
        "object": "STRING",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
        "timedelta64[ns]": "TIME",
    }

    snowflake_columns = {}
    for column_name, pandas_data_type in data_type_dict.items():
        snowflake_columns[column_name] = snowflake_data_types.get(
            str(pandas_data_type), "STRING"
        )

    return snowflake_columns
