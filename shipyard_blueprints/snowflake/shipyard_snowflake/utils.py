import psutil
import os
import pandas as pd
from dask import dataframe as dd
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization.ssh import serialize_ssh_public_key
from typing import Dict, List, Optional, Tuple, Union
from copy import deepcopy


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


def map_snowflake_to_pandas(snowflake_data_types: List[List]) -> Union[Dict, None]:
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
    for item in snowflake_data_types:
        field = item[0]
        dtype = item[1]
        try:
            converted = snowflake_to_pandas[str(dtype).upper()]
            if converted is None:
                raise Exception(
                    f"Invalid datatypes: the datatype {field} is not a recognized snowflake datatype"
                )

            pandas_dtypes[field] = converted
        except KeyError as e:
            raise Exception(
                f"Invalid datatype: the datatype {field} is not a recognized snowflake datatype"
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
