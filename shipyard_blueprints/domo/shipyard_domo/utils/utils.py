import pandas as pd
import os
from typing import List, Dict, Any, Optional, Union
from random import random, randrange
from itertools import islice
from io import StringIO
from math import exp, log, floor, ceil
from pydomo.datasets import Schema, Column, ColumnType

from shipyard_domo.utils.exceptions import ColumnMismatch, InvalidDatatype


def map_domo_to_pandas(domo_schema: List[Dict[Any, Any]]) -> Dict[str, str]:
    """Maps the domo datatypes to the associated pandas datatype

    Args:
        domo_schema (list(dict)): Takes in a list of the columns and their associated domo data types
    """
    pandas_dtypes = {}

    for schema in domo_schema:
        dtype = schema["type"]
        col = schema["name"]
        if dtype == "BOOLEAN":
            pandas_dtypes[col] = "bool"
        elif dtype == "DATETIME":
            pandas_dtypes[col] = "datetime64[ns]"
        elif dtype == "DECIMAL":
            pandas_dtypes[col] = "float64"
        elif dtype == "LONG":
            pandas_dtypes[col] = "int64"
        elif dtype == "STRING":
            pandas_dtypes[col] = "object"
        else:
            pandas_dtypes[col] = "object"
    return pandas_dtypes


def reservoir_sample(iterable, k=1):
    """
    Select k items uniformly from iterable.
    Returns the whole population if there are k or fewer items.

    Args:
        iterable: An iterable object from which to sample.
        k (optional): The number of items to sample. Defaults to 1.

    Returns:
        A list of k items sampled from the iterable.

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


#
# def make_schema(data_types: list, file_name: str, folder_name: str):
#     """Constructs a domo schema which is required for the stream upload
#
#     Args:
#         data_types (list): The column name as well as the Domo data types in the form of [['Column1', 'STRING'],['Column2','DECIMAL']]
#         file_name (str): The path for the file to read
#         folder_name (str): _description_
#
#     Returns:
#         Schema: Schema object of the dataset
#     """
#     if isinstance(file_name, list):
#         schemas = []
#         for file in file_name:
#             file_path = file
#             if folder_name is not None:
#                 file_path = os.path.normpath(
#                     os.path.join(os.getcwd(), folder_name, file)
#                 )
#             df = pd.read_csv(file_path, nrows=1)
#             cols = list(df.columns)
#             if len(cols) != len(data_types):
#                 raise ColumnMismatch
#             domo_schema = []
#             for pair in data_types:
#                 col = pair[0]
#                 dtype = pair[1]
#                 dt_upper = str(dtype).upper()
#                 if dt_upper not in [
#                     "STRING",
#                     "DECIMAL",
#                     "LONG",
#                     "DOUBLE",
#                     "DATE",
#                     "DATETIME",
#                 ]:
#                     raise InvalidDatatype(dt_upper)
#                 domo_schema.append(Column(dt_upper, col))
#                 schema = Schema(domo_schema)
#                 schemas.append(schema)
#         assert all(schemas[0] == s for s in schemas)
#         return schemas[0]
#     else:
#         file_path = file_name
#         if folder_name is not None:
#             file_path = os.path.normpath(
#                 os.path.join(os.getcwd(), folder_name, file_name)
#             )
#         df = pd.read_csv(file_path, nrows=1)
#         cols = list(df.columns)
#         if len(cols) != len(data_types):
#             raise ColumnMismatch
#
#         domo_schema = []
#         for pair in data_types:
#             col = pair[0]
#             dtype = pair[1]
#             dt_upper = str(dtype).upper()
#             if dt_upper not in [
#                 "STRING",
#                 "DECIMAL",
#                 "LONG",
#                 "DOUBLE",
#                 "DATE",
#                 "DATETIME",
#             ]:
#                 raise InvalidDatatype(dt_upper)
#             domo_schema.append(Column(dt_upper, col))
#
#         return Schema(domo_schema)


def count_lines(filename):
    with open(filename, "r") as file:
        return sum(1 for line in file)


def normalize_file_path(file_name: str, folder_name: str) -> str:
    """
    Normalize the file path by joining folder_name and file_name.

    Args:
        file_name (str): The path for the file to read.
        folder_name (str): The folder containing the file.

    Returns:
        str: Normalized file path.
    """
    if folder_name:
        return os.path.normpath(os.path.join(os.getcwd(), folder_name, file_name))
    return file_name


def read_csv_and_check_columns(
    file_path: str, data_types: List[List[Union[str, str]]]
) -> List[Column]:
    """
    Read the CSV file and check if column count matches data types.

    Args:
        file_path (str): Path to the CSV file.
        data_types (list): List of column names and Domo data types.

    Returns:
        List[Column]: List of Column objects representing the schema.
    """
    df = pd.read_csv(file_path, nrows=1)
    cols = list(df.columns)
    if len(cols) != len(data_types):
        raise ColumnMismatch
    return [Column(str(dtype).upper(), col) for col, dtype in data_types]


def make_schema(
    data_types: List[List[Union[str, str]]],
    file_name: Union[str, List[str]],
    folder_name: str = None,
) -> Schema:
    """
    Constructs a Domo schema required for stream upload.

    Args:
        data_types (list): Column names and Domo data types (e.g., [['Column1', 'STRING'], ['Column2', 'DECIMAL']]).
        file_name (Union[str, List[str]]): Path(s) for the file(s) to read.
        folder_name (str, optional): Folder containing the file(s).

    Returns:
        Schema: Schema object of the dataset.
    """
    if isinstance(file_name, list):
        schemas = Schema(
            [
                read_csv_and_check_columns(
                    normalize_file_path(file, folder_name), data_types
                )
                for file in file_name
            ]
        )
        assert all(schemas[0] == s for s in schemas)
        return schemas[0]
    else:
        file_path = normalize_file_path(file_name, folder_name)
        return Schema(read_csv_and_check_columns(file_path, data_types))
