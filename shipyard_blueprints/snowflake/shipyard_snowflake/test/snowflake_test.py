import os
import pandas as pd
import pytest
from shipyard_snowflake import SnowflakeClient
from dotenv import load_dotenv, find_dotenv
from shipyard_snowflake.utils.utils import (
    map_pandas_to_snowflake,
    infer_schema,
    reservoir_sample,
    read_file,
)

if env_exists := os.path.exists(".env"):
    load_dotenv()

    df_path = "shipyard_snowflake/test/simple.csv"

    user = os.getenv("SNOWFLAKE_USER")
    pwd = os.getenv("SNOWFLAKE_PWD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    database = os.getenv("SNOWFLAKE_DATABASE")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    # rsa_key = "/Users/wespoulsen/.ssh/snowflake_key.p8"
    role = os.getenv("SNOWFLAKE_ROLE")

    client = SnowflakeClient(
        username=user,
        password=pwd,
        database=database,
        account=account,
        warehouse=warehouse,
        schema=schema,
        role=role,
    )
    snowflake_dtypes = [
        ["string_col", "VARCHAR"],
        ["char_col", "CHAR"],
        ["int_col", "INT"],
        ["float_col", "FLOAT"],
        ["date_col", "DATE"],
        ["datetime_col", "TIMESTAMP"],
        ["bool_col", "BOOLEAN"],
    ]


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_put():
    conn = client.connect()
    create_sql = client._create_table_sql(
        table_name="PUT_TEST", columns=snowflake_dtypes
    )
    client.execute_query(conn=conn, query=create_sql)
    client.put(conn, file_path=df_path, table_name="PUT_TEST")
    client.copy_into(conn, table_name="PUT_TEST")


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_put_no_datatypes():
    conn = client.connect()
    sample = infer_schema(df_path)
    snowflake_dtypes = map_pandas_to_snowflake(sample)
    create_sql = client._create_table_sql(
        table_name="LARGER_TEST", columns=snowflake_dtypes
    )
    client.execute_query(conn, create_sql)
    client.put(
        conn, file_path="shipyard_snowflake/test/larger.csv", table_name="LARGER_TEST"
    )
    client.copy_into(conn, table_name="LARGER_TEST")
