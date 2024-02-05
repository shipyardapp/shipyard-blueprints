from shipyard_snowflake import SnowflakeClient
import unittest
import os
import pandas as pd


user = os.getenv("SNOWFLAKE_USER")
pwd = os.getenv("SNOWFLAKE_PWD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
schema = os.getenv("SNOWFLAKE_SCHEMA")
database = os.getenv("SNOWFLAKE_DATABASE")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
rsa_key = "/Users/wespoulsen/.ssh/snowflake_key.p8"
role = os.getenv("SNOWFLAKE_ROLE")

client = SnowflakeClient(
    username=user,
    password=pwd,
    database=database,
    account=account,
    warehouse=warehouse,
    schema=schema,
    rsa_key=rsa_key,
)


def upload_test():
    df_path = "shipyard_snowflake/test/simple.csv"
    conn = client.connect()
    df = client.read_file(df_path)
    client.upload(conn, df=df, table_name="NEW_DATATYPES", insert_method="replace")
    print("Successfully uploaded file")


def upload_dtypes_test():
    df_path = "shipyard_snowflake/test/simple.csv"
    conn = client.connect()
    snowflake_dtypes = [
        ["string_col", "VARCHAR"],
        ["char_col", "CHAR"],
        ["int_col", "INT"],
        ["float_col", "FLOAT"],
        ["date_col", "DATE"],
        ["datetime_col", "TIMESTAMP"],
        ["bool_col", "BOOLEAN"],
    ]

    df = read_file(df_path, snowflake_dtypes=snowflake_dtypes)
    client.upload(conn, df=df, table_name="NEW_DATATYPES", insert_method="replace")


def fetch_test():
    conn = client.connect()
    df = client.fetch(conn, "SELECT * FROM NEW_DATATYPES")
    print(df.head())


def rsa_test():
    rsa_client = SnowflakeClient(
        username="BPUSER",
        password=None,
        rsa_key=rsa_key,
        database=database,
        account=account,
        warehouse=warehouse,
        schema=schema,
    )
    conn = rsa_client.connect()
    df = rsa_client.fetch(conn, "SELECT * FROM NEW_DATATYPES")
    print(df.head())
