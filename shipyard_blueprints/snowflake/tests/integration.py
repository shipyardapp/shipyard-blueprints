import os
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from shipyard_snowflake import SnowflakeClient
from pytest import fixture
from shipyard_snowflake import utils

load_dotenv(find_dotenv())


@fixture
def snowflake():
    user = os.getenv("SNOWFLAKE_USERNAME")
    pwd = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    db = os.getenv("SNOWFLAKE_DATABASE")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

    return SnowflakeClient(
        username=user,
        pwd=pwd,
        account=account,
        schema=schema,
        database=db,
        warehouse=warehouse,
    )


def test_put():
    # 1. Load a file using a put
    # 2. Download the loaded table
    # 3. Check that the file is exactly the same as the source
    # conn = snowflake.connect()
    user = os.getenv("SNOWFLAKE_USERNAME")
    pwd = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    db = os.getenv("SNOWFLAKE_DATABASE")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

    client = SnowflakeClient(
        username=user,
        pwd=pwd,
        account=account,
        schema=schema,
        database=db,
        warehouse=warehouse,
    )

    conn = client.connect()

    table_name = "test_put"
    file_path = "/Users/wespoulsen/Repos/Blueprints/shipyard-blueprints/shipyard_blueprints/snowflake/snowflake.csv"

    # get the datatypes
    dts = utils.infer_schema(file_path)
    snowflake_dts = utils.map_pandas_to_snowflake(dts)

    # create the table
    create_table_sql = client._create_table(
        table_name=table_name, columns=snowflake_dts
    )

    client.execute_query(conn=conn, query=create_table_sql)

    client.put(conn=conn, file_path=file_path, table_name=table_name)

    client.copy_into(conn=conn, table_name=table_name)

    print("Completed put")


if __name__ == "__main__":
    test_put()
