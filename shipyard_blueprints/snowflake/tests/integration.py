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

    file_path = "/Users/wespoulsen/Repos/Blueprints/shipyard-blueprints/shipyard_blueprints/snowflake/snowflake.csv"

    df = pd.read_csv(file_path, parse_dates=True)
    # df2 = utils._parse_dates(df)

    datatypes = utils.infer_schema(file_path, 100)

    snowflake_datatypes = utils.map_pandas_to_snowflake(datatypes)

    print(snowflake_datatypes)


if __name__ == "__main__":
    test_put()
