import os
import pandas as pd
from shipyard_snowflake import SnowparkClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

df_path = "shipyard_snowflake/test/simple.csv"

user = os.getenv("SNOWFLAKE_USER")
pwd = os.getenv("SNOWFLAKE_PWD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
schema = os.getenv("SNOWFLAKE_SCHEMA")
database = os.getenv("SNOWFLAKE_DATABASE")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
# rsa_key = "/Users/wespoulsen/.ssh/snowflake_key.p8"
role = os.getenv("SNOWFLAKE_ROLE")

client = SnowparkClient(
    username=user,
    pwd=pwd,
    database=database,
    account=account,
    warehouse=warehouse,
    schema=schema,
)


def test_upload():
    session = client.connect()
    df = pd.read_csv(df_path)
    client.upload(session=session, df=df, table_name="SNOWPARK_TEST", overwrite=True)
    print("Done")


def test_larger_upload():
    session = client.connect()
    larger = "shipyard_snowflake/test/larger.csv"
    df = pd.read_csv(larger)
    client.upload(session=session, df=df, table_name="LARGER_TEST", overwrite=True)
    print("Done")


def test_larger_upload_append():
    session = client.connect()
    larger = "shipyard_snowflake/test/larger.csv"
    df = pd.read_csv(larger)
    client.upload(session=session, df=df, table_name="LARGER_TEST", overwrite=False)
    print("Done")


if __name__ == "__main__":
    test_larger_upload_append()
