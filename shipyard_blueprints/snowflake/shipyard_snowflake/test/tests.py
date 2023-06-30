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

client = SnowflakeClient(username = user, pwd = pwd, database = database, account = account, warehouse = warehouse, schema = schema)

def upload_test():
    df_path = 'shipyard_snowflake/test/simple.csv'
    conn = client.connect()
    df = client.read_file(df_path)
    client.upload(conn, df = df, table_name = 'NEW_DATATYPES',if_exists= 'replace' )
    print('Successfully uploaded file')

def upload_dtypes_test():
    df_path = 'shipyard_snowflake/test/simple.csv'
    conn = client.connect()
    snowflake_dtypes = [['string_col','VARCHAR'],['char_col','CHAR'], ['int_col','INT'],['float_col','FLOAT'],['date_col','DATE'],['datetime_col','TIMESTAMP'],['bool_col','BOOLEAN']]

    df = client.read_file(df_path, snowflake_dtypes = snowflake_dtypes)
    client.upload(conn, df = df, table_name = 'NEW_DATATYPES',if_exists= 'replace')

def fetch_test():
    conn = client.connect()
    df = client.fetch(conn, 'SELECT * FROM NEW_DATATYPES')
    print(df.head())



if __name__ == "__main__":
    # upload_test()
    # upload_dtypes_test()
    fetch_test()
