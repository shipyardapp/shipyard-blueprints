import os
import pytest
import pandas as pd
import json
from shipyard_notion import NotionClient, create_row_payload
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())
token = str(os.getenv("NOTION_ACCESS_TOKEN"))
db = os.getenv("NOTION_DB")
client = NotionClient(token)
try:
    df = pd.read_csv("test_sample.csv")
except:
    df = pd.read_csv("tests/test_sample.csv")

def test_upload():
    data = client.client.databases.query(database_id=db)
    client.upload(database_id=db, data=df, insert_method="append")


def test_upload_replace():
    data = client.client.databases.query(database_id=db)
    client.upload(database_id=db, data=df, insert_method="replace")


def test_fetch():
    data = client.client.databases.query(database_id=db)
    with open("notion_data.json", "w") as f:
        json.dump(data, f)
    return


if __name__ == "__main__":
    # test_create()
    # test_upload()
    test_fetch()
    # test_upload_replace()
