import os
import pytest
import pandas as pd
import json
from shipyard_notion import NotionClient, create_row_payload
from dotenv import load_dotenv, find_dotenv

if env_exists := os.path.exists(".env"):
    load_dotenv()


@pytest.fixture
def token():
    return str(os.getenv("NOTION_ACCESS_TOKEN"))


@pytest.fixture
def db():
    return os.getenv("NOTION_DB")


@pytest.fixture
def client():
    return NotionClient(token)


@pytest.mark.skipif(not env_exists, reason="no .env file")
def test_upload():
    data = client.client.databases.query(database_id=db)
    client.upload(database_id=db, data=df, insert_method="append")


@pytest.mark.skipif(not env_exists, reason="no .env file")
def test_upload_replace():
    data = client.client.databases.query(database_id=db)
    client.upload(database_id=db, data=df, insert_method="replace")


@pytest.mark.skipif(not env_exists, reason="no .env file")
def test_fetch():
    data = client.client.databases.query(database_id=db)
    with open("notion_data.json", "w") as f:
        json.dump(data, f)
    return
