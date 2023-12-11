import pytest
import os
from dotenv import load_dotenv, find_dotenv
from shipyard_databricks_sql import DatabricksSqlClient

load_dotenv(find_dotenv())


@pytest.fixture
def databricks_client():
    return DatabricksSqlClient(
        server_host=os.getenv("DATABRICKS_SERVER_HOST"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"),
        catalog=os.getenv("CATALOG"),
        schema=os.getenv("SCHEMA"),
    )


def test_download(databricks_client):
    query = os.getenv("TEST_QUERY")
    df = databricks_client.fetch(query=query)
    assert df.shape[0] == 10
