import os
import requests
from shipyard_databricks import DatabricksClient
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())


def conn_helper(client: DatabricksClient) -> int:
    try:
        response = requests.get(
            f"https://{client.instance_url}/api/2.0/clusters/list",
            headers=client.headers,
        )
        if response.status_code == 200:
            return 0
        else:
            return 1
    except Exception as e:
        client.logger.error("Could not connect to Databricks")
        client.logger.exception(e)
        return 1


def test_good_connection():
    client = DatabricksClient(
        access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"),
        instance_url=os.getenv("DATABRICKS_INSTANCE_URL"),
    )

    assert conn_helper(client) == 0


def test_bad_token():
    client = DatabricksClient(
        access_token="bad_token", instance_url=os.getenv("DATABRICKS_INSTANCE_URL")
    )

    assert conn_helper(client) == 1


def test_bad_url():
    client = DatabricksClient(
        access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"), instance_url="bad_url"
    )
    assert conn_helper(client) == 1
