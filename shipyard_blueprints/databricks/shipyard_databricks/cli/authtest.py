import os
import sys
import requests
from shipyard_databricks import DatabricksClient


def main():
    client = DatabricksClient(
        access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"),
        instance_url=os.getenv("DATABRICKS_INSTANCE_URL"),
    )
    try:
        response = requests.get(
            f"{client.base_url}/clusters/list", headers=client.headers
        )
        if response.status_code == 200:
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception as e:
        client.logger.error("Could not connect to Databricks")
        sys.exit(1)


if __name__ == "__main__":
    main()
