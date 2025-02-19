# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "shipyard-databricks",
#     "requests",
#     "shipyard-templates",
# ]
# ///
import os
import sys

import requests
from shipyard_templates import ShipyardLogger

from shipyard_databricks import DatabricksClient

logger = ShipyardLogger.get_logger()


def main():
    try:
        client = DatabricksClient(
            access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"),
            instance_url=os.getenv("DATABRICKS_INSTANCE_URL"),
        )
        response = requests.get(
            f"{client.base_url}/clusters/list", headers=client.headers
        )
        if response.status_code == 200:
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception as e:
        logger.error(f"Could not connect to Databricks. Message from Databricks: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
