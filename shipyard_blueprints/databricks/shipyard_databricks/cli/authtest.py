import os
import requests
from shipyard_blueprints import DatabricksClient

def main():
    client = DatabricksClient(access_token = os.getenv('DATABRICKS_ACCESS_TOKEN'),instance_url= os.getenv('DATABRICKS_INSTANCE_URL'))
    try:
        response = requests.get(f"{client.base_url}/clusters/list",headers = client.headers)
        if response.status_code == 200:
            return 0
        else:
            return 1
    except Exception as e:
        client.logger.error('Could not connect to Databricks')
        return 1


if __name__ == "__main__":
    main()
