import os
from shipyard_blueprints import DatabricksClient

def get_args():
    args = {}
    args['access_token'] = os.getenv('DATABRICKS_ACCESS_TOKEN')
    args['instance_url'] = os.getenv('DATABRICKS_INSTANCE_URL')
    return args 


def main():
    args = get_args()
    token = args['access_token']
    url = args['instance_url']
    databricks_client = DatabricksClient(url, token)
    try:
        databricks_client.connect()
        databricks_client.logger.info("Successfully connected to Databricks")
        return 0
    except Exception as e:
        databricks_client.logger.error("Could not connect to Databricks with the given access token and instance url")
        return 1


if __name__ == "__main__":
    main()
