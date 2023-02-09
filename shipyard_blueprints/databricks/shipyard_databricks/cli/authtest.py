from shipyard_blueprints import DatabricksClient
import argparse

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest = "access_token", required = True)
    parser.add_argument("--instance-url", dest = "instance_url", required = True)
    args = parser.parse_args()
    return args 


def main():
    args = get_args()
    token = args.access_token
    url = args.instance_url
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
