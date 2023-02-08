import argparse
from shipyard_blueprints import AzureBlobClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection-string",
                        dest="connection_string", required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    conn_str = args.connection_string

    azure = AzureBlobClient(connection_string=conn_str)
    try:
        conn = azure.connect()
        return 0
    except Exception as e:
        azure.logger.error(
            "Could not connect to the Azure with the provided connection string")
        return 1


if __name__ == "__main__":
    main()
