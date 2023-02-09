from shipyard_blueprints import DomoClient
import argparse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--client-id", dest='client_id', required=True)
    parser.add_argument('--secret-key', dest='secret_key', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    client_id = args.client_id
    secret_key = args.secret_key
    try:
        domo = DomoClient(client_id, secret_key)
        domo.logger.info("Successfully connected to Domo")
        return 0
    except Exception as e:
        domo.logger.error(
            "Could not connect to Domo with the Client ID and Secret ID provided")
        return 1


if __name__ == "__main__":
    main()
