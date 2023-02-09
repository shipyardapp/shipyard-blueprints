import argparse
from shipyard_blueprints import LookerClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", dest='base_url', required=True)
    parser.add_argument('--client-id', dest='client_id', required=True)
    parser.add_argument('--client-secret', dest='client_secret', required=True)

    args = parser.parse_args()
    return args


def main():
    args = get_args()
    base_url = args.base_url
    client_id = client_id
    client_secret = client_secret
    looker = LookerClient(base_url, client_id, client_secret)
    try:
        looker.connect()
        looker.logger.info("Successfully connected to Looker")
        return 0
    except Exception as e:
        looker.logger.error(
            "Could not connect to Looker with the client ID and client secret provided")
        return 1


if __name__ == "__main__":
    main()
