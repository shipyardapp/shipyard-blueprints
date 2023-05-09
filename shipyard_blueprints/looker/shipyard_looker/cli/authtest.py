import argparse
import os
from shipyard_blueprints import LookerClient


def get_args():
    args = {}
    args['client_id'] = os.environ.get('LOOKER_CLIENT_ID')
    args['client_secret'] = os.environ.get('LOOKER_CLIENT_SECRET')
    args['looker_url'] = os.environ.get('LOOKER_URL')
    return args


def main():
    args = get_args()
    client_id = args['client_id']
    client_secret = args['client_secret']
    base_url = args['looker_url']
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
