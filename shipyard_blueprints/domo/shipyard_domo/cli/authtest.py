import os
from shipyard_blueprints import DomoClient


def get_args():
    args = {}
    args['client_id'] = os.getenv('DOMO_CLIENT_ID')
    args['secret_key'] = os.getenv('DOMO_SECRET_KEY')
    return args


def main():
    args = get_args()
    client_id = args['client_id']
    secret_key = args['secret_key']
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
