from shipyard_blueprints import ModeClient
import argparse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token-id", dest='token_id', required=True)
    parser.add_argument('--token-secret', dest='token_secret', required=True)
    parser.add_argument('--account', dest='account', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    token_id = args.token_id
    token_secret = args.token_secret
    account = args.account
    mode = ModeClient(token_id, token_secret, account)
    try:
        response = mode.connect()
        if response.status_code == 200:
            mode.logger.info("Successfully connected to Mode API")
            return 0
        else:
            mode.logger.error(
                "Could not connect to Mode API with the token id and token secret provided")
            return 1
    except Exception as e:
        mode.logger.error(
            "Could not connect to Mode API with the token id and token secret provided")
        return 1


if __name__ == "__main__":
    main()
