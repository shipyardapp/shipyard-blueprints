import os
from shipyard_blueprints import ModeClient



def get_args():
    args = {}
    args['token_id'] = os.environ.get('MODE_TOKEN_ID')
    args['token_secret'] = os.environ.get('MODE_TOKEN_PASSWORD')
    args['account'] = os.environ.get('MODE_WORKSPACE_NAME') 
    return args


def main():
    args = get_args()
    token_id = args['token_id']
    token_secret = args['token_secret']
    account = args['account']
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
