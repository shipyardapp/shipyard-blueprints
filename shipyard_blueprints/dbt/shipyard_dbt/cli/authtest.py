import os
from shipyard_blueprints import DbtClient


def main():
    client = DbtClient(access_token = os.getenv('DBT_API_KEY'),
                       account_id = os.getenv('DBT_ACCOUNT_ID'))

    return client.connect()

if __name__ == '__main__':
    main()
