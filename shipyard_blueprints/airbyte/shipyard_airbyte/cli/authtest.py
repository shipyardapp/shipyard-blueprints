import os
from shipyard_blueprints import AirbyteClient

def get_args():
    args = {}
    args['access_token'] = os.getenv('AIRBYTE_API_TOKEN')
    return args


def main():
    args = get_args()
    token = args['access_token']
    client = AirbyteClient(token)
    response = client.connect()
    if response == 200:
        return 0 
    else:
        return 1

if __name__ == '__main__':
    main()