import os 
from shipyard_blueprints import FivetranClient


def get_args():
    args = {}
    args['api_key'] = os.getenv('FIVETRAN_API_KEY')
    args['api_secret'] = os.getenv('FIVETRAN_API_SECRET')
    return args


def main():
    args = get_args()
    client = FivetranClient(args['api_key'], args['api_secret'])
    resp = client.connect()
    if resp == 200:
        return 0
    else:
        return 1


if __name__ == '__main__':
    main()