import os
from shipyard_blueprints import CoalesceClient

def get_args():
    args = {}
    args['token'] = os.getenv('COALESCE_ACCESS_TOKEN')
    return args

def main():
    args = get_args()
    token = args['token']
    client = CoalesceClient(token)
    response = client.connect()
    if response == 200:
        return 0
    else:
        return 1

if __name__ == '__main__':
    main()