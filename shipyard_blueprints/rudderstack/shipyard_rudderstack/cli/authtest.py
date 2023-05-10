import os
from shipyard_blueprints import RudderStackClient

def get_args():
    args = {}
    args['access_token'] = os.environ.get('RUDDERSTACK_ACCESS_TOKEN')
    return args

def main():
    args = get_args()
    access_token = args['access_token']
    client = RudderStackClient(access_token=access_token)
    conn = client.connect()
    if conn == 200:
        return 0
    else:
        return 1

if __name__ == '__main__':
    main()
