import os
from shipyard_blueprints import HightouchClient

def get_args():
    args = {}
    args['access_token'] = os.environ.get('HIGHTOUCH_API_KEY')
    return args

def main():
    args = get_args()
    client = HightouchClient(access_token=args['access_token'])
    conn = client.connect()
    if conn == 200:
        return 0 
    else:
        return 1

if __name__ == '__main__':
    main()