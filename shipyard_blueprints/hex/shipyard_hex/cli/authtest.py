import os
from shipyard_blueprints import HexClient

def get_args():
    args = {}
    args['api_token'] = os.environ.get('HEX_API_TOKEN')
    args['project_id'] = os.environ.get('HEX_PROJECT_ID')
    return args

def main():
    args = get_args()
    hex_client = HexClient(api_token=args['api_token'], project_id=args['project_id'])
    conn = hex_client.connect()
    if conn == 200:
        return 0
    else: 
        return 1

if __name__ == '__main__':
    main()