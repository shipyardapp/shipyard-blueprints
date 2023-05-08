import os
from shipyard_blueprints import CensusClient


def get_args():
    args = {}
    args['access_token'] = os.getenv('CENSUS_API_KEY')
    return args


def main():
    args = get_args()
    access_token = args['access_token']
    client = CensusClient(access_token)
    # test the connection
    conn = client.connect()
    if conn == 200:
        return 0
    else: 
        return 1



if __name__ == "__main__":
    main()
