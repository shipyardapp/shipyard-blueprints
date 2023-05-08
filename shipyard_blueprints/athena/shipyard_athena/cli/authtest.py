import argparse
import os
from shipyard_blueprints import AthenaClient


def get_args():
    args = {}
    args['aws_access_key'] = os.getenv('AWS_ACCESS_KEY_ID')
    args['aws_secret_key'] = os.getenv('AWS_SECRET_ACCESS_KEY')
    args['region'] = os.getenv('AWS_DEFAULT_REGION')
    return args

def main():
    args = get_args()
    access_key = args['aws_access_key']
    secret_key = args['aws_secret_key']
    region = args['region']
    athena = AthenaClient(access_key, secret_key, region)

    con = athena.connect()
    if con == 1:
        return 1
    else:
        return 0


if __name__ == '__main__':
    main()
