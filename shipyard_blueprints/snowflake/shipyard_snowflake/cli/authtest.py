import argparse
import os
from shipyard_blueprints import SnowflakeClient


def get_args():
    args = {}
    args['user'] = os.getenv('SNOWFLAKE_USERNAME')
    args['password'] = os.getenv('SNOWFLAKE_PASSWORD')
    args['account'] = os.getenv('SNOWFLAKE_ACCOUNT')
    return args


def main():
    args = get_args()
    user = args['user']
    pwd = args['password']
    account = args['account'] 

    snowflake = SnowflakeClient(username=user, pwd=pwd, account=account)
    conn = snowflake.connect()
    if conn == 1:
        return 1
    else:
        return 0


if __name__ == '__main__':
    main()
