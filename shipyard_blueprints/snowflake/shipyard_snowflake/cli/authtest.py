import argparse
# from shipyard_blueprints import SnowflakeClient
from ..snowflake import SnowflakeClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", required=True, dest='user')
    parser.add_argument('--pwd', required=True, dest='pwd')
    parser.add_argument('--account', required=False,
                        default=None, dest='account')
    parser.add_argument('--database', required=False,
                        default=None, dest='database')
    parser.add_argument('--schema', required=False,
                        default=None, dest='schema')
    parser.add_argument('--warehouse', required=False,
                        default=None, dest='warehouse')

    args = parser.parse_args()
    return args


def main():
    args = get_args()
    user = args.user
    pwd = args.pwd
    account = args.account
    database = args.database
    schema = args.schema
    warehouse = args.warehouse

    snowflake = SnowflakeClient(username=user, pwd=pwd, database=database,
                                account=account, warehouse=warehouse, schema=schema)
    conn = snowflake.connect()
    if conn == 1:
        return 1
    else:
        return 0


if __name__ == '__main__':
    main()
