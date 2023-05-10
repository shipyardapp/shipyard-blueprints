import os
from shipyard_blueprints import SqlServerClient


def get_args():
    args = {}
    args['user'] = os.getenv('MSSQL_USERNAME')
    args['password'] = os.getenv('MSSQL_PASSWORD')
    args['host'] = os.getenv('MSSQL_HOST')
    args['database'] = os.getenv('MSSQL_DATABASE')
    args['port'] = os.getenv('MSSQL_PORT')
    return args


def main():
    args = get_args()
    host = args['host']
    user = args['user']
    pwd = args['password']
    port = args['port']
    database = args['database']
    sqlserver = SqlServerClient(
        user=user, pwd=pwd, host=host, port=port, database=database)
    try:
        con = sqlserver.connect()
        return 0
    except Exception as e:
        sqlserver.logger.error(
            "Could not connect to postgres with given credentials")
        return 1


if __name__ == "__main__":
    main()
