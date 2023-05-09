import os
from shipyard_blueprints import PostgresqlClient


def get_args():
    args = {}
    args['host'] = os.environ.get('POSTGRES_HOST')
    args['username'] = os.environ.get('POSTGRES_USERNAME')
    args['password'] = os.environ.get('POSTGRES_PASSWORD')
    args['port'] = os.environ.get('POSTGRES_PORT')
    args['database'] = os.environ.get('POSTGRES_DATABASE')
    return args


def main():
    args = get_args()
    host = args['host']
    user = args['username']
    pwd = args['password']
    port = args['port']
    database = args['database']
    postgres = PostgresqlClient(
        user=user, pwd=pwd, host=host, port=port, database=database)
    try:
        con = postgres.connect()
        postgres.logger.info("Successfully established connection")
        return 0
    except Exception as e:
        postgres.logger.error(
            "Could not connect to postgres with given credentials")
        return 1


if __name__ == "__main__":
    main()
