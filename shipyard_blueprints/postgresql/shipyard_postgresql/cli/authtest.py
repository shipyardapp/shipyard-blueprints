import argparse
from shipyard_blueprints import PostgresqlClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", dest='host', required=True)
    parser.add_argument('--user', dest='user', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--port', required=True, dest='port', default='5432')
    parser.add_argument('--database', dest='database', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    host = args.host
    user = args.user
    pwd = args.password
    port = args.port
    database = args.database
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
