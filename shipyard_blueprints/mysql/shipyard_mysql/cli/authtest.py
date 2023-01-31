import argparse
from shipyard_blueprints import MySqlClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", dest='host', required=True)
    parser.add_argument('--user', dest='user', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--port', dest='port', required=False, default=3306)
    parser.add_argument('--database', dest='database', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    host = args.host
    user = args.user
    password = args.password
    port = args.port
    database = args.database
    mysql = MySqlClient(user=user, pwd=password, host=host,
                        port=port, database=database)
    con = mysql.connect()
    if con == 1:
        return 1
    else:
        return 0


if __name__ == '__main__':
    main()
