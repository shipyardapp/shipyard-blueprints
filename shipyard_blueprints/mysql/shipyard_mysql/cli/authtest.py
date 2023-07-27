import os
import sys
from shipyard_blueprints import MySqlClient


def get_args():
    return {
        'host': os.environ.get('MYSQL_HOST'),
        'port': os.environ.get('MYSQL_PORT'),
        'username': os.environ.get('MYSQL_USERNAME'),
        'password': os.environ.get('MYSQL_PASSWORD'),
        'database': os.environ.get('MYSQL_DATABASE'),
    }


def main():
    args = get_args()
    host = args['host']
    user = args['username']
    password = args['password']
    port = args['port']
    database = args['database']
    mysql = MySqlClient(user=user, pwd=password, host=host,
                        port=port, database=database)
    con = mysql.connect()
    if con == 1:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
