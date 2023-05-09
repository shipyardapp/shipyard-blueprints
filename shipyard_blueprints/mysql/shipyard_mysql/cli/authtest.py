import os
from shipyard_blueprints import MySqlClient


def get_args():
    args = {}
    args['host'] = os.environ.get('MYSQL_HOST')
    args['port'] = os.environ.get('MYSQL_PORT')
    args['username'] = os.environ.get('MYSQL_USERNAME')
    args['password'] = os.environ.get('MYSQL_PASSWORD')
    args['database'] = os.environ.get('MYSQL_DATABASE')
    return args


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
        return 1
    else:
        return 0


if __name__ == '__main__':
    main()
