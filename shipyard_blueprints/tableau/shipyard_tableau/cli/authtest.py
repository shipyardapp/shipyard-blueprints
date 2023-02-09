import argparse
from shipyard_tableau import TableauClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", dest='username', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--site-id', dest='site_id', required=True, default='')
    parser.add_argument("--server-url", dest='server_url', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    username = args.username
    password = args.password
    site = args.site_id
    server_url = args.server_url
    tableau = TableauClient(username, password, server_url, site)
    try:
        conn = tableau.connect()
        tableau.logger.info("Successfully connected to Tableau")
        return 0
    except Exception as e:
        tableau.logger.error(
            "Could not connect to Tableau with the username and password provided")
        return 1


if __name__ == "__main__":
    main()
