import os
from shipyard_tableau import TableauClient


def get_args():
    args = {}
    args['username'] = os.getenv['TABLEAU_USERNAME']
    args['password'] = os.getenv['TABLEAU_PASSWORD']
    args['server_url'] = os.getenv['TABLEAU_SERVER_URL']
    args['project_id'] = os.getenv['TABLEAU_PROJECT_NAME']
    args['site'] = os.getenv['TABLEAU_SITE_ID']

    return args


def main():
    args = get_args()
    username = args['username']
    password = args['password']
    site = args['site']
    server_url = args['server_url']
    project = args['project_id']
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
