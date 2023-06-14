import os
from shipyard_jira import JiraClient


def main():
    return JiraClient(access_token=os.getenv('JIRA_ACCESS_TOKEN'),
                      domain=os.getenv('JIRA_DOMAIN'),
                      email_address=os.getenv('JIRA_EMAIL')
                      ).connect()


if __name__ == '__main__':
    main()
