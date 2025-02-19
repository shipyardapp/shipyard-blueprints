# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "shipyard-jira",
# ]
# ///
import os
import sys
from shipyard_jira import JiraClient


def main():
    sys.exit(
        JiraClient(
            access_token=os.getenv("JIRA_ACCESS_TOKEN"),
            domain=os.getenv("JIRA_DOMAIN"),
            email_address=os.getenv("JIRA_EMAIL"),
        ).connect()
    )


if __name__ == "__main__":
    main()
