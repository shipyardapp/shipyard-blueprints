import os
import sys

from shipyard_tableau.tableau import TableauClient


def main():
    sys.exit(
        TableauClient(username=os.getenv("TABLEAU_USERNAME"),
                      password=os.getenv("TABLEAU_PASSWORD"),
                      server_url=os.getenv("TABLEAU_SERVER_URL"),
                      site=os.getenv("TABLEAU_SITE_ID"),
                      ).connect(sign_in_method=os.getenv("TABLEAU_SIGN_IN_METHOD"))
    )


if __name__ == "__main__":
    main()
