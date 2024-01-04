import os
import sys
from shipyard_tableau.tableau import TableauClient


def main():
    sys.exit(
        TableauClient(
            os.getenv["TABLEAU_USERNAME"],
            os.getenv["TABLEAU_PASSWORD"],
            os.getenv["TABLEAU_SERVER_URL"],
            os.getenv["TABLEAU_SITE_ID"],
        ).connect(os.getenv("TABLEAU_SIGN_IN_METHOD"))
    )


if __name__ == "__main__":
    main()
