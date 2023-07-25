import os
from shipyard_tableau import TableauClient


def main():
    return TableauClient(os.getenv["TABLEAU_USERNAME"],
                         os.getenv["TABLEAU_PASSWORD"],
                         os.getenv["TABLEAU_SERVER_URL"],
                         os.getenv["TABLEAU_SITE_ID"]
                         ).connect(os.getenv('TABLEAU_SIGN_IN_METHOD'))


if __name__ == "__main__":
    main()
