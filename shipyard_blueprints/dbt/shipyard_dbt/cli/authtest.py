import os
import sys
from shipyard_dbt import DbtClient


def main():
    client = DbtClient(
        access_token=os.getenv("DBT_API_KEY"), account_id=os.getenv("DBT_ACCOUNT_ID")
    )

    sys.exit(client.connect())


if __name__ == "__main__":
    main()
