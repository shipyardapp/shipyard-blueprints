import os
import sys
from shipyard_fivetran import FivetranClient


def main():
    sys.exit(
        FivetranClient(
            access_token=os.getenv("FIVETRAN_API_KEY"),
            api_secret=os.getenv("FIVETRAN_API_SECRET"),
        ).connect()
    )


if __name__ == "__main__":
    main()
