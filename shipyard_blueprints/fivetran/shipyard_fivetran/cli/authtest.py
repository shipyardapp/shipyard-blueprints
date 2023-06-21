import os
from shipyard_fivetran import FivetranClient


def main():
    return FivetranClient(
        access_token=os.getenv("FIVETRAN_API_KEY"),
        api_secret=os.getenv("FIVETRAN_API_SECRET"),
    ).connect()


if __name__ == "__main__":
    main()
