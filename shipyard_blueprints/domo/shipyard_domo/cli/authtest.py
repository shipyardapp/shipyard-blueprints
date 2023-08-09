import os
import sys
from shipyard_blueprints import DomoClient


def main():
    sys.exit(
        DomoClient(
            client_id=os.getenv("DOMO_CLIENT_ID"),
            secret_key=os.getenv("DOMO_SECRET_KEY"),
            access_token=os.getenv("DOMO_ACCESS_TOKEN"),
            domo_instance=os.getenv("DOMO_INSTANCE_ID"),
        ).connect()
    )


if __name__ == "__main__":
    main()
