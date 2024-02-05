import os
import sys
from shipyard_mode import ModeClient


def main():
    sys.exit(
        ModeClient(
            os.environ.get("MODE_TOKEN_ID"),
            os.environ.get("MODE_TOKEN_PASSWORD"),
            os.environ.get("MODE_WORKSPACE_NAME"),
        ).connect()
    )


if __name__ == "__main__":
    main()
