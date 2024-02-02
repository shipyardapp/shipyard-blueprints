import os
import sys
from shipyard_shortcut import ShortcutClient


def main():
    sys.exit(ShortcutClient(access_token=os.getenv("SHORTCUT_ACCESS_TOKEN")).connect())


if __name__ == "__main__":
    main()
