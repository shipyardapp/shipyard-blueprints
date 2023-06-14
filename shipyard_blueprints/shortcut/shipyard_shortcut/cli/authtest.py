import os
from shipyard_shortcut import ShortcutClient


def main():
    return ShortcutClient(access_token=os.getenv('SHORTCUT_ACCESS_TOKEN')).connect()


if __name__ == '__main__':
    main()
