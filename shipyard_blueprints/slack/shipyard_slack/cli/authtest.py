import os
from shipyard_blueprints import SlackClient

def main():
    return SlackClient(os.getenv('SLACK_TOKEN')).connect()


if __name__ == '__main__':
    main()
