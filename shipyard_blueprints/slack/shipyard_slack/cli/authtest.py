import os
import sys
from shipyard_slack.slack import SlackClient


def main():
    sys.exit(SlackClient(os.getenv("SLACK_TOKEN")).connect())


if __name__ == "__main__":
    main()
