import os
from shipyard_blueprints import SlackClient


def get_args():
    args = {}
    args['slack_token'] = os.getenv['SLACK_TOKEN']
    return args


def main():
    args = get_args()
    slack_token = args['slack_token']
    slack = SlackClient(slack_token)
    try:
        conn = slack.connect()
        slack.logger.info("Successfully connected to Slack")
        return 0
    except Exception as e:
        slack.logger.error("Could not connect to Slack")
        return 1


if __name__ == '__main__':
    main()
