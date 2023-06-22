import os
from shipyard_blueprints import SlackClient

def main():
    slack_token = os.getenv('SLACK_TOKEN')
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
