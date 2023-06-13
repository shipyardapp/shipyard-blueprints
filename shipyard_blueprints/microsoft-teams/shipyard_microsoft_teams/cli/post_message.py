import argparse
import sys

import requests


def main():
    parser = argparse.ArgumentParser(description='Send a message to Microsoft Teams using an incoming webhook')
    parser.add_argument('--webhook-url', required=True, help='URL of the incoming webhook')
    parser.add_argument('--message-content', required=True, help='Content of the message to be sent')
    parser.add_argument('--message-title', required=False, help='Title of the message to be sent')
    args = parser.parse_args()

    response = requests.post(args.webhook_url, json={'title': args.message_title, 'text': args.message_content})

    if response.ok:
        print('Message sent successfully')
    else:
        print('Message failed to send')
        sys.exit(1)


if __name__ == '__main__':
    main()
