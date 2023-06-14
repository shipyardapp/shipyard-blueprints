import argparse
import sys
import logging
import requests


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
        handlers=[logging.StreamHandler()],
    )

    parser = argparse.ArgumentParser(
        description="Send a message to Microsoft Teams using an incoming webhook"
    )
    parser.add_argument(
        "--webhook-url", required=True, help="URL of the incoming webhook"
    )
    parser.add_argument(
        "--message-content", required=True, help="Content of the message to be sent"
    )
    parser.add_argument(
        "--message-title", required=False, help="Title of the message to be sent"
    )
    args = parser.parse_args()

    response = requests.post(
        args.webhook_url,
        json={"title": args.message_title, "text": args.message_content},
    )

    if response.ok:
        logging.info("Message sent successfully")
    else:
        logging.error(
            f"An error occurred when attempting to post message to Microsoft teams.\n"
            f"{response.text}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
