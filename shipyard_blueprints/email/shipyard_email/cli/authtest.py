# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "shipyard-email",
# ]
# ///
import os
import sys

from shipyard_email.email_client import EmailClient


def main():
    sys.exit(
        EmailClient(
            smtp_host=os.getenv("EMAIL_SMTP_HOST"),
            smtp_port=os.getenv("EMAIL_SMTP_PORT"),
            username=os.getenv("EMAIL_USERNAME"),
            password=os.getenv("EMAIL_PASSWORD"),
        ).connect()
    )


if __name__ == "__main__":
    main()
