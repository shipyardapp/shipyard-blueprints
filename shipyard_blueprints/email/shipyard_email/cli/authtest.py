import os
import sys
from shipyard_email import EmailClient


def main():
    sys.exit(
        EmailClient(
            smtp_host=os.getenv("EMAIL_SMTP_HOST"),
            smtp_port=os.getenv("EMAIL_SMTP_PORT"),
            username=os.getenv("EMAIL_USERNAME"),
            password=os.getenv("EMAIL_PASSWORD"),
            send_method=os.getenv("EMAIL_SEND_METHOD"),
        ).connect()
    )


if __name__ == "__main__":
    main()
