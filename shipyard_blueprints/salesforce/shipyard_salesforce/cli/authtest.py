import os
import sys

from shipyard_salesforce import SalesforceClient


def main():
    try:
        client = SalesforceClient(
            access_token=os.getenv("SALESFORCE_ACCESS_TOKEN"),
            domain=os.getenv("SALESFORCE_DOMAIN"),
            username=os.getenv("SALESFORCE_USERNAME"),
            password=os.getenv("SALESFORCE_PASSWORD"),
            security_token=os.getenv("SALESFORCE_SECURITY_TOKEN"),
            consumer_key=os.getenv("SALESFORCE_CONSUMER_KEY"),
            consumer_secret=os.getenv("SALESFORCE_CONSUMER_SECRET"),
        )
    except Exception as e:
        print(e)
        sys.exit(1)
    else:
        sys.exit(client.connect())


if __name__ == "__main__":
    main()
