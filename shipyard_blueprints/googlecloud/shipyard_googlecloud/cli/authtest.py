import os
import sys
from shipyard_googlecloud import GoogleCloudClient


def main():
    client = GoogleCloudClient(
        service_account=os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    try:
        client.connect()
        sys.exit(0)
    except Exception as e:
        client.logger.error(
            "Could not connect to Google Cloud with the service account provided"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
