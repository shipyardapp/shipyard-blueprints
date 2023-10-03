import os
import sys
import pandas
import argparse

from shipyard_salesforce import SalesforceClient
from shipyard_templates import ExitCodeException


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=False)
    parser.add_argument("--consumer-key", dest="consumer_key", required=False)
    parser.add_argument("--consumer-secret", dest="consumer_secret", required=False)
    parser.add_argument("--username", dest="username", required=False)
    parser.add_argument("--password", dest="password", required=False)
    parser.add_argument("--security-token", dest="security_token", required=False)
    parser.add_argument("--domain", dest="domain", required=False)

    parser.add_argument("--filename", dest="filename", required=True)
    parser.add_argument("--field-names", dest="field_names", required=True)
    parser.add_argument("--object-type", dest="object_type", required=True)

    return parser.parse_args()


def main():
    args = get_args()
    fieldnames = [
        field.strip() for field in args.field_names.split(",")
    ]  # handles removing leading and trailing whitespace
    try:
        salesforce = SalesforceClient(
            access_token=args.access_token,
            domain=args.domain,
            username=args.username,
            password=args.password,
            security_token=args.security_token,
            consumer_key=args.consumer_key,
            consumer_secret=args.consumer_secret,
        )
    except ExitCodeException as e:
        print(f"Failed to create SalesforceClient: {e}")
        sys.exit(e.exit_code)

    try:
        data = salesforce.export_data(sobject=args.object_type, fieldnames=fieldnames)
        filename = args.filename

        filename = filename.strip()
        if not filename.endswith(".csv"):
            filename += ".csv"

        salesforce.logger.info(f"Attempting to write to file {filename}")
        pandas.DataFrame(data).to_csv(filename, index=False)
        salesforce.logger.info(f"Wrote to file {filename}")

    except ExitCodeException as e:
        salesforce.logger.error(e)
        sys.exit(e.exit_code)


if __name__ == "__main__":
    main()
