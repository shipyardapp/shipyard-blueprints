import os
import sys
import csv
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

    parser.add_argument("--object-type", dest="object_type", required=True)
    parser.add_argument("--csv-file", dest="csv_file")

    return parser.parse_args()


def extract_csv_headers(csv_filename):
    with open(csv_filename, "r") as file:
        reader = csv.reader(file)
        return next(reader)


def headers_match(headers, salesforce_property_names):
    return all(header in salesforce_property_names for header in headers)


def log_unmatched_headers(client, headers, salesforce_property_names):
    if unmatched_headers := [
        header for header in headers if header not in salesforce_property_names
    ]:
        client.logger.info("Detected issues with the following header(s):")
        for header in unmatched_headers:
            client.logger.info(header)


def format_property_message(salesforce_properties):
    property_msg = (
        "\nThe following properties are available for use in the CSV file: \n"
    )
    property_msg += f"\n{'-' * 10}"  # 10 dashes as a separator
    property_msg += "\nName: The value you will use in the CSV file."
    property_msg += "\nLabel: The value you will see in the Salesforce UI."
    property_msg += "\nType: The datatype of the value."
    property_msg += "\nCreatable: Indicates if a field can be created by the user."
    property_msg += "\nUpdateable: Indicates if a field can be updated by the user."
    property_msg += "\nNillable: Indicates if a field can be left blank."
    property_msg += "\nUnique: Indicates if a field must have a unique value."

    property_msg += f"\n{'-' * 10}"  # 10 dashes as a separator
    for salesforce_property in salesforce_properties:
        if (
            not salesforce_property["createable"]
            and not salesforce_property["updateable"]
        ):
            continue

        property_msg += f'\nName: {salesforce_property["name"]}'
        property_msg += f'\nLabel: {salesforce_property["label"]}'
        property_msg += f'\nType: {salesforce_property["type"]}'
        property_msg += f'\nCreatable: {salesforce_property["createable"]}'
        property_msg += f'\nUpdateable: {salesforce_property["updateable"]}'
        property_msg += f'\nNillable: {salesforce_property["nillable"]}'
        property_msg += f'\nUnique: {salesforce_property["unique"]}'
        property_msg += f"\n{'-' * 10}"  # 10 dashes as a separator

    return property_msg


def main():
    args = get_args()
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
        salesforce_properties = salesforce.get_sobject_fields(args.object_type)
        salesforce_property_names = [
            salesforce_property["name"] for salesforce_property in salesforce_properties
        ]
        if args.csv_file:
            salesforce.logger.info(
                f"Checking if all headers in {args.csv_file} exist in the salesforce properties."
            )
            headers = extract_csv_headers(args.csv_file)
            if headers_match(headers, salesforce_property_names):
                salesforce.logger.info(
                    "All headers in the CSV file are valid salesforce properties."
                )
            else:
                log_unmatched_headers(salesforce, headers, salesforce_property_names)
                sys.exit(salesforce.EXIT_CODE_INVALID_INPUT)
        else:
            salesforce.logger.info(format_property_message(salesforce_properties))

    except ExitCodeException as e:
        salesforce.logger.error(e)
        sys.exit(e.exit_code)
    except FileNotFoundError as e:
        salesforce.logger.error(e)
        sys.exit(salesforce.EXIT_CODE_FILE_NOT_FOUND)
    except Exception as e:
        salesforce.logger.error(e)
        sys.exit(salesforce.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
