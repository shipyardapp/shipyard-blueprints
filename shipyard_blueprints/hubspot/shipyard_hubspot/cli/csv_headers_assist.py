import csv
import argparse
import sys

from shipyard_hubspot import HubspotClient
from shipyard_templates import ExitCodeException


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--object-type", dest="object_type", required=True)
    parser.add_argument("--csv-file", dest="csv_file")
    return parser.parse_args()


def extract_csv_headers(csv_filename):
    with open(csv_filename, "r") as file:
        reader = csv.reader(file)
        return next(reader)


def headers_match(headers, hubspot_property_names):
    return all(header in hubspot_property_names for header in headers)


def log_unmatched_headers(client, headers, hubspot_property_names):
    if unmatched_headers := [
        header for header in headers if header not in hubspot_property_names
    ]:
        client.logger.info("Detected issues with the following header(s):")
        for header in unmatched_headers:
            client.logger.info(header)


def format_property_message(hubspot_properties):
    property_msg = (
        "\nThe following properties are available for use in the CSV file: \n"
    )
    property_msg += f"\n{'-' * 10}"  # 10 dashes as a separator
    property_msg += "\nName: The value you will use in the CSV file."
    property_msg += "\nLabel: The value you will see in the Hubspot UI."
    property_msg += "\nDescription: The description of what the value is "
    property_msg += "\nType: The datatype of the value."

    property_msg += f"\n{'-' * 10}"  # 10 dashes as a separator
    for hubspot_property in hubspot_properties:
        property_msg += f'\nName: {hubspot_property["name"]}'
        property_msg += f'\nLabel: {hubspot_property["label"]}'
        property_msg += f'\nDescription: {hubspot_property["description"]}'
        property_msg += f'\nType: {hubspot_property["type"]}'
        property_msg += f"\n{'-' * 10}"  # 10 dashes as a separator

    return property_msg


def main():
    # TODO: Add support for multiple files
    args = get_args()
    try:
        client = HubspotClient(access_token=args.access_token)
        hubspot_properties = client.get_available_contact_properties(args.object_type)
        hubspot_property_names = [
            hubspot_property["name"] for hubspot_property in hubspot_properties
        ]

        if args.csv_file:
            client.logger.info(
                f"Checking if all headers in {args.csv_file} exist in the Hubspot properties."
            )
            headers = extract_csv_headers(args.csv_file)
            if headers_match(headers, hubspot_property_names):
                client.logger.info(
                    "All headers in the CSV file are valid Hubspot properties."
                )
            else:
                log_unmatched_headers(client, headers, hubspot_property_names)
                sys.exit(client.EXIT_CODE_INVALID_INPUT)
        else:
            client.logger.info(format_property_message(hubspot_properties))

    except ExitCodeException as e:
        client.logger.error(e)
        sys.exit(e.exit_code)
    except FileNotFoundError as e:
        client.logger.error(e)
        sys.exit(client.EXIT_CODE_FILE_NOT_FOUND)
    except Exception as e:
        client.logger.error(e)
        sys.exit(client.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
