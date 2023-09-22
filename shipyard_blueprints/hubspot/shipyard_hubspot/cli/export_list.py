import sys
import time
import requests
import argparse

from shipyard_hubspot import HubspotClient
from shipyard_templates import ExitCodeException


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=True)
    parser.add_argument("--export-name", dest="export_name", required=True)
    parser.add_argument("--object-type", dest="object_type", required=True)
    parser.add_argument("--list-id", dest="list_id", required=True)
    parser.add_argument("--object-properties", dest="object_properties", required=True)
    parser.add_argument(
        "--destination-filename", dest="destination_filename", required=True
    )
    return parser.parse_args()


def download_export(export_download_url, destination_filename):
    print(
        f"Downloading export file from {export_download_url} to {destination_filename}"
    )
    response = requests.get(url=export_download_url, stream=True)
    response.raise_for_status()
    with open(destination_filename, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded export file to {destination_filename}")


def export_and_wait(client, args):
    object_properties = [
        obj.strip() for obj in args.object_properties.split(",")
    ]  # Clean up the input
    client.logger.info(
        f"Attempting to export list {args.list_id} with object properties {object_properties}"
    )
    export_id = client.export_list(
        export_name=args.export_name,
        list_id=args.list_id,
        object_type=args.object_type,
        object_properties=object_properties,
    ).get("id")

    export_details = client.get_export(export_id)

    while export_details.get("status") in {"PROCESSING", "PENDING"}:
        client.logger.info(" Waiting 30 seconds...")
        time.sleep(30)

        export_details = client.get_export(export_id)

    return export_details


def main():
    args = get_args()
    client = HubspotClient(access_token=args.access_token)

    try:
        export_details = export_and_wait(client, args)
        download_export(
            export_details.get("result"), f"{args.destination_filename}.csv"
        )
    except ExitCodeException as e:
        client.logger.error(e)
        sys.exit(e.exit_code)


if __name__ == "__main__":
    main()
