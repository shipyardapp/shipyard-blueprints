import argparse
import json
import os
import sys

import requests
from shipyard_bp_utils import files as shipyard
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", dest="url", required=True)
    parser.add_argument(
        "--custom-headers", dest="custom_headers", required=False, default="{}"
    )
    # authentication header is separated, so it can be obfuscated
    # as a password type in the Blueprint.
    parser.add_argument(
        "--authentication-headers",
        dest="authentication_headers",
        required=False,
        default="{}",
    )
    parser.add_argument(
        "--destination-file-name",
        dest="destination_file_name",
        default="",
        required=False,
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    return parser.parse_args()


def extract_filename_from_url(url):
    return url.split("/")[-1]


def download_file(url, destination_name, headers=None, params=None):
    logger.info(f"Currently downloading the file from {url}...")
    try:
        with requests.get(url, headers=headers, stream=True, params=params) as r:
            r.raise_for_status()
            with open(destination_name, "wb") as f:
                for chunk in r.iter_content(chunk_size=(16 * 1024 * 1024)):
                    f.write(chunk)

        logger.info(f"Successfully downloaded {url} to {destination_name}.")
    except requests.exceptions.HTTPError as eh:
        logger.error("URL returned an HTTP Error.\n", eh)
        sys.exit(1)
    except requests.exceptions.ConnectionError as ec:
        logger.error(
            "Could not connect to the URL. Check to make sure that it was typed correctly.\n",
            ec,
        )
        sys.exit(2)
    except requests.exceptions.Timeout as et:
        logger.error("Timed out while connecting to the URL.\n", et)
        sys.exit(3)
    except requests.exceptions.RequestException as e:
        logger.error("Unexpected error occurred. Please try again.\n", e)
        sys.exit(4)
    return


def add_to_headers(headers, key, value):
    headers[key] = value
    return headers


def main():
    args = get_args()
    url = args.url

    if args.custom_headers == "":
        custom_headers = json.loads("{}")
    else:
        custom_headers = json.loads(args.custom_headers)

    if args.authentication_headers == "":
        authentication_headers = json.loads("{}")
    else:
        authentication_headers = json.loads(args.authentication_headers)
    destination_file_name = args.destination_file_name or extract_filename_from_url(url)
    destination_folder_name = shipyard.clean_folder_name(args.destination_folder_name)
    destination_name = shipyard.combine_folder_and_file_name(
        destination_folder_name, destination_file_name
    )
    headers = {}
    params = {}

    if destination_folder_name:
        shipyard.create_folder_if_dne(destination_folder_name)
    if custom_headers:
        for key, value in custom_headers.items():
            headers = add_to_headers(headers, key, value)
    if authentication_headers:
        for key, value in custom_headers.items():
            headers = add_to_headers(headers, key, value)

    download_file(url, destination_name, headers, params)
    written_file_size = os.path.getsize(destination_name)
    logger.info(
        f"The downloaded file contained {written_file_size / 1000000}MB of data."
    )
    if written_file_size == 0:
        logger.warning("File downloaded contained no data.")
        sys.exit(5)


if __name__ == "__main__":
    main()
