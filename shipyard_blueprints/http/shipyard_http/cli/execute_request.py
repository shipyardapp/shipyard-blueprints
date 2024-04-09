import argparse
import sys

import requests
import shipyard_bp_utils as shipyard_utils
from shipyard_bp_utils.artifacts import Artifact
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--method",
        dest="method",
        required=True,
        choices={"GET", "POST", "PUT", "PATCH"},
    )
    parser.add_argument("--url", dest="url", required=True)
    parser.add_argument(
        "--authorization-header",
        dest="authorization_header",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--content-type", dest="content_type", required=False, default=None
    )
    parser.add_argument("--message", dest="message", required=False)
    parser.add_argument(
        "--print-response",
        dest="print_response",
        default="FALSE",
        choices={"TRUE", "FALSE"},
        required=False,
    )
    parser.add_argument(
        "--destination-file-name",
        dest="destination_file_name",
        default="response.txt",
        required=False,
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    return parser.parse_args()


def execute_request(method, url, headers=None, message=None, params=None):
    try:
        request = requests.request(method, url, headers=headers, data=message, params=params)
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
    return request


def write_response_to_file(req, destination_name):
    with open(destination_name, "w") as response_output:
        response_output.write(req.text)
    return


def main():
    args = get_args()

    headers = {}
    method = args.method
    url = args.url
    url_hash = shipyard_utils.text.hash_text(url, "sha256")

    if args.authorization_header:
        headers["Authorization"] = args.authorization_header
    if args.content_type:
        headers["Content-Type"] = args.content_type

    message = args.message
    print_response = shipyard_utils.args.convert_to_boolean(args.print_response)
    artifact = Artifact("httprequest")

    destination_folder_name = shipyard_utils.files.clean_folder_name(args.destination_folder_name)
    if destination_folder_name:
        shipyard_utils.files.create_folder_if_dne(destination_folder_name)

    destination_name = shipyard_utils.files.combine_folder_and_file_name(
        destination_folder_name, args.destination_file_name
    )

    request = execute_request(method, url, headers, message)
    write_response_to_file(request, destination_name)

    logger.info(f"Successfully sent request {url} and stored response to {destination_name}.")

    artifact_filename = f"{method.lower()}_{url_hash}"
    artifact.responses.write_json(f"{artifact_filename}", request.text)

    if print_response:
        logger.info(f"\n\n Response body: {request.content}")


if __name__ == "__main__":
    main()
