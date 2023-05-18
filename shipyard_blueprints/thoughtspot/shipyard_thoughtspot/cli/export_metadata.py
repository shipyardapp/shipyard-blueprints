import argparse
import json
from shipyard_thoughtspot import ThoughtSpotClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", dest="token", required=True)
    parser.add_argument(
        "--metadata-identifier",
        dest="metadata_identifier",
        required=True,
    )
    parser.add_argument(
        "--file-name", dest="file_name", required=False, default="metadata.json"
    )
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    args_dict = {
        "token": args.token,
        "metadata_identifier": args.metadata_identifier,
    }
    file_name = args.file_name

    client = ThoughtSpotClient(args.token)
    client.logger.info(f"Getting metadata for {args.metadata_identifier}")
    try:
        response = client.get_metadata(args.metadata_identifier)
        if response.status_code == 200:
            client.logger.info(f"Successfully fetched metadata")
            with open(file_name, "w") as f:
                json.dump(response.json(), f)
                f.close()
            client.logger.info(f"Metadata saved to {file_name}")
        else:
            client.logger.error(f"There was an error fetching metadata")
            client.logger.error(f"Status code: {response.status_code}")
            client.logger.error(f"Message from the API is: {response.json()}")
    except Exception as e:
        client.logger.error("Error in sending API request")
        client.logger.error(e)
        return client.EXIT_CODE_BAD_REQUEST


if __name__ == "__main__":
    main()
