import argparse
from shipyard_thoughtspot import ThoughtSpotClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", dest="token", required=True)
    parser.add_argument(
        "--metadata-identifier", dest="metadata_identifier", required=True
    )
    parser.add_argument(
        "--file-format", dest="file_format", default="csv", required=True
    )
    parser.add_argument(
        "--runtime-filter", dest="runtime_filter", default="", required=False
    )
    parser.add_argument(
        "--runtime-sort", dest="runtime_sort", default="", required=False
    )
    parser.add_argument(
        "--file-name", dest="file_name", default="liveboard", required=False
    )

    args = parser.parse_args()
    return args


def main():
    args = get_args()
    args_dict = {
        "metadata_identifier": args.metadata_identifier,
        "file_format": args.file_format,
        "runtime_filter": None if args.runtime_filter == "" else args.runtime_filter,
        "runtime_sort": None if args.runtime_sort == "" else args.runtime_sort,
        "file_name": args.file_name,
    }
    client = ThoughtSpotClient(token=args.token)
    try:
        response = client.get_live_report(**args_dict)
        if response.status_code == 200:
            client.logger.info(
                f"Successfully exported live report to {args.file_name}.{args.file_format}"
            )
    except Exception as e:
        client.logger.error("There was an error in exporting the live report")
        client.logger.error(e)
        return client.EXIT_CODE_LIVE_REPORT_ERROR


if __name__ == "__main__":
    main()
