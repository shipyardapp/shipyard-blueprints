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
        "--file-name", dest="file_name", default="export", required=False
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
        client.logger.info(f"Attempting to export answer report")
        response = client.get_answer_report(**args_dict)
        if response.status_code == 200:
            client.logger.info(
                f"Successfully exported answer report to {args.file_name}.{args.file_format}"
            )
    except Exception as e:
        client.logger.error("There was an error in exporting the answer report")
        client.logger.error(e)
        return client.EXIT_CODE_ANSWER_REPORT_ERROR


if __name__ == "__main__":
    main()
