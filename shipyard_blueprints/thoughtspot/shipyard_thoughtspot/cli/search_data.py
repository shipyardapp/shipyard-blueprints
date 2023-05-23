import argparse
from shipyard_thoughtspot import ThoughtSpotClient


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", dest="token", required=True)
    parser.add_argument("--query", dest="query", required=True)
    parser.add_argument("--table-id", dest="table_id", required=True)
    parser.add_argument("--num-rows", dest="num_rows", default="", required=False)
    parser.add_argument(
        "--file-name", dest="file_name", default="search_data.csv", required=False
    )
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    args_dict = {
        "query": args.query,
        "table_identifier": args.table_id,
        "num_rows": None if args.num_rows == "" else args.num_rows,
    }
    file_name = args.file_name
    client = ThoughtSpotClient(token=args.token)
    try:
        client.logger.info(f"Attempting to export search results")
        data = client.search_data(**args_dict, file_format="csv")
        client.logger.info(f"Writing file to {file_name}")
        data.to_csv(file_name, index=False)
    except Exception as e:
        client.logger.error("There was an error in exporting the query results")
        client.logger.error(e)
        return client.EXIT_CODE_BAD_REQUEST


if __name__ == "__main__":
    main()
