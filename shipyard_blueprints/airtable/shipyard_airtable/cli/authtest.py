from shipyard_blueprints import AirtableClient
import argparse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", dest='api_key', required=True)
    parser.add_argument('--base-id', dest='base_id', required=True)
    parser.add_argument('--table-name', dest='table_name', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    api_key = args.api_key
    base_id = args.base_id
    table = args.table_name
    airtable = AirtableClient(base_id, api_key, table)
    try:
        conn = airtable.connect()
        return 0
    except Exception as e:
        return 1


if __name__ == '__main__':
    main()
