import os
from shipyard_blueprints import AirtableClient


def get_args():
    args = {}
    args['api_key'] = os.environ['AIRTABLE_API_KEY']
    return args


def main():
    args = get_args()
    api_key = args['api_key']
    airtable = AirtableClient(api_key=api_key)
    try:
        conn = airtable.connect()
        if conn == 200:
            return 0
        else :
            return 1
    except Exception as e:
        return 1


if __name__ == '__main__':
    main()
