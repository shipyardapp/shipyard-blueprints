import os
import sys
from shipyard_airtable import AirtableClient


def main():
    airtable = AirtableClient(api_key=os.environ["AIRTABLE_API_KEY"])
    try:
        conn = airtable.connect()
        if conn == 200:
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception as e:
        sys.exit(1)


if __name__ == "__main__":
    main()
