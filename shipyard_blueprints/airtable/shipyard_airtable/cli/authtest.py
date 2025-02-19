# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "shipyard-airtable",
# ]
# ///
import os
import sys
from shipyard_airtable import AirtableClient


def main():
    sys.exit(AirtableClient(api_key=os.environ.get("AIRTABLE_API_KEY")).connect())


if __name__ == "__main__":
    main()
