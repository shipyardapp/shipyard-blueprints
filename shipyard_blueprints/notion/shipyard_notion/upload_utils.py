import argparse
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any, Optional, List


from csv2notion.cli_args import parse_args
from csv2notion.cli_steps import convert_csv_to_notion_rows, new_database, upload_rows
from csv2notion.csv_data import CSVData
from csv2notion.notion_db import get_collection_id, get_notion_client
from csv2notion.utils_exceptions import CriticalError, NotionError

def map_args(args:argparse.Namespace) -> List[str]:
    """ Takes the command line arguments passed in from Shipyard then maps them to the appropriate arguments for csv2notiun

    Args:
        args: The command line arguments from shipyard. Options are:
        1) --token (Required)
        2) --url 
        3) --file-name
        4) --file-name-match-type  (exact_match, regex_match)
        5) --folder-name 
        6 ) --insert-method (replace, append)
    """
    arg_str = f"--token {args.token}"
    if args.url:
        arg_str += f" --url {args.url}"
    if args.folder_name:
        file_name = os.path.normpath(os.path.join(args.folder_name, args.file_name))
    else:
        file_name = args.file_name

    # handle cases for either just a single file or multiple files
    if args.file_name_match_type == 'regex_match':
        file_name = f"*{file_name}"

    arg_str += f" {file_name}"

    return arg_str.split(' ')
    
    


# NOTE: this is code directly from the csv2notion package
logger = logging.getLogger(__name__)

# TODO: Make this run by passing in remapped args taken provided by shipyard
def cli(*argv: str) -> None:
    args = parse_args(argv)

    setup_logging(is_verbose=args.verbose, log_file=args.log)

    logger.info("Validating CSV & Notion DB schema")

    csv_data = CSVData(
        args.csv_file, args.column_types, args.fail_on_duplicate_csv_columns
    )

    if not csv_data:
        raise CriticalError("CSV file is empty")

    client = get_notion_client(
        args.token,
        is_randomize_select_colors=args.randomize_select_colors,
    )

    if args.url:
        collection_id = get_collection_id(client, args.url)
    else:
        collection_id = new_database(args, client, csv_data)

    notion_rows = convert_csv_to_notion_rows(csv_data, client, collection_id, args)

    logger.info("Uploading {0}...".format(args.csv_file.name))

    upload_rows(
        notion_rows,
        client=client,
        collection_id=collection_id,
        is_merge=args.merge,
        max_threads=args.max_threads,
    )

    logger.info("Done!")


def setup_logging(is_verbose: bool = False, log_file: Optional[Path] = None) -> None:
    # logging.basicConfig(format="%(levelname)s: %(message)s")
    logging.basicConfig(format = "%(asctime)s - %(name)s - %(levelname)s -%(lineno)d: %(message)s"
                        )

    logging.getLogger("csv2notion").setLevel(
        logging.DEBUG if is_verbose else logging.INFO
    )

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)-8.8s] %(message)s")
        )
        logging.getLogger("csv2notion").addHandler(file_handler)

    logging.getLogger("notion").setLevel(logging.WARNING)


def abort(*_: Any) -> None:  # pragma: no cover
    print("\nAbort")  # noqa: WPS421
    os._exit(1)


def main() -> None:
    signal.signal(signal.SIGINT, abort)

    try:
        cli(*sys.argv[1:])
    except (NotionError, CriticalError) as e:
        logger.critical(str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(1)
