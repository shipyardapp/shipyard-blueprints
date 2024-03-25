import argparse
import sys
import shipyard_bp_utils as shipyard
from shipyard_redshift import RedshiftClient
from shipyard_templates import ExitCodeException, ShipyardLogger, Database

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", dest="username", required=False)
    parser.add_argument("--password", dest="password", required=False)
    parser.add_argument("--host", dest="host", required=False)
    parser.add_argument("--database", dest="database", required=False)
    parser.add_argument("--port", dest="port", default="5439", required=False)
    parser.add_argument("--query", dest="query", required=True)
    parser.add_argument(
        "--destination-file-name",
        dest="destination_file_name",
        default="output.csv",
        required=True,
    )
    parser.add_argument(
        "--destination-folder-name",
        dest="destination_folder_name",
        default="",
        required=False,
    )
    parser.add_argument(
        "--file-header", dest="file_header", default="True", required=False
    )
    parser.add_argument("--url-parameters", dest="url_parameters", required=False)
    args = parser.parse_args()

    if args.host and not (args.database or args.username):
        parser.error("--host requires --database and --username")
    if args.database and not (args.host or args.username):
        parser.error("--database requires --host and --username")
    if args.username and not (args.host or args.username):
        parser.error("--username requires --host and --username")
    return args


def main():
    args = get_args()
    target_file = args.destination_file_name
    target_dir = args.destination_folder_name
    target_path = shipyard.files.combine_folder_and_file_name(
        folder_name=target_dir, file_name=target_file
    )
    file_header = shipyard.args.convert_to_boolean(args.file_header)
    query = args.query
    redshift_args = {
        "host": args.host,
        "user": args.username,
        "pwd": args.password,
        "database": args.database,
        "port": args.port,
        "url_params": args.url_parameters if args.url_parameters != "" else None,
    }
    redshift = RedshiftClient(**redshift_args)
    try:
        if target_dir:
            shipyard.files.create_folder_if_dne(target_dir)
        redshift.read_chunks(query, target_path, file_header)
        logger.info(f"Successfully downloaded query results to {target_path}")

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(
            f"An unexpected error occurred when attempting to fetch results from Redshift. Message from the server reads: {e}"
        )
        sys.exit(Database.EXIT_CODE_UNKNOWN)
    finally:
        redshift.close()


if __name__ == "__main__":
    main()
