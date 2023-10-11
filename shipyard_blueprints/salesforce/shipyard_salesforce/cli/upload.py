import os
import sys
import argparse
import pandas

from shipyard_salesforce import SalesforceClient
from shipyard_templates import ExitCodeException
from shipyard_bp_utils.files import find_files_by_regex_or_exact_match


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-token", dest="access_token", required=False)
    parser.add_argument("--consumer-key", dest="consumer_key", required=False)
    parser.add_argument("--consumer-secret", dest="consumer_secret", required=False)
    parser.add_argument("--username", dest="username", required=False)
    parser.add_argument("--password", dest="password", required=False)
    parser.add_argument("--security-token", dest="security_token", required=False)
    parser.add_argument("--domain", dest="domain", required=False)

    parser.add_argument("--object-type", dest="object_type", required=True)
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_match_type",
        choices={"exact_match", "regex_match"},
        required=True,
    )
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", required=False
    )
    parser.add_argument("--source-file-name", dest="source_file_name", required=True)
    parser.add_argument("--import-operation", dest="import_operation", required=True)
    parser.add_argument("--id-field", dest="id_field", required=False)
    return parser.parse_args()


def read_file(file_path):
    """Read the contents of the given file and returns the values as a dict

    Args:
        file_path (str): The path to the file to read.

    Returns:
        str: The contents of the file.
    """
    file_extension = file_path.split(".")[-1].lower()

    # Load data based on file type
    if file_extension == "csv":
        df = pandas.read_csv(file_path)
    elif file_extension in ["xlsx", "xls"]:
        df = pandas.read_excel(file_path)
    else:
        raise ValueError("Unsupported file format")
    df = df.where(pandas.notnull(df), None)
    return df.to_dict(orient="records")


def main():
    args = get_args()
    errors = []

    try:
        salesforce = SalesforceClient(
            access_token=args.access_token,
            domain=args.domain,
            username=args.username,
            password=args.password,
            security_token=args.security_token,
            consumer_key=args.consumer_key,
            consumer_secret=args.consumer_secret,
        )
    except ExitCodeException as e:
        print(f"Failed to create SalesforceClient: {e}")
        sys.exit(e.exit_code)

    try:
        files = find_files_by_regex_or_exact_match(
            start_path=args.source_folder_name,
            pattern=args.source_file_name,
            exact_match=args.source_match_type == "exact_match",
            valid_extensions={".csv", ".xlsx", ".xls"},
        )
        if not files:
            raise ExitCodeException(
                f"Could not find any files matching {args.source_file_name} in {args.source_folder_name}",
                salesforce.EXIT_CODE_FILE_NOT_FOUND,
            )
        for file in files:
            salesforce.logger.info(f"Attempting to import data from {file}...")
            records = read_file(file)
            try:
                if args.import_operation == "insert":
                    salesforce.import_data(
                        sobject=args.object_type,
                        records=records,
                        import_type=args.import_operation,
                    )
                else:
                    salesforce.import_data(
                        sobject=args.object_type,
                        records=records,
                        import_type=args.import_operation,
                        id_field_key=args.id_field,
                    )
            except ExitCodeException as err:
                salesforce.logger.error(
                    f"Failed to {args.import_operation} the following file: {file}"
                )
                errors.append(
                    {"file": file, "error_msg": err.message, "exit_code": err.exit_code}
                )
            else:
                salesforce.logger.info(f"Successfully imported data from {file}")
    except ExitCodeException as e:
        salesforce.logger.error(e)
        sys.exit(e.exit_code)
    except ValueError as e:
        salesforce.logger.error(e)
        sys.exit(salesforce.EXIT_CODE_INVALID_INPUT)

    if errors:
        salesforce.logger.error("The following files(s) had failures:")
        for error in errors:
            salesforce.logger.error(error)

        first_error = errors[0]["exit_code"]
        if all(
            error["exit_code"] == first_error for error in errors
        ):  # Check if all errors have the same exit code
            sys.exit(first_error)
        else:
            sys.exit(1)


if __name__ == "__main__":
    main()
