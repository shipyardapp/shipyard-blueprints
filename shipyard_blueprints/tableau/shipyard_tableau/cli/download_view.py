import argparse
import sys
import shipyard_bp_utils as shipyard
import csv
from shipyard_tableau import TableauClient
from shipyard_tableau.errors import EXIT_CODE_TABLEAU_VIEW_DOWNLOAD_ERROR
from shipyard_templates import ShipyardLogger, ExitCodeException
from shipyard_templates import DataVisualization
from shipyard_tableau.errors import (
    EXIT_CODE_STATUS_INCOMPLETE,
    EXIT_CODE_FINAL_STATUS_ERRORED,
    EXIT_CODE_FINAL_STATUS_CANCELLED,
)

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", dest="username", required=False)
    parser.add_argument("--password", dest="password", required=False)
    parser.add_argument("--access-token-name", dest="access_token_name", required=False)
    parser.add_argument(
        "--access-token-value", dest="access_token_value", required=False
    )
    parser.add_argument("--client-id", dest="client_id", required=False)  # for jwt
    parser.add_argument(
        "--client-secret", dest="client_secret", required=False
    )  # for jwt
    parser.add_argument(
        "--secret-value", dest="secret_value", required=False
    )  # for jwt

    parser.add_argument(
        "--sign-in-method",
        dest="sign_in_method",
        default="username_password",
        choices={"username_password", "access_token", "jwt"},
        required=False,
    )
    parser.add_argument("--site-id", dest="site_id", required=True)
    parser.add_argument("--server-url", dest="server_url", required=True)
    parser.add_argument("--view-name", dest="view_name", required=True)
    parser.add_argument(
        "--file-type",
        dest="file_type",
        choices=["png", "pdf", "csv"],
        required=True,
    )
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
    parser.add_argument("--file-options", dest="file_options", required=False)
    parser.add_argument("--workbook-name", dest="workbook_name", required=True)
    parser.add_argument("--project-name", dest="project_name", required=True)
    return parser.parse_args()


def write_view_content_to_file(destination_full_path, view_content, file_type):
    """
    Write the byte contents to the specified file path.
    """
    try:
        if file_type == "csv":
            content_str = view_content.decode("utf-8")
            lines = content_str.splitlines()
            with open(destination_full_path, "w", newline="") as f:
                writer = csv.writer(f)
                for line in lines:
                    writer.writerow(line.split(","))
        else:
            with open(destination_full_path, "wb") as f:
                f.write(view_content)
    except OSError as e:
        raise ExitCodeException(
            f"Could not write file: {destination_full_path}",
            EXIT_CODE_TABLEAU_VIEW_DOWNLOAD_ERROR,
        ) from e


def main():
    try:
        args = get_args()
        sign_in_method = args.sign_in_method
        workbook_name = args.workbook_name
        view_name = args.view_name
        project_name = args.project_name
        file_path = shipyard.files.combine_folder_and_file_name(
            args.destination_folder_name, args.destination_file_name
        )

        client = TableauClient(server_url=args.server_url, site=args.site_id)
        if sign_in_method == "username_password":
            client.add_user_and_pwd(args.username, args.password)
        elif sign_in_method == "access_token":
            client.add_personal_access_token(
                args.access_token_name, args.access_token_value
            )
        elif sign_in_method == "jwt":
            client.add_jwt(
                args.username, args.client_id, args.client_secret, args.secret_value
            )

        client.connect(sign_in_method=sign_in_method)

        view_id = client.get_view_id(
            view_name=view_name, project_name=project_name, workbook_name=workbook_name
        )

        view_content = client.download_view(view_id=view_id, file_format=args.file_type)

        if args.destination_folder_name:
            shipyard.files.create_folder_if_dne(args.destination_folder_name)

        write_view_content_to_file(
            destination_full_path=file_path,
            view_content=view_content,
            file_type=args.file_type,
        )

        logger.info(f"Downloaded view content successfully to {file_path}")

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(DataVisualization.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
