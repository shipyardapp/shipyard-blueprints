import argparse
import sys

from shipyard_bp_utils import files as shipyard
from shipyard_templates import ExitCodeException, ShipyardLogger, DataVisualization

from shipyard_tableau import tableau_utils

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", dest="username", required=True)
    parser.add_argument("--password", dest="password", required=True)
    parser.add_argument(
        "--sign-in-method",
        dest="sign_in_method",
        default="username_password",
        choices={"username_password", "access_token"},
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


def generate_view_content(server, view_id, file_type):
    """
    Given a specific view_id, populate the view and return the bytes necessary for creating the file.
    """
    view_content = None
    view_object = server.views.get_by_id(view_id)
    if file_type == "png":
        server.views.populate_image(view_object)
        view_content = view_object.image
    if file_type == "pdf":
        server.views.populate_pdf(view_object, req_options=None)
        view_content = view_object.pdf
    if file_type == "csv":
        server.views.populate_csv(view_object, req_options=None)
        view_content = view_object.csv
    return view_content


def write_view_content_to_file(
    destination_full_path, view_content, file_type, view_name
):
    """
    Write the byte contents to the specified file path.
    """
    try:
        with open(destination_full_path, "wb") as f:
            if file_type == "csv":
                f.writelines(view_content)
            else:
                f.write(view_content)
        logger.info(f"Successfully downloaded {view_name} to {destination_full_path}")
    except OSError as e:
        raise ExitCodeException(
            f"Could not write file: {destination_full_path}",
            tableau_utils.EXIT_CODE_FILE_WRITE_ERROR,
        ) from e


def main():
    try:
        args = get_args()

        # Set all file parameters
        destination_folder_name = shipyard.clean_folder_name(
            args.destination_folder_name
        )
        if destination_folder_name:
            shipyard.create_folder_if_dne(
                destination_folder_name=destination_folder_name
            )

        destination_full_path = shipyard.combine_folder_and_file_name(
            folder_name=destination_folder_name, file_name=args.destination_file_name
        )

        server, connection = tableau_utils.connect_to_tableau(
            args.username,
            args.password,
            args.site_id,
            args.server_url,
            args.sign_in_method,
        )

        with connection:
            view_name = args.view_name
            file_type = args.file_type

            project_id = tableau_utils.get_project_id(
                server=server, project_name=args.project_name
            )
            workbook_id = tableau_utils.get_workbook_id(
                server=server, project_id=project_id, workbook_name=args.workbook_name
            )
            view_id = tableau_utils.get_view_id(
                server=server,
                project_id=project_id,
                workbook_id=workbook_id,
                view_name=view_name,
            )

            view_content = generate_view_content(
                server=server, view_id=view_id, file_type=file_type
            )

            write_view_content_to_file(
                destination_full_path=destination_full_path,
                view_content=view_content,
                file_type=file_type,
                view_name=view_name,
            )

    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)

    except Exception as e:
        logger.error(f"An unexpected error occurred {e}")
        sys.exit(DataVisualization.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
