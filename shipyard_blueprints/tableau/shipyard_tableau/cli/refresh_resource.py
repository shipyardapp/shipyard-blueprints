import argparse
import sys

from shipyard_bp_utils import args as shipyard
from shipyard_bp_utils.artifacts import Artifact
from shipyard_templates import ShipyardLogger, ExitCodeException, DataVisualization

from shipyard_tableau import tableau_utils

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", dest="username", required=True)
    parser.add_argument("--password", dest="password", required=True)
    parser.add_argument("--site-id", dest="site_id", required=True)
    parser.add_argument("--server-url", dest="server_url", required=True)
    parser.add_argument(
        "--sign-in-method",
        dest="sign_in_method",
        default="username_password",
        choices={"username_password", "access_token"},
        required=False,
    )
    parser.add_argument("--workbook-name", dest="workbook_name", required=False)
    parser.add_argument("--datasource-name", dest="datasource_name", required=False)
    parser.add_argument("--project-name", dest="project_name", required=True)
    parser.add_argument(
        "--check-status", dest="check_status", default="TRUE", required=False
    )
    return parser.parse_args()


def refresh_datasource(server, datasource_id, datasource_name):
    """
    Refreshes the data of the specified datasource_id.
    """

    try:
        datasource = server.datasources.get_by_id(datasource_id)
        refreshed_datasource = server.datasources.refresh(datasource)
        logger.info(f"Datasource {datasource_name} was successfully triggered.")
        return refreshed_datasource

    except Exception as e:
        logger.error(e)
        if "Resource Conflict" in e.args[0]:
            error_msg = (
                "A refresh or extract operation for the datasource is already underway."
            )
        elif "is not allowed." in e.args[0]:
            error_msg = (
                "Refresh or extract operation for the datasource is not allowed."
            )
        else:
            error_msg = ("An unknown refresh or extract error occurred.",)

        raise ExitCodeException(error_msg, tableau_utils.EXIT_CODE_REFRESH_ERROR) from e


def refresh_workbook(server, workbook_id, workbook_name):
    """
    Refreshes the data of the specified datasource_id.
    """

    try:
        workbook = server.workbooks.get_by_id(workbook_id)
        refreshed_workbook = server.workbooks.refresh(workbook)
        logger.info(f"Workbook {workbook_name} was successfully triggered.")
        return refreshed_workbook
    except Exception as e:

        if "Resource Conflict" in e.args[0]:
            error_msg = (
                "A refresh or extract operation for the workbook is already underway."
            )
        elif "is not allowed." in e.args[0]:
            error_msg = "Refresh or extract operation for the workbook is not allowed."
        else:
            error_msg = "An unknown refresh or extract error occurred."
        raise ExitCodeException(error_msg, tableau_utils.EXIT_CODE_REFRESH_ERROR)


def main():
    try:
        artifact = Artifact("tableau")
        args = get_args()

        workbook_name = args.workbook_name
        datasource_name = args.datasource_name
        should_check_status = shipyard.convert_to_boolean(args.check_status)

        server, connection = tableau_utils.connect_to_tableau(
            args.username,
            args.password,
            args.site_id,
            args.server_url,
            args.sign_in_method,
        )

        with connection:
            project_id = tableau_utils.get_project_id(server, args.project_name)

            # Allowing user to provide one or the other. These will form two separate Blueprints
            # that use the same underlying script.
            if datasource_name:
                datasource_id = tableau_utils.get_datasource_id(
                    server, project_id, datasource_name
                )
                refreshed_datasource = refresh_datasource(
                    server, datasource_id, datasource_name
                )
                job_id = refreshed_datasource.id
            if workbook_name:
                workbook_id = tableau_utils.get_workbook_id(
                    server, project_id, workbook_name
                )
                refreshed_workbook = refresh_workbook(
                    server, workbook_id, workbook_name
                )
                job_id = refreshed_workbook.id

            if should_check_status:
                try:
                    # `wait_for_job` will automatically check every few seconds
                    # and throw if the job isn't executed successfully
                    logger.info("Waiting for the job to complete...")
                    server.jobs.wait_for_job(job_id)
                    exit_code = tableau_utils.determine_job_status(server, job_id)
                except BaseException:
                    exit_code = tableau_utils.determine_job_status(server, job_id)
                sys.exit(exit_code)
            else:
                artifact.variables.create_pickle("job_id", job_id)
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An unexpected error occurred {e}")
        sys.exit(DataVisualization.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
