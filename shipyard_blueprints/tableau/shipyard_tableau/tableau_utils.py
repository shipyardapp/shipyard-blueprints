import tableauserverclient as TSC
from shipyard_templates import ShipyardLogger, ExitCodeException, DataVisualization

EXIT_CODE_FILE_WRITE_ERROR = 100

EXIT_CODE_INVALID_PROJECT = 201
EXIT_CODE_INVALID_WORKBOOK = 202
EXIT_CODE_INVALID_VIEW = 203
EXIT_CODE_INVALID_JOB = 204
EXIT_CODE_INVALID_DATASOURCE = 205
EXIT_CODE_REFRESH_ERROR = 206

EXIT_CODE_FINAL_STATUS_CANCELLED = 210
EXIT_CODE_FINAL_STATUS_ERRORED = 211
EXIT_CODE_STATUS_INCOMPLETE = 212

logger = ShipyardLogger.get_logger()


def connect_to_tableau(
    username: str, password: str, site_id: str, server_url: str, sign_in_method: str
):
    """
    Helper function to establish connection and server

    Args:
        username (str): Tableau username
        password (str): Tableau password
        site_id (str): Tableau site_id
        server_url (str): Tableau server_url
        sign_in_method (str): Tableau sign_in_method
    Returns:
        server: Tableau server
        connection: Tableau connection
    """
    try:
        if site_id.lower() == "default":
            site_id = ""

        if sign_in_method == "access_token":
            tableau_auth = TSC.PersonalAccessTokenAuth(
                token_name=username, personal_access_token=password, site_id=site_id
            )

        elif sign_in_method == "username_password":
            tableau_auth = TSC.TableauAuth(username, password, site_id=site_id)

        server = TSC.Server(
            server_url,
            use_server_version=True,
        )
        connection = server.auth.sign_in(tableau_auth)
        logger.info("Successfully authenticated with Tableau.")
        return server, connection
    except Exception as e:
        logger.error("Failed to connect to Tableau.")
        if sign_in_method == "username_password":
            raise ExitCodeException(
                "Invalid username or password. Please check for typos and try again.",
                DataVisualization.EXIT_CODE_INVALID_CREDENTIALS,
            ) from e

        if sign_in_method == "access_token":
            raise ExitCodeException(
                "Invalid token name or access token. Please check for typos and try again.",
                DataVisualization.EXIT_CODE_INVALID_CREDENTIALS,
            ) from e


def get_project_id(server, project_name):
    """
    Looks up and returns the project_id of the project_name that was specified.
    """
    try:
        req_option = TSC.RequestOptions()
        req_option.filter.add(
            TSC.Filter(
                TSC.RequestOptions.Field.Name,
                TSC.RequestOptions.Operator.Equals,
                project_name,
            )
        )

        project_matches = server.projects.get(req_options=req_option)
        if len(project_matches[0]) == 1:
            return project_matches[0][0].id
        else:
            raise ExitCodeException(
                f"{project_name} could not be found. Please check for typos and ensure that the name you provide "
                f"matches"
                f"exactly (case sensitive)",
                EXIT_CODE_INVALID_PROJECT,
            )
    except Exception as e:
        raise ExitCodeException(
            f"An error occurred while trying to get the project_id for {project_name}. {e}",
            EXIT_CODE_INVALID_PROJECT,
        ) from e


def get_datasource_id(server, project_id, datasource_name):
    """
    Looks up and returns the datasource_id of the datasource_name that was specified, filtered by project_id matches.
    """
    try:
        req_option = TSC.RequestOptions()
        req_option.filter.add(
            TSC.Filter(
                TSC.RequestOptions.Field.Name,
                TSC.RequestOptions.Operator.Equals,
                datasource_name,
            )
        )

        datasource_matches = server.datasources.get(req_options=req_option)

        # We can't filter by project_id or project_name in the initial request,
        # so we have to find all name matches and look for a project_id match.
        datasource_id = None
        for datasource in datasource_matches[0]:
            if datasource.project_id == project_id:
                datasource_id = datasource.id
        if datasource_id is None:
            raise ExitCodeException(
                f"{datasource_name} could not be found that lives in the project you specified. Please check for typos "
                f"and ensure that the name(s) you provide match exactly (case sensitive)",
                EXIT_CODE_INVALID_DATASOURCE,
            )
        return datasource_id
    except Exception as e:
        raise ExitCodeException(
            f"An error occurred while trying to get the datasource_id for {datasource_name}. {e}",
            EXIT_CODE_INVALID_DATASOURCE,
        ) from e


def get_workbook_id(server, project_id, workbook_name):
    """
    Looks up and returns the workbook_id of the workbook_name that was specified, filtered by project_id matches.
    """
    try:
        req_option = TSC.RequestOptions()
        req_option.filter.add(
            TSC.Filter(
                TSC.RequestOptions.Field.Name,
                TSC.RequestOptions.Operator.Equals,
                workbook_name,
            )
        )

        workbook_matches = server.workbooks.get(req_options=req_option)
        # We can't filter by project_id in the initial request,
        # so we have to find all name matches and look for a project_id match.
        workbook_id = None
        for workbook in workbook_matches[0]:
            if workbook.project_id == project_id:
                workbook_id = workbook.id
        if workbook_id is None:
            raise ExitCodeException(
                f"{workbook_name} could not be found in the project you specified. Please check for typos and ensure that "
                f"the name(s) you provide match exactly (case sensitive)",
                EXIT_CODE_INVALID_WORKBOOK,
            )
        return workbook_id
    except Exception as e:
        raise ExitCodeException(
            f"An error occurred while trying to get the workbook_id for {workbook_name}. {e}",
            EXIT_CODE_INVALID_WORKBOOK,
        ) from e


def get_view_id(server, project_id, workbook_id, view_name):
    """
    Looks up and returns the view_id of the view_name that was specified, filtered by project_id AND workbook_id matches.
    """
    try:
        req_option = TSC.RequestOptions()
        req_option.filter.add(
            TSC.Filter(
                TSC.RequestOptions.Field.Name,
                TSC.RequestOptions.Operator.Equals,
                view_name,
            )
        )

        view_matches = server.views.get(req_options=req_option)
        # We can't filter by project_id or workbook_id in the initial request,
        # so we have to find all name matches and look for those matches.
        view_id = None
        for view in view_matches[0]:
            if view.project_id == project_id and view.workbook_id == workbook_id:
                view_id = view.id
        if view_id is None:
            raise ExitCodeException(
                f"{view_name} could not be found that lives in the project and workbook you specified. Please check for "
                f"typos and ensure that the name(s) you provide match exactly (case sensitive)",
                EXIT_CODE_INVALID_VIEW,
            )
        return view_id
    except Exception as e:
        raise ExitCodeException(
            f"An error occurred while trying to get the view_id for {view_name}. {e}",
            EXIT_CODE_INVALID_VIEW,
        ) from e


def get_job_info(server, job_id):
    """
    Gets information about the specified job_id.
    """
    try:
        return server.jobs.get_by_id(job_id)
    except Exception as e:
        raise ExitCodeException(
            f"Job {job_id} was not found. {e}",
            EXIT_CODE_INVALID_JOB,
        ) from e


def determine_job_status(server, job_id):
    """
    Job status response handler.

    The finishCode indicates the status of the job: -1 for incomplete, 0 for success, 1 for error, or 2 for cancelled.
    """
    job_info = get_job_info(server, job_id)
    if job_info.finish_code == -1:
        if job_info.started_at is None:
            logger.info(f"Tableau reports that the job {job_id} has not yet started.")
        else:
            logger.info(f"Tableau reports that the job {job_id} is not yet complete.")
        return EXIT_CODE_STATUS_INCOMPLETE
    elif job_info.finish_code == 0:
        logger.info(f"Tableau reports that job {job_id} was successful.")
        return 0
    elif job_info.finish_code == 1:
        logger.info(f"Tableau reports that job {job_id} erred.")
        return EXIT_CODE_FINAL_STATUS_ERRORED
    elif job_info.finish_code == 2:
        logger.info(f"Tableau reports that job {job_id} was cancelled.")
        return EXIT_CODE_FINAL_STATUS_CANCELLED
    else:
        logger.info(f"Something went wrong when fetching status for job {job_id}")
        return DataVisualization.EXIT_CODE_UNKNOWN_ERROR
