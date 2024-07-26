import time
from json import JSONDecodeError

import msal
from shipyard_templates.exit_code_exception import ExitCodeException
from shipyard_templates.shipyard_logger import ShipyardLogger

logger = ShipyardLogger.get_logger()

FAILED_JOB_STATUSES = {"Failed", "PartialComplete"}
ONGOING_JOB_STATUSES = {"Queued", "InProgress", "Unknown"}
COMPLETE_JOB_STATUSES = {"Completed", "Success"}


def generate_access_token_from_client(bi_instance):
    return msal.ConfidentialClientApplication(
        client_id=bi_instance.client_id,
        client_credential=bi_instance.client_secret,
        authority=f"https://login.microsoftonline.com/{bi_instance.tenant_id}",
    ).acquire_token_for_client(
        scopes=["https://analysis.windows.net/powerbi/api/.default"]
    )


def generate_access_token_from_un_pw(bi_instance):
    return msal.PublicClientApplication(
        client_id=bi_instance.client_id,
        authority=f"https://login.microsoftonline.com/{bi_instance.tenant_id}",
    ).acquire_token_by_username_password(
        username=bi_instance.username,
        password=bi_instance.password,
        scopes=["https://analysis.windows.net/powerbi/api/.default"],
    )


def validate_refresh_object_type(bi_instance, object_type):
    object_type = object_type.lower().strip()
    if object_type not in ["dataset", "dataflow"]:
        raise ValueError
    return object_type


def generate_access_token(bi_instance):
    logger.info("Generating access token")
    try:
        token_data = generate_access_token_from_client(bi_instance)
    except ValueError as e:
        raise ExitCodeException(
            f"Error{e}", bi_instance.EXIT_CODE_INVALID_CREDENTIALS
        ) from e

    except Exception as e:
        raise ExitCodeException(
            f"An unexpected error occurred while generating access token. Error{e}",
            bi_instance.EXIT_CODE_UNKNOWN_ERROR,
        ) from e
    else:
        if token := token_data.get("access_token"):
            logger.info("Access token generated successfully")
            return token
        if "error" in token_data:
            error_type = token_data.get("error")
            error_description = token_data.get("error_description")
            if error_type in {"invalid_grant", "invalid_client"}:
                raise ExitCodeException(
                    error_description, bi_instance.EXIT_CODE_INVALID_INPUT
                )
            else:
                raise ExitCodeException(
                    f"Error TYPE: {error_type}, Error: {error_description}",
                    bi_instance.EXIT_CODE_UNKNOWN_ERROR,
                )


def handle_error_response(bi_instance, response):
    try:
        logger.debug(response.text)
        response_details = response.json()
        error_info = response_details.get("error", {})

    except JSONDecodeError:
        logger.warning("Response was not JSON...handling as text")
        error_info = {"message": response.text} if response.text else {}

    if error_info.get("code") == "DailyDataflowRefreshLimitExceeded":
        raise ExitCodeException(
            error_info.get(
                "message",
                f"<Daily Dataflow Refresh Limit Exceeded> \n {response.text}",
            ),
            bi_instance.EXIT_CODE_RATE_LIMIT,
        )
    if response.status_code == 401:
        raise ExitCodeException(
            error_info.get("message", f"<Unauthorized> \n {response.text}"),
            bi_instance.ERROR_CODE_INVALID_CREDENTIALS,
        )
    elif response.status_code == 403:
        raise ExitCodeException(
            error_info.get("message", f"<Forbidden> \n {response.text}"),
            bi_instance.ERROR_CODE_INVALID_CREDENTIALS,
        )
    elif response.status_code == 404:
        raise ExitCodeException(
            error_info.get("message", f"<Not Found\n {response.text}"),
            bi_instance.EXIT_CODE_INVALID_INPUT,
        )
    elif response.status_code == 400:
        raise ExitCodeException(
            error_info.get("message", f"<Bad Request\n {response.text}"),
            bi_instance.EXIT_CODE_BAD_REQUEST,
        )
    elif response.status_code == 405:
        raise ExitCodeException(
            error_info.get("message", f"<Method Not Allowed\n {response.text}"),
            bi_instance.EXIT_CODE_BAD_REQUEST,
        )
    elif response.status_code == 429:
        raise ExitCodeException(
            error_info.get("message", f"<Too Many Requests\n {response.text}"),
            bi_instance.EXIT_CODE_RATE_LIMIT,
        )
    else:
        raise ExitCodeException(
            f"Unknown Error<{response.status_code}>: {response.text}",
            bi_instance.EXIT_CODE_UNKNOWN_ERROR,
        )


def wait_for_dataset_refresh_completion(
        bi_instance, group_id, dataset_id, request_id, wait_time=60
):
    logger.info("Waiting for refresh to complete")
    job_status = "Unknown"

    while job_status in ONGOING_JOB_STATUSES:
        job_status = bi_instance.check_recent_dataset_refresh_by_request_id(
            group_id, dataset_id, request_id
        ).get("status")

        if job_status in COMPLETE_JOB_STATUSES:
            logger.info(f"Job completed with status {job_status}")
            logger.info("Refresh completed")
            return

        if job_status in FAILED_JOB_STATUSES:
            raise ExitCodeException(
                f"Dataset refresh failed with status {job_status}",
                bi_instance.EXIT_CODE_FAILED_REFRESH_JOB,
            )

        elif job_status in ONGOING_JOB_STATUSES:
            logger.info(
                f"Job currently in {job_status}. Waiting {wait_time} seconds before checking again."
            )
            time.sleep(wait_time)
        else:
            raise ExitCodeException(
                f"Unknown job status {job_status}",
                bi_instance.EXIT_CODE_UNKNOWN_REFRESH_JOB_STATUS,
            )


def wait_for_dataflow_refresh_completion(
        bi_instance, group_id, dataflow_id, wait_time=60
):
    logger.info("Waiting for refresh to complete")
    job_status = "Unknown"

    while job_status in ONGOING_JOB_STATUSES:
        transactions = bi_instance.get_dataflow_transactions(group_id, dataflow_id)
        sorted_transactions = sorted(
            transactions["value"], key=lambda x: x["startTime"], reverse=True
        )

        if len(sorted_transactions) == 0:
            raise ExitCodeException(
                f"No transactions found for dataflow {dataflow_id}",
                bi_instance.EXIT_CODE_INVALID_INPUT,
            )

        job_status = sorted_transactions[0].get(
            "status"
        )  # Get the latest transaction status

        if job_status in COMPLETE_JOB_STATUSES:
            logger.info(f"Job completed with status {job_status}")
            logger.info("Refresh completed")
            return

        if job_status in FAILED_JOB_STATUSES:
            raise ExitCodeException(
                f"Dataflow refresh failed with status {job_status}",
                bi_instance.EXIT_CODE_FAILED_REFRESH_JOB,
            )

        elif job_status in ONGOING_JOB_STATUSES:
            logger.info(
                f"Job currently in {job_status}. Waiting {wait_time} seconds before checking again."
            )
            time.sleep(wait_time)
        else:
            raise ExitCodeException(
                f"Unknown job status {job_status}",
                bi_instance.EXIT_CODE_UNKNOWN_REFRESH_JOB_STATUS,
            )
