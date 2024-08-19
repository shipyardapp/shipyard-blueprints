import os

import msal
from shipyard_templates import DataVisualization
from shipyard_templates.exit_code_exception import ExitCodeException
from shipyard_templates.shipyard_logger import ShipyardLogger

logger = ShipyardLogger.get_logger()


def generate_access_token_from_client(client_id, tenant_id, client_secret):
    return msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
    ).acquire_token_for_client(
        scopes=["https://analysis.windows.net/powerbi/api/.default"]
    )


def generate_token_from_un_pw(client_id, tenant_id, username, password):
    return msal.PublicClientApplication(
        client_id=client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
    ).acquire_token_by_username_password(
        username=username,
        password=password,
        scopes=["https://analysis.windows.net/powerbi/api/.default"],
    )


def validate_refresh_object_type(object_type):
    object_type = object_type.lower().strip()
    if object_type not in ["dataset", "dataflow"]:
        raise ValueError
    return object_type


def generate_access_token(bi_instance):
    logger.debug("Generating access token")

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


def validate_args(args):
    """
    Handle datatype conversions and validations for arguments

    @param args: Namespace object containing arguments
    @return: Tuple containing wait_for_completion and wait_time
    """

    if args.poke_interval is None or args.poke_interval == "":
        wait_time = 1
    else:
        wait_time = int(args.poke_interval) * 60

    if wait_time < 0:
        raise ExitCodeException(
            "Error: poke-interval must be greater than 0",
            DataVisualization.EXIT_CODE_INVALID_INPUT,
        )
    elif wait_time > 60:
        raise ExitCodeException(
            "Error: poke-interval must be less than or equal to 60",
            DataVisualization.EXIT_CODE_INVALID_INPUT,
        )

    wait_for_completion = None
    if type(args.wait_for_completion) is str:
        if args.wait_for_completion.upper() == "TRUE":
            wait_for_completion = True
        elif args.wait_for_completion.upper() == "FALSE":
            wait_for_completion = False

    elif type(args.wait_for_completion) is bool:
        wait_for_completion = args.wait_for_completion

    return wait_for_completion, wait_time


def get_credential_group(args):
    if access_token := os.getenv("OAUTH_ACCESS_TOKEN"):
        logger.debug("Using access token for authentication")
        return {"access_token": access_token}

    if args.client_id and args.client_secret and args.tenant_id:
        logger.debug("Using Client ID, Client Secret, and Tenant for authentication")
        return {
            "client_id": args.client_id,
            "client_secret": args.client_secret,
            "tenant_id": args.tenant_id,
        }
    # if args.client_id and args.tenant and args.username and args.password:
    #     logger.debug("Using Client ID, Tenant, Username, and Password for authentication")
    #     return {
    #         "client_id": args.client_id,
    #         "tenant": args.tenant,
    #         "username": args.username,
    #         "password": args.password
    #     }
    #
    raise ExitCodeException(
        "Either access token or Client ID, Client Secret, and Tenant must be provided",
        DataVisualization.EXIT_CODE_INVALID_CREDENTIALS,
    )
