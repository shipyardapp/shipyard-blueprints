import argparse
import sys
from shipyard_tableau import TableauClient
from shipyard_templates import ShipyardLogger, ExitCodeException
from shipyard_bp_utils.args import convert_to_boolean
from shipyard_templates import DataVisualization

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--username", dest="username", required=False
    )  # for username_password
    parser.add_argument(
        "--password", dest="password", required=False
    )  # for username_password
    parser.add_argument(
        "--access-token-name", dest="access_token_name", required=False
    )  # for access_token
    parser.add_argument(
        "--access-token-value", dest="access_token_value", required=False
    )  # for access_token
    parser.add_argument("--site-id", dest="site_id", required=True)
    parser.add_argument("--server-url", dest="server_url", required=True)
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
    parser.add_argument("--workbook-name", dest="workbook_name", required=True)
    # parser.add_argument("--datasource-name", dest="datasource_name", required=False)
    parser.add_argument("--project-name", dest="project_name", required=True)
    parser.add_argument(
        "--check-status", dest="check_status", default="TRUE", required=False
    )
    return parser.parse_args()


def main():
    try:
        args = get_args()
        sign_in_method = args.sign_in_method
        workbook_name = args.workbook_name
        project_name = args.project_name
        check_status = convert_to_boolean(args.check_status)
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

        workbook_id = client.get_workbook_id(
            workbook_name=workbook_name, project_name=project_name
        )
        data = client.refresh_workbook(workbook_id)
        logger.info(f"Data from api: {data}")

        if check_status:
            job_id = data["job"]["id"]
        else:
            logger.info(f"Successfully refreshed {workbook_name}")

    except ExitCodeException as ec:
        logger.error(ec.message)
        sys.exit(ec.exit_code)

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(DataVisualization.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
