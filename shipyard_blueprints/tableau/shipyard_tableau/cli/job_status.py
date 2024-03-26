import argparse
import sys

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
    parser.add_argument("--job-id", dest="job_id", required=False)
    return parser.parse_args()


def main():
    try:
        args = get_args()
        username = args.username
        password = args.password
        site_id = args.site_id
        server_url = args.server_url
        sign_in_method = args.sign_in_method
        artifact = Artifact("tableau")
        job_id = args.job_id or artifact.variables.read_pickle("job_id")

        server, connection = tableau_utils.connect_to_tableau(
            username, password, site_id, server_url, sign_in_method
        )

        with connection:
            sys.exit(tableau_utils.determine_job_status(server, job_id))
    except ExitCodeException as e:
        logger.error(e)
        sys.exit(e.exit_code)
    except Exception as e:
        logger.error(f"An unexpected error occurred {e}")
        sys.exit(DataVisualization.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
