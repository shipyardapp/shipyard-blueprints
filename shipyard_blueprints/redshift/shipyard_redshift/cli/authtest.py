import os
import sys
from shipyard_redshift import RedshiftClient


def get_args():
    args = {
        "host": os.environ.get("REDSHIFT_HOST"),
        "user": os.environ.get("REDSHIFT_USERNAME"),
    }
    try:
        args["port"] = os.environ.get("REDSHIFT_PORT")
    except Exception as e:
        args["port"] = "5432"
    args["password"] = os.environ.get("REDSHIFT_PASSWORD")
    args["database"] = os.environ.get("REDSHIFT_DATABASE")
    return args


def main():
    args = get_args()
    host = args["host"]
    user = args["user"]
    pwd = args["password"]
    port = args["port"]
    database = args["database"]
    redshift = RedshiftClient(
        user=user, pwd=pwd, host=host, port=port, database=database
    )
    try:
        con = redshift.connect()
        redshift.logger.info("Connected to Redshift")
        sys.exit(0)
    except Exception as e:
        redshift.logger.error("Could not connect to Redshift with given credentials")
        sys.exit(1)


if __name__ == "__main__":
    main()
