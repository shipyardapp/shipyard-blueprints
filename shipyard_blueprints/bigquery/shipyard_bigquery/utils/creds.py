import os


def get_credentials():
    if access_token := os.environ.get("OAUTH_ACCESS_TOKEN"):
        return {"access_token": access_token}
    elif service_account := os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return {"service_account": service_account}
    else:
        raise ValueError("Either service account or access token must be provided")
