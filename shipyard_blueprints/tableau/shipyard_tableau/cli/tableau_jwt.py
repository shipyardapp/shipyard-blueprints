import argparse
import sys
import requests
import jwt
import datetime
import uuid
import xml.etree.ElementTree as ET


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--client-id", dest="client_id", required=True)
    parser.add_argument("--client-secret", dest="client_secret", required=True)
    parser.add_argument("--secret-value", dest="secret_value", required=True)
    parser.add_argument("--site-id", dest="site_id", required=True)
    parser.add_argument("--server-url", dest="server_url", required=True)
    parser.add_argument("--username", dest="username", required=True)
    parser.add_argument("--workbook-name", dest="workbook_name", required=False)
    parser.add_argument("--datasource-name", dest="datasource_name", required=False)
    parser.add_argument("--project-name", dest="project_name", required=True)
    parser.add_argument(
        "--check-status", dest="check_status", default="TRUE", required=False
    )
    return parser.parse_args()


def login(
    server_url: str,
    username: str,
    client_id: str,
    secret_id: str,
    secret_value: str,
    site_path: str,
):
    print(server_url, username, client_id, secret_id, secret_value, site_path)
    try:
        url = f"{server_url}/api/3.22/auth/signin"
        token = jwt.encode(
            {
                "iss": client_id,
                "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=5),
                "jti": str(uuid.uuid4()),
                "aud": "tableau",
                "sub": username,
                # "scp": ["tableau:views:read", "tableau:workbooks:read", "tableau:datasources:read", "tableau:datasources:run", "tableau:workbooks:run"],
                "scp": [
                    "tableau:views:*",
                    "tableau:workbooks:*",
                    "tableau:datasources:*",
                    "tableau:tasks:*",
                    "tableau:users:*",
                    "tableau:projects*",
                    "tableau:content:read",
                ],
            },
            secret_value,
            algorithm="HS256",
            headers={
                "kid": secret_id,
                "iss": client_id,
            },
        )
        site_path = "shipyarddevelopmentdev664002"
        payload = f"""
                <tsRequest>
                    <credentials jwt="{token}">
                        <site contentUrl="{site_path}" />
                    </credentials>
                </tsRequest>
                """
        headers = {"Content-Type": "application/xml", "Accept": "application/json"}

        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        print("Successfully logged in")
        auth_token = response.json()["credentials"]["token"]
        site_id = response.json()["credentials"]["site"]["id"]
        user_id = response.json()["credentials"]["user"]["id"]
        return auth_token, site_id, user_id
    except Exception as e:
        print(f"Error logging into Tableau: {e}")


def get_workbook_id(
    workbook_name: str,
    auth_token: str,
    site_id: str,
    server_url: str,
    project_name: str,
):
    try:
        workbooks_url = f"{server_url}/api/3.22/sites/{site_id}/workbooks"
        headers = {"X-Tableau-Auth": auth_token, "Accept": "application/json"}
        response = requests.get(workbooks_url, headers=headers)
        response.raise_for_status()
        workbook_data = response.json()["workbooks"]["workbook"]
        projects = list(
            filter(lambda x: x["project"]["name"] == project_name, workbook_data)
        )
        for workbook in projects:
            if workbook["name"] == workbook_name:
                return workbook["id"]
    except Exception as e:
        print(f"Error fetching workbook ID: {e}")


def refresh_workbook(workbook_id: str, auth_token: str, site_id: str, server_url: str):
    try:
        workbook_url = (
            f"{server_url}/api/3.22/sites/{site_id}/workbooks/{workbook_id}/refresh"
        )
        headers = {
            "X-Tableau-Auth": auth_token,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        response = requests.post(workbook_url, headers=headers, json={})
        print(response.text)
        response.raise_for_status()
    except Exception as e:
        print(f"Error refreshing workbook: {e}")


def get_datasource_id(
    datasource_name: str,
    project_name: str,
    auth_token: str,
    site_id: str,
    server_url: str,
):
    try:
        datasource_url = f"{server_url}/api/3.22/sites/{site_id}/datasources"
        headers = {"X-Tableau-Auth": auth_token, "Accept": "application/json"}
        response = requests.get(datasource_url, headers=headers)
        resp_json = response.json()
        datasource_data = resp_json["datasources"]["datasource"]
        projects = list(
            filter(lambda x: x["project"]["name"] == project_name, datasource_data)
        )

        for datasource in projects:
            if datasource["name"] == datasource_name:
                return datasource["id"]
    except Exception as e:
        print(f"Error fetching datasource ID: {e}")


def refresh_datasource(
    datasource_id: str, auth_token: str, site_id: str, server_url: str
):
    try:
        datasource_url = (
            f"{server_url}/api/3.22/sites/{site_id}/datasources/{datasource_id}/refresh"
        )
        headers = {
            "X-Tableau-Auth": auth_token,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        response = requests.post(datasource_url, headers=headers, json={})
        response.raise_for_status()
    except Exception as e:
        print(f"Error refreshing datasource: {e}")


def main():
    try:
        args = get_args()
        username = args.username
        client_id = args.client_id
        secret_id = args.client_secret
        secret_value = args.secret_value
        site_path = args.site_id
        server_url = args.server_url
        workbook_name = args.workbook_name
        datasource_name = args.datasource_name
        project_name = args.project_name

        auth_token, site_id, user_id = login(
            server_url, username, client_id, secret_id, secret_value, site_path
        )
        if workbook_name:
            workbook_id = get_workbook_id(
                workbook_name=workbook_name,
                auth_token=auth_token,
                site_id=site_id,
                server_url=server_url,
                project_name=project_name,
            )
            refresh_workbook(
                workbook_id=workbook_id,
                auth_token=auth_token,
                site_id=site_id,
                server_url=server_url,
            )
        elif datasource_name:
            datasource_id = get_datasource_id(
                datasource_name=datasource_name,
                project_name=project_name,
                auth_token=auth_token,
                site_id=site_id,
                server_url=server_url,
            )
            refresh_datasource(
                datasource_id=datasource_id,
                auth_token=auth_token,
                site_id=site_id,
                server_url=server_url,
            )
        else:
            print("Workbook or Datasource name is required")
            sys.exit(1)
    except Exception as e:
        print(f"Error trying to refresh resource: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
