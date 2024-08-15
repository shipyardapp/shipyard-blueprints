from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from requests import Request
import os

# Path to your client_secret.json file
CLIENT_SECRET_FILE = "dev/client_secret.json"
SCOPES = ["https://www.googleapis.com/auth/drive"]


def get_credentials():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = service_account.Credentials.from_service_account_file("token.json")
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("dev/token.json", "w") as token:
            token.write(creds.to_json())

    return creds


def main():
    creds = get_credentials()
    print("Successfully updated token")


if __name__ == "__main__":
    main()
