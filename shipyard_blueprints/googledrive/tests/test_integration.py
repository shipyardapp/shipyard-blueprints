import os
import pytest
from shipyard_googledrive import GoogleDriveClient
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

CREDS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if env_exists := os.path.exists(".env"):
    load_dotenv()


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_upload():
    gc = GoogleDriveClient(service_account=CREDS)

    status = gc.upload(
        file_path="sample.csv",
        drive_file_name="new.csv",
        drive="Blueprint Shared Drive",
    )

    print(f"Done and the id is {status}")


@pytest.mark.skipif(not env_exists, reason="No .env file found")
def test_download():
    gc = GoogleDriveClient(
        service_account=CREDS, shared_drive_name="Blueprint Shared Drive"
    )
    file_name = "sample.csv"
    dest_folder = "downloaded"
    print(f"Attempting to download {file_name} from {gc.shared_drive_name}")
    gc.download(
        drive_file_name=file_name,
        drive="0AJOXmPCbxECLUk9PVA",
        destination_path=dest_folder,
        destination_file_name="downloaded_file.csv",
    )
    print("Successfully downloaded")
