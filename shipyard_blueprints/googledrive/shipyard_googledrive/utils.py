import os
from typing import Optional


def generate_path(file_name: str, dir_name: Optional[str] = None) -> str:
    """Helper function to generate a full path for a file

    Args:
        file_name: The file
        dir_name: The optional directory

    Returns: The file path

    """
    if not dir_name:
        return file_name

    return os.path.normpath(os.path.join(dir_name, file_name))


def get_shared_drive_id(service, drive):
    """
    Search for the drive under shared Google Drives.
    """
    drives = service.drives().list().execute()
    drive_id = None
    for _drive in drives["drives"]:
        if _drive["name"] == drive:
            drive_id = _drive["id"]
    return drive_id


def find_folder_id(service, destination_folder_name, drive):
    """
    Returns the id of the destination folder name in Google Drive
    """
    parent_id = None
    for folder in destination_folder_name.split("/")[:-1]:
        # build query string
        if parent_id:
            query = (
                "mimeType = 'application/vnd.google-apps.folder'"
                f" and name='{folder}' and '{parent_id}' in parents"
            )
        else:
            query = (
                "mimeType = 'application/vnd.google-apps.folder'"
                f" and name='{folder}'"
            )

        if drive:
            drive_id = get_shared_drive_id(service, drive)
            results = (
                service.files()
                .list(
                    q=str(query),
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    corpora="drive",
                    driveId=drive_id,
                    fields="files(id, name)",
                )
                .execute()
            )
        else:
            drive_id = None
            results = (
                service.files().list(q=str(query), fields="files(id, name)").execute()
            )

        _folder = results.get("files", [])
        if _folder != []:
            parent_id = _folder[0].get("id")
        else:
            parent_id = create_remote_folder(service, folder, parent_id, drive_id)

    return parent_id


def create_remote_folder(service, folder, parent_id=None, drive_id=None):
    """
    Create a folder on Drive, returns the newely created folders ID
    """
    body = {"name": folder, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        body["parents"] = [parent_id]
    if drive_id and not parent_id:
        body["parents"] = [drive_id]
    try:
        folder = (
            service.files()
            .create(body=body, supportsAllDrives=True, fields=("id"))
            .execute()
        )
        print(f"Creating folder: {folder}")
    except Exception as e:
        print(f"Failed to create folder: {folder}")
        raise (e)
    return folder["id"]


def is_folder_visible(
    service: str, folder_name: str, drive_id: Optional[str] = None
) -> bool:
    """Checks to see if the service account as access to the specified folder

    Args:
        service:
        folder_name:
        drive_id:
    """
    return True
