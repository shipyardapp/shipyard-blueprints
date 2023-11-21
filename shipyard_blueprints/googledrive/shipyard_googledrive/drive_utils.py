import os
from typing import Optional, Union, List, Any
import logging

from shipyard_templates import ExitCodeException


def is_folder_shared(
    logger: logging.Logger, service_account_email: str, folder_id: str, drive_service
) -> bool:
    """Helper function to see if a provided folder is shared with the service account

    Args:
        drive_service (): The service connection
        service_account_email: The email of the service account
        folder_id: The ID of the folder in Google Drive

    Returns: True if folder is shared, False if not
    """
    try:
        permissions = drive_service.permissions().list(fileId=folder_id).execute()
        for permission in permissions.get("permissions", []):
            if (
                permission["type"] == "user"
                and permission["emailAddress"] == service_account_email
            ):
                return True

    except Exception as e:
        logger.warning(
            f"An exception was found during this call most likely indicating that no folder ID exists, returning False. Exception message: {str(e)}"
        )
        return False

    else:
        logger.warning("Folder ID is not shared with service account")
        return False


def does_file_exist(
    logger: logging.Logger,
    parent_folder_id: str,
    file_name: str,
    service,
    drive_id: Optional[str] = None,
) -> bool:
    """Helper function to see if the file already exists in the accessible Google Drive

    Args:
        service (): The drive connection
        parent_folder_id: The ID of the parent folder
        file_name: The name of the file
        drive_id: The optional ID of the shared drive

    Returns: True if exists, False if not

    """
    query = f"name='{file_name}' and '{parent_folder_id}' in parents"
    try:
        if drive_id:
            results = (
                service.files()
                .list(
                    q=query,
                    includeItemsFromAllDrives=True,
                    corpora="drive",
                    driveId=drive_id,
                    supportsAllDrives=True,
                )
                .execute()
            )
        else:
            results = service.files().list(q=query).execute()
        if results.get("files", []) != []:
            return True
    except Exception as e:
        # this means that the file was not found
        logger.warning(
            f"An exception was thrown and now file was found, returning False: {str(e)}"
        )
        return False
    else:
        logger.warning("No file was found, returning false")
        return False


def get_file_id(
    file_name: str,
    service,
    drive_id: Optional[str] = None,
    folder_id: Optional[str] = None,
) -> Union[str, None]:
    """Helper function to retrieve the file id in Google Drive

    Args:
        service (): The Google Drive service connection
        file_name: The name of the file to lookup in Google Drive
        drive_id: The Optional ID of the drive
        folder_id: The optional ID of the folder. This is only necessary if the file resides in a folder

    Raises:
        ExitCodeException:

    Returns: The ID of the file if exists, otherwise None

    """
    query = f"name='{file_name}'"
    if folder_id:
        query += f"and '{folder_id} in parents"
    try:
        if drive_id:
            results = (
                service.files()
                .list(
                    q=query,
                    includeItemsFromAllDrives=True,
                    corpora="drive",
                    driveId=drive_id,
                    supportsAllDrives=True,
                )
                .execute()
            )
        else:
            results = service.files().list(q=query).execute()

    except Exception as e:
        raise ExitCodeException(f"Error in fetching file id: {str(e)}", exit_code=203)

    else:
        if "files" in results and len(results["files"]) > 0:
            return results["files"][0]["id"]
        return None


def create_remote_folder(
    folder_name: str,
    service,
    parent_id: Optional[str] = None,
    drive_id: Optional[str] = None,
) -> str:
    """Helper function to create a folder in Google Drive

    Args:
        service (): The google service connection
        folder_name: The name of the folder to create
        parent_id: The optional folder to place the newly created folder within
        drive_id: The optional drive to create the folder in

    Raises:
        ExitCodeException:

    Returns: The ID of the newly created folder

    """
    body = {"name": folder_name, "mimeType": "application/vnd.google-apps.folder"}
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
    except Exception as e:
        raise ExitCodeException(
            f"Failed to create folder {folder_name} in Goolge Drive", 208
        )
    return folder["id"]


def get_folder_id(
    service,
    folder_identifier: Optional[str] = None,
) -> Union[str, None]:
    """Helper function to grab the folder ID when provided either the name of the folder or the ID (preferred). This is instituted for backwards compatibility in the Shipyard blueprint

    Args:
        service (): The Google Drive service connection
        folder_identifier: The name of the folder or the ID from the URL

    Returns: The folder ID or None if nonexistent

    """
    if not folder_identifier:
        return None
    try:
        # every folder ID starts with 1 and is 33 chars long
        if is_folder_id(folder_identifier):
            return folder_identifier
        else:
            results = (
                service.files()
                .list(
                    q=f"name = '{folder_identifier} and mimeType = 'application/vnd.google-apps.folder'",
                    fields="files(id)",
                )
                .execute()
            )
            folders = results.get("files", [])
            if folders:
                return folders[0]["id"]
            else:
                return None
    except Exception:
        return None


def get_drive_id(drive_id: str, service) -> Union[str, None]:
    """Helper function to grab the drive ID when either the name of the drive or the ID is provided. This is instituted for backwards compatibility in the Shipyard blueprint

    Args:
        service (): The Google Drive service connection
        drive_id:  The name of the drive or the ID from the URL

    Returns: The ID of the drive or None if not found

    """
    try:
        if len(drive_id) == 33 and str(drive_id).startswith("OAJ"):
            return drive_id
        else:
            results = (
                service.drives()
                .list(q=f"name = '{drive_id}'", fields="drives(id)")
                .execute()
            )
            drives = results.get("drives", [])
            if drives:
                return drives[0]["id"]
            else:
                return None
    except Exception:
        return None


def is_folder_id(folder_identifier: str) -> bool:
    """Helper function to determine if the input is a legitimate folder ID or a folder name

    Args:
        folder_identifier: Either the folder name or the ID from the URL

    Returns: True if the format matches that of a folder ID, false otherwise

    """
    if len(folder_identifier) == 33 and str(folder_identifier).startswith("1"):
        return True
    return False


def get_file_matches(
    service,
    pattern: str,
    folder_id: Optional[str] = None,
    drive_id: Optional[str] = None,
) -> List[Any]:
    """Helper function to return all the files that match a particular pattern

    Args:
        service (): The google service connection
        pattern: The pattern to search for
        folder_id: The folder to search within. If omitted, all file matches across all folders will be returned

    Raises:
        ExitCodeException:

    Returns: The list of the matches

    """
    folder_id = get_folder_id(folder_id, service=service)
    try:
        query = f"name contains '{pattern}'"
        if folder_id:
            query += f" and '{folder_id}' in parents"
        results = service.files().list(q=query).execute()
        files = results.get("files", [])
    except Exception as e:
        raise ExitCodeException(f"Error in finding matching files: {str(e)}", 210)

    else:
        return results.get("files", [])
