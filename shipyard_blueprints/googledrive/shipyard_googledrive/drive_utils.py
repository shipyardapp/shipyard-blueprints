import os
import logging
import re
import glob
from typing import Optional, Union, List, Any, Set
from pathlib import Path

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
    # query = f"name='{file_name}' and '{parent_folder_id}' in parents"
    query = f"'{parent_folder_id}' in parents and name='{file_name}'"
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
        query += f"and '{folder_id}' in parents"
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
    drive_id: Optional[str] = None,
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
            if not drive_id:
                results = service.files().list()
            else:
                results = (
                    service.files()
                    .list(
                        q=f"name = '{folder_identifier}' and mimeType = 'application/vnd.google-apps.folder'",
                        fields="files(id)",
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                        corpora="drive",
                        driveId=drive_id,
                    )
                    .execute()
                )
            folders = results.get("files", [])
            if len(folders) > 1:
                raise ExitCodeException(
                    f"Multiple folders with name {folder_identifier} found, please use the folder ID instead",
                    204,
                )

            if folders:
                return folders[0]["id"]
            else:
                return None

    except ExitCodeException as ec:
        raise ExitCodeException(ec.message, ec.exit_code)
    except Exception as e:
        return None


def get_drive_id(drive_id: str, service) -> Union[str, None]:
    """Helper function to grab the drive ID when either the name of the drive or the ID is provided. This is instituted for backwards compatibility in the Shipyard blueprint

    Args:
        service (): The Google Drive service connection
        drive_id:  The name of the drive or the ID from the URL

    Returns: The ID of the drive or None if not found

    """
    try:
        if len(drive_id) == 19 and str(drive_id).startswith("0A"):
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
        drive_id: The shared drive to search within

    Raises:
        ExitCodeException:

    Returns: The list of the matches

    """
    try:
        query = None
        if folder_id:
            query = f"'{folder_id}' in parents"
            if drive_id:
                if query:
                    results = (
                        service.files()
                        .list(
                            q=query,
                            supportsAllDrives=True,
                            includeItemsFromAllDrives=True,
                            corpora="drive",
                            driveId=drive_id,
                        )
                        .execute()
                    )
                else:
                    results = (
                        service.files()
                        .list(
                            supportsAllDrives=True,
                            includeItemsFromAllDrives=True,
                            corpora="drive",
                            driveId=drive_id,
                        )
                        .execute()
                    )
            else:
                if query:
                    results = service.files().list(q=query).execute()
                else:
                    results = service.files().list().execute()

            files = results.get("files", [])
        else:
            files = []
            all_folder_ids = get_all_folder_ids(service, drive_id=drive_id)
            for f_id in all_folder_ids:
                query = f"'{f_id}' in parents"
                if drive_id:
                    results = (
                        service.files()
                        .list(
                            q=query,
                            supportsAllDrives=True,
                            includeItemsFromAllDrives=True,
                            corpora="drive",
                            driveId=drive_id,
                        )
                        .execute()
                    )
                else:
                    results = service.files().list(q=query).execute()

                files.extend(results.get("files", []))

            # grab the files in the root
            if drive_id:
                root_results = (
                    service.files()
                    .list(
                        q="trashed=false and mimeType!='application/vnd.google-apps.folder'",
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                        corpora="drive",
                        driveId=drive_id,
                    )
                    .execute()
                )
            else:
                root_results = (
                    service.files()
                    .list(
                        q="trashed=false and mimeType!='application/vnd.google-apps.folder'"
                    )
                    .execute()
                )

            files.extend(root_results.get("files", []))

        matches = []
        id_set = set()
        for f in files:
            if re.search(pattern, f["name"]) and f["id"] not in id_set:
                matches.append(f)
                id_set.add(f["id"])
    except Exception as e:
        raise ExitCodeException(f"Error in finding matching files: {str(e)}", 210)

    else:
        return matches


def get_all_folder_ids(service, drive_id: Optional[str] = None) -> List[Any]:
    # Set the query to retrieve all folders
    query = "mimeType='application/vnd.google-apps.folder' and trashed=false"

    # Execute the query to get the list of folders
    if drive_id:
        results = (
            service.files()
            .list(
                q=query,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                corpora="drive",
                driveId=drive_id,
            )
            .execute()
        )
    else:
        results = service.files().list(q=query).execute()

    folders = results.get("files", [])

    # Extract and return the folder IDs
    folder_ids = [folder["id"] for folder in folders]
    # folder_ids.append('root') # add so that the files not within a folder will be returned as well
    return folder_ids


def list_local_files(directory: Optional[str] = None) -> List[str]:
    """Returns all the files located within the directory path. If the dirname is not provided, then the current working directory will be used and it will span all subdirectories

    Args:
        dirname: The optional directory to span

    Returns: List of all the files

    """
    if directory is None:
        directory = os.getcwd()

    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            files.append(file_path)

    return files
