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


def determine_destination_full_path(
    destination_folder_name, destination_file_name, source_full_path, file_number=None
):
    """
    Determine the final destination name of the file being uploaded.
    """
    destination_file_name = determine_destination_file_name(
        destination_file_name=destination_file_name,
        source_full_path=source_full_path,
        file_number=file_number,
    )
    destination_full_path = combine_folder_and_file_name(
        destination_folder_name, destination_file_name
    )
    return destination_full_path


def extract_file_name_from_source_full_path(source_full_path):
    """
    Use the file name provided in the source_full_path variable. Should be run
    only if a destination_file_name is not provided.
    """
    destination_file_name = os.path.basename(source_full_path)
    return destination_file_name


def enumerate_destination_file_name(destination_file_name, file_number=1):
    """
    Append a number to the end of the provided destination file name.
    Only used when multiple files are matched to, preventing the destination
    file from being continuously overwritten.
    """
    if re.search(r"\.", destination_file_name):
        destination_file_name = re.sub(
            r"\.", f"_{file_number}.", destination_file_name, 1
        )
    else:
        destination_file_name = f"{destination_file_name}_{file_number}"
    return destination_file_name


def determine_destination_file_name(
    *, source_full_path, destination_file_name, file_number=None
):
    """
    Determine if the destination_file_name was provided, or should be extracted
    from the source_file_name, or should be enumerated for multiple file
    uploads.
    """
    if destination_file_name:
        if file_number:
            destination_file_name = enumerate_destination_file_name(
                destination_file_name, file_number
            )
        else:
            destination_file_name = destination_file_name
    else:
        destination_file_name = extract_file_name_from_source_full_path(
            source_full_path
        )

    return destination_file_name


def clean_folder_name(folder_name):
    """
    Cleans folders name by removing duplicate '/' as well as leading and
    trailing '/' characters.
    """
    folder_name = folder_name.strip("/")
    if folder_name != "":
        folder_name = os.path.normpath(folder_name)
    return folder_name


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path
    variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}'
    )
    combined_name = os.path.normpath(combined_name)

    return combined_name
