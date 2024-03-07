import ftplib
import os
from copy import deepcopy

from shipyard_templates import CloudStorage, ShipyardLogger

from shipyard_ftp import exceptions

logger = ShipyardLogger().get_logger()


class FtpClient(CloudStorage):
    def __init__(self, host: str, user: str, pwd: str, port: int = None) -> None:
        self._client = None
        self.host = host
        self.user = user
        self.pwd = pwd
        self.port = port or 21

    @property
    def client(self):
        """
        Returns the current FTP client if it exists, otherwise creates a new one

        :return: ftplib.FTP
        """
        if self._client is None:
            self._client = self.get_client()
        return self._client

    def connect(self):
        """
        Connect to the FTP server and return the status of the connection

        Returns 0 if the connection is successful, otherwise returns 1:
        """
        try:
            self.get_client()
        except Exception:
            return 1
        else:
            return 0

    def move(self, source_full_path: str, destination_full_path: str):
        """
        Move a single file from one directory of the ftp server to another

        Args:
            source_full_path (str): The full path of the file to move
            destination_full_path (str): The full path of the destination directory
        Raises:
            InvalidCredentials: If the credentials are invalid
            MoveError: If the file cannot be moved
        """
        current_dir = self.client.pwd()
        source_path = os.path.normpath(os.path.join(current_dir, source_full_path))
        dest_path = os.path.normpath(os.path.join(current_dir, destination_full_path))
        try:
            self.client.rename(source_path, dest_path)
        except exceptions.InvalidCredentials:
            raise
        except Exception as e:
            raise exceptions.MoveError(
                f"Failed to move {source_path}. Ensure that the source/destination file name and folder name are "
                f"correct. Error message from server: {e} ",
            ) from e

        logger.info(f"{source_path} successfully moved to " f"{dest_path}")

    def remove(self, file_path: str):
        """
        Remove a file from the ftp server

        Args:
            file_path (str): The full path of the file to remove
        Raises:
            InvalidCredentials: If the credentials are invalid
            DeleteError: If the file cannot be deleted
        """
        try:
            self.client.delete(file_path)
            logger.info(f"Successfully deleted {file_path}")
        except exceptions.InvalidCredentials:
            raise
        except Exception as e:
            raise exceptions.DeleteError(
                f"Failed to delete file {file_path}. Ensure that the folder path and file name our correct"
                f"Message from server: {e}"
            )

    def upload(self, source_full_path: str, destination_full_path: str):
        """
        Upload a file to the FTP server

        Args:
            source_full_path (str): The full path of the file to upload
            destination_full_path (str): The full path of the destination directory
        Raises:
            InvalidCredentials: If the credentials are invalid
            UploadError: If the file cannot be uploaded
        """
        try:
            with open(source_full_path, "rb") as f:
                self.client.storbinary(f"STOR {destination_full_path}", f)
        except exceptions.InvalidCredentials:
            raise
        except Exception as e:
            raise exceptions.UploadError(
                f"Failed to upload {source_full_path} to FTP server"
            ) from e

        logger.info(
            f"{source_full_path} successfully uploaded to " f"{destination_full_path}"
        )

    def get_client(self):
        """
        Attempts to create an FTP client at the specified host with the
        specified credentials

        Returns:
            ftplib.FTP: The FTP client
        Raises:
            InvalidCredentials: If the credentials are invalid
        """
        logger.info(
            f"Connecting to FTP server at {self.host}:{self.port} with user {self.user}..."
        )
        try:
            client = ftplib.FTP(timeout=300)
            client.connect(self.host, int(self.port))
            client.login(self.user, self.pwd)
            client.set_pasv(True)
            client.set_debuglevel(0)
            logger.info(
                f"Connected to FTP server at {self.host}:{self.port} successfully."
            )
            return client
        except Exception as e:
            raise exceptions.InvalidCredentials(
                f"Error accessing the FTP server with the specified credentials\n"
                f"The server says: {e}"
            ) from e

    def download(self, file_name: str, destination_full_path: str = None):
        """
        Download a selected file from the FTP server to local storage in
        the current working directory or specified path.

        Args:
            file_name (str): The name of the file to download
            destination_full_path (str): The full path of the destination directory
        Raises:
            InvalidCredentials: If the credentials are invalid
            DownloadError: If the file cannot be downloaded
        """
        if not self.client:
            self.get_client()

        os.makedirs(os.path.dirname(destination_full_path), exist_ok=True)
        try:
            logger.info(f"Attempting to download {file_name}...")
            with open(destination_full_path, "wb") as f:
                self.client.retrbinary(f"RETR {file_name}", f.write)
            logger.info(
                f"{file_name} successfully downloaded to {destination_full_path}"
            )
        except exceptions.InvalidCredentials:
            raise

        except Exception as e:
            os.remove(destination_full_path)
            raise exceptions.DownloadError(
                f"Failed to download {file_name}. Message from server: {e}"
            ) from e

        logger.info(f"{file_name} successfully downloaded to {destination_full_path}")

    def create_new_folders(self, destination_path: str):
        """
        Changes working directory to the specified destination path
        and creates it if it doesn't exist

        Args:
            destination_path (str): The full path of the destination directory
        Raises:
            InvalidCredentials: If the credentials are invalid
        """
        if not self.client:
            self.get_client()

        original_dir = self.client.pwd()
        for folder in destination_path.split("/"):
            try:
                self.client.cwd(folder)
            except exceptions.InvalidCredentials:
                raise
            except Exception:
                self.client.mkd(folder)
                self.client.cwd(folder)
        self.client.cwd(original_dir)

    def find_files_in_directory(self, folder_filter: str, files: list, folders: list):
        """
        Pull in a list of all entities under a specific directory and categorize them into files and folders.

        Does this by changing the directory to the entity name. If it can't, it's a file.

        Args:
            folder_filter (str): The full path of the folder to search
            files (list): A list of files
            folders (list): A list of folders
        Returns:
            list: The list of files and folders
        Raises:
            InvalidCredentials: If the credentials are invalid
        """
        original_dir = self.client.pwd()
        names = self.client.nlst(folder_filter)
        for name in names:
            if all(char == "." for char in name):  # Skip relative directories
                continue

            if "/" not in name:
                # Accounts for an issue where some FTP servers return file names
                # without folder prefixes.
                name = f"{folder_filter}/{name}"

            try:
                # If you can change the directory to the entity_name, it's a
                # folder.
                self.client.cwd(name)
                folders.append(name)
            except exceptions.InvalidCredentials:
                raise
            except ftplib.error_perm:
                files.append(name)
                continue
            self.client.cwd(original_dir)

        folders.remove(folder_filter)

        return files, folders

    def is_file(self, filename: str) -> bool:
        """
        Helper function to check if the path in a ftp server is a file or a directory.
        If it is a directory, ftp.size returns None

        Args:
            filename (str): The full path of the file to check
        Returns:
            bool: True if the path is a file, False otherwise
        Raises:
            InvalidCredentials: If the credentials are invalid
        """
        try:
            if self.client.size(filename) is not None:
                return True
        except exceptions.InvalidCredentials:
            raise
        except Exception:
            return False

    def get_all_nested_items(
        self, working_directory: str, dir_list: list = None
    ) -> list:
        """
        Recursive function to get all the nested files and directories on the FTP server.

        Args:
            working_directory (str): The full path of the working directory
            dir_list (list): A list of directories
        Returns:
            list: The list of files and folders
        Raises:
            InvalidCredentials: If the credentials are invalid
        """
        if dir_list is None:
            dir_list = []
        dirs = deepcopy(dir_list)
        paths = self.client.nlst(working_directory)
        for path in paths:
            if not self.is_file(path):
                dirs = self.get_all_nested_items(path, dir_list=dirs)
            else:
                dirs.append(path)
        return dirs
