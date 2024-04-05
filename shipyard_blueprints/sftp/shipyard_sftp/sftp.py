import os
import re
import stat
from typing import Optional

import paramiko
from shipyard_templates import CloudStorage, ExitCodeException
from shipyard_templates.shipyard_logger import ShipyardLogger

from shipyard_sftp.exceptions import (
    UnknownException,
    InvalidCredentialsError,
    UploadError,
    FileMatchException,
    DownloadException,
    FileNotFound,
    DeleteException,
)

logger = ShipyardLogger().get_logger()


class SftpClient(CloudStorage):
    EXIT_CODE_DELETE_ERROR = 100

    def __init__(
            self, host: str, port: int, key: Optional[str] = None, user: Optional[str] = None,
            pwd: Optional[str] = None,
            transport: Optional[paramiko.Transport] = None
    ) -> None:
        """
        Initializes an SFTP client with the given connection parameters.

        Parameters:
        - host (str): The hostname or IP address of the SFTP server.
        - port (int): The port number of the SFTP server.
        - key (str, optional): The path to the private key file (if key-based authentication is used).
        - user (str, optional): The username for authentication.
        - pwd (str, optional): The password for authentication (if password-based authentication is used).

        Note: This class can use out-of-the-box paramiko for SFTP operations by calling the client property.
        ex:
            sftp = SftpClient(host, port, key, user, pwd)
            sftp.client.listdir()
        """
        self.transport = transport
        self.host = host
        self.port = port
        self.key = key
        self.user = user
        self.pwd = pwd
        self._client = None

    @property
    def client(self):
        """
        Returns an SFTP client connected to the specified host using the given authentication credentials.
        If the client is not already connected, it creates a new connection.

        Returns:
        - paramiko.SFTPClient: An SFTP client connected to the server.
        """
        logger.debug(f"Getting SFTP client for {self.host}")
        if self._client is None:
            self._client = self.get_sftp_client()
        return self._client

    def connect(self):
        """
        Connects to the SFTP server using the given authentication credentials.

        Returns:
        - int: 0 if the connection was successful, 1 otherwise.
        """
        logger.debug(f"Attempting to connect to {self.host}")
        try:
            self.get_sftp_client()
        except ExitCodeException as e:
            logger.authtest(e)
            return 1
        else:
            return 0

    def list_files(self, path: str = None):
        """
        Lists the files in the given directory.
        If no directory is specified, lists the files in the current directory.

        Currently not used in the current implementation but is kept for future use.

        Parameters:
        - path (str, optional): The remote directory path to list files from.

        Returns:
        - list: A list of file names in the directory.
        """
        return self.client.listdir(path) if path else self.client.listdir()

    def download(self, filename: str, destination: str):
        """
        Downloads a file from the SFTP server to the local machine.

        Parameters:
        - filename (str): The name of the file to download.
        - destination (str): The local path to save the downloaded file.

        Raises:
        - ExitCodeException(EXIT_CODE_FILE_NOT_FOUND): If the file was not found on the server.
        - ExitCodeException(EXIT_CODE_DOWNLOAD_ERROR): If an error occurred while downloading the file.
        - ExitCodeException: If a class method raises an exception, it is re-raised as an ExitCodeException.
        """
        try:
            if self.if_exists(filename):
                self.client.get(filename, destination)
            else:
                logger.error(f"{filename} does not exist.")
                raise FileNotFound()
        except FileNotFoundError as e:
            raise FileNotFound(e) from e
        except ExitCodeException:
            raise
        except Exception as e:
            raise DownloadException(e) from e

    def move(self, source: str, destination: str):
        """
        Moves a file from one location to another on the SFTP server.

        Parameters:
        - source (str): The original file path.
        - destination (str): The new file path.

        Raises:
        - ExitCodeException(EXIT_CODE_FILE_NOT_FOUND): If the source file was not found.
        - ExitCodeException(EXIT_CODE_UNKNOWN_ERROR): If an unknown error occurred while moving the file.
        - ExitCodeException: If a class method raises an exception, it is re-raised as an ExitCodeException.
        """
        logger.debug(f"Moving {source} to {destination}")
        try:
            self.client.stat(source)
            self.create_directory(os.path.dirname(destination))
            self.client.rename(source, destination)
        except FileNotFoundError as e:
            logger.error(f"Error: the file {source} was not not found")
            raise FileNotFound(e) from e
        except ExitCodeException:
            raise
        except OSError as e:
            logger.error(f"Error occurred while moving {source} to {destination}. Be sure the destination filename is "
                         f"correct or a file with the same name does not already exists.")
            raise UnknownException(e) from e
        except Exception as e:
            logger.error(f"Error occurred while moving {source} to {destination}. Due to {e}")
            raise UnknownException(e) from e

    def remove(self, filename: str):
        """
        Deletes a file from the SFTP server.

        Parameters:
        - filename (str): The name of the file to delete.

        Raises:
        - ExitCodeException(EXIT_CODE_DELETE_ERROR): If an error occurred while deleting the file.
        - ExitCodeException: If a class method raises an exception, it is re-raised as an ExitCodeException.
        """
        logger.debug(f"Deleting {filename}")
        try:
            self.client.remove(filename)
        except ExitCodeException:
            raise
        except Exception as e:
            raise DeleteException(e) from e

    def upload(self, localpath: str, remotepath: str):
        """
        Uploads a file from the local machine to the SFTP server.

        Parameters:
        - localpath (str): The local path of the file to upload.
        - remotepath (str): The remote path to save the uploaded file.

        Raises:
            UploadError: If an error occurred while uploading the file.
        """
        logger.debug(f"Uploading {localpath} to {remotepath}")
        try:
            self.create_directory(os.path.dirname(remotepath))
            self.client.put(localpath, remotepath, confirm=True)
            logger.info(f"Successfully uploaded {localpath} to {remotepath}")
        except ExitCodeException:
            raise
        except Exception as e:
            logger.error(f"Failed to upload {localpath} to {remotepath}")
            raise UploadError(e) from e

    def list_files_recursive(self, path: str, files_list: list = None):
        """
        Recursively lists all files in the given directory and its subdirectories.

        Parameters:
        - path (str): The remote directory path to list files from.
        - files_list (list, optional): A list that accumulates the file paths.

        Returns:
        - list: A list of file paths in the directory and its subdirectories.

        Raises:
            - FileMatchException: If an error occurred while listing the directory.
        """
        logger.debug(f"Listing files in {path}")
        try:
            if files_list is None:
                files_list = []
            for attr in self.client.listdir_attr(path):
                file_path = f"{path}/{attr.filename}"
                if stat.S_ISDIR(attr.st_mode):
                    self.list_files_recursive(file_path, files_list)
                else:
                    files_list.append(file_path)
            logger.debug(f"Files found: {files_list}")
            return files_list
        except ExitCodeException:
            raise
        except Exception as e:
            raise FileMatchException(e) from e

    def create_directory(self, remote_directory: str):
        """
        Recursively create remote directories if they do not exist.

        Parameters:
        - remote_directory (str): The remote directory path to create.
        """
        directories = self.get_directory_levels(remote_directory)
        for directory in directories:
            if not self.if_exists(directory):
                self.client.mkdir(directory)

    def get_sftp_client(self) -> paramiko.SFTPClient:
        """
        Creates and returns an SFTP client connected to the specified host using the given authentication credentials.

        Parameters:
        - host (str): The hostname or IP address of the SFTP server.
        - port (int): The port number of the SFTP server.
        - username (str): The username for authentication.
        - key (str, optional): The path to the private key file (if key-based authentication is used).
        - password (str, optional): The password for authentication (if password-based authentication is used).

        Returns:
        - paramiko.SFTPClient: An SFTP client connected to the server.

        Raises:
        - InvalidCredentialsError: If the given credentials are invalid.
        - UnknownException: If an unknown error occurred while creating the SFTP client.
        """
        try:
            if self.key:
                logger.debug(
                    f"Connecting to {self.host} using key-based authentication"
                )
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                key = paramiko.RSAKey.from_private_key_file(self.key)
                ssh.connect(
                    hostname=self.host, port=self.port, username=self.user, pkey=key
                )
                return ssh.open_sftp()

            else:
                logger.debug(
                    f"Connecting to {self.host} using password-based authentication"
                )
                if not self.transport:
                    logger.debug(f"Creating simple transport to {self.host}")
                    transport = paramiko.Transport((self.host, int(self.port)))
                else:
                    logger.debug("Explicit transport provided.")
                    transport = self.transport

                transport.connect(None, self.user, self.pwd)

                return paramiko.SFTPClient.from_transport(transport)

        except (
                paramiko.SSHException,
                paramiko.AuthenticationException,
                ValueError,
                FileNotFoundError,
        ) as auth_error:
            raise InvalidCredentialsError(auth_error) from auth_error
        except Exception as err:
            raise UnknownException(err) from err

    def find_files(self, folder_path: str, pattern: str):
        """
        Recursively finds files in the given directory and its subdirectories that match the given pattern.

        Aligns more with the original legacy code but is not used in the current implementation.
        Replaced with list_files_recursive.

        Parameters:
        - folder_path (str): The remote directory path to search for files.
        - pattern (str): The regular expression pattern to match file names.

        Returns:
        - list: A list of file paths in the directory and its subdirectories that match the pattern.

        Raises:
        - FileMatchException: If an error occurred while searching for files.
        """
        all_files = []
        try:
            for entry in self.client.listdir_attr(folder_path):
                if entry.filename.startswith("."):
                    continue  # Skip hidden files
                full_path = os.path.join(folder_path, entry.filename)
                if stat.S_ISDIR(entry.st_mode):
                    all_files.extend(self.find_files(full_path, pattern))
                else:
                    all_files.append(full_path)
        except Exception as e:
            raise FileMatchException(e) from e

        return [str(f) for f in all_files if re.match(pattern, os.path.basename(f))]

    def if_exists(self, path: str):
        """
        Checks if a file or directory exists on the SFTP server.

        Parameters:
        - path (str): The remote file or directory path to check.

        Returns:
        - bool: True if the file or directory exists, False otherwise.
        """
        try:
            self.client.stat(path)
            logger.debug(f"{path} exists.")
            return True
        except FileNotFoundError:
            logger.debug(f"{path} does not exist.")
            return False

    @staticmethod
    def get_directory_levels(path: str):
        """
        Splits a path into its constituent directories, including each level with its parent directories,
        using a for loop for explicit construction of each directory level.

        Used to create remote directories if they do not exist.

        Parameters:
        - path (str): The full path to split, ensuring no leading or trailing slashes.

        Returns:
        - list: A list of directory paths, including each level with its parent directories.
        """
        dirs = path.strip("/").split("/")
        nest_paths = []
        nested_path = ""

        for dir in dirs:
            if dir:
                nested_path = os.path.join(nested_path, dir) if nested_path else dir
                nest_paths.append(nested_path)

        return nest_paths

    def close(self):
        """
        Closes the SFTP client connection and sets the client attribute to None in order to reconnect if needed.
        """
        self.client.close()
        self._client = None
