import os

from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContainerClient
from shipyard_templates import CloudStorage, ShipyardLogger, ExitCodeException

from shipyard_azureblob import exceptions

logger = ShipyardLogger().get_logger()


class AzureBlobClient(CloudStorage):
    def __init__(self, connection_string: str, container_name: str = None) -> None:
        self.connection_string = (connection_string,)
        self.container_name = container_name
        self._service_account = None
        self._container = None

    @property
    def blob_service_client(self) -> BlobServiceClient:
        """
        Blob Service Client - this client represents interaction with the Azure storage account itself, and allows
        you to acquire preconfigured client instances to access the containers and blobs within.
        It provides operations to retrieve and configure the account properties as well as list, create, and delete
        containers within the account. To perform operations on a specific container or blob, retrieve a client using
        the get_container_client or get_blob_client methods.
        """
        if not self._service_account:
            try:
                self._service_account = BlobServiceClient.from_connection_string(
                    conn_str=self.connection_string
                )
                return self._service_account
            except Exception as e:
                raise exceptions.InvalidCredentialsError(e) from e

    @property
    def container(self) -> ContainerClient:
        """
        Container - this client represents interaction with a specific container (which need not exist yet), and allows
        you to acquire preconfigured client instances to access the blobs within.It provides operations to create,
        delete, or configure a container and includes operations to list, upload, and delete the blobs within it.
        To perform operations on a specific blob within the container,retrieve a client using the get_blob_client method
        """
        if not self.container_name:
            raise exceptions.InvalidInputError("Container name not provided")
        if not self._container:
            try:
                self._container = self.blob_service_client.get_container_client(
                    self.container_name
                )
            except ExitCodeException:
                raise
            except ResourceNotFoundError as e:
                raise exceptions.InvalidInputError(
                    f'Container "{self.container_name}" does not exist'
                ) from e
            except Exception as e:
                raise exceptions.UnknownException(e) from e
        return self._container

    def connect(self) -> int:
        """
        Checks if the connection to the Azure Storage Blob is successful

        Returns:
            int: 0 if the connection is successful, 1 if the connection is not successful
        """
        try:
            service_account = self.blob_service_client
        except Exception as e:
            logger.authtest(
                f"Could not connect to the Azure with the provided connection string: Message from Azure: {e}"
            )
            return 1
        else:
            logger.authtest("Successfully connected to Azure")
            return 0

    def move(self, source_full_path: str, destination_full_path: str) -> None:
        """
        Moves a single blob inside the same Azure Storage Blob Container.
        Since there's no move function, this function is a combination of copy and delete

        Args:
            source_full_path (str): The full path of the blob to be moved
            destination_full_path (str): The full path of the blob to be moved to

        Raises:
            exceptions.MoveError: If the move operation fails
        """
        logger.debug(
            f"Attempting to move blob {source_full_path} to {destination_full_path}..."
        )
        source = self.container.get_blob_client(source_full_path)
        destination = self.container.get_blob_client(destination_full_path)

        destination.start_copy_from_url(source.url, requires_sync=True)
        copy_job = destination.get_blob_properties().copy

        logger.debug(f"Copy status: {copy_job.status}")
        if copy_job.status != "success":
            destination.abort_copy(copy_job.id)
            raise exceptions.MoveError(
                f"Copy blob from {source_full_path} failed with status {copy_job.status}"
            )
        source.delete_blob()
        logger.info(f"Successfully moved {source.blob_name} to {destination.blob_name}")

    def upload(self, source_full_path: str, destination_full_path: str) -> None:
        """
        Uploads a single file to Azure Storage Blob.

        Args:
            source_full_path (str): The full path of the file to be uploaded
            destination_full_path (str): The full path of the file to be uploaded to

        Raises:
            exceptions.UploadError: If the upload operation fails
        """
        try:
            blob = self.container.get_blob_client(destination_full_path)

            with open(source_full_path, "rb") as data:
                blob.upload_blob(data)
        except ExitCodeException:
            raise
        except ResourceExistsError as e:
            logger.error(f'File "{source_full_path}" already exists in the container')
            raise e
        except Exception as e:
            raise exceptions.UploadError(
                f"Failed to upload {source_full_path} to {self.container_name}/{destination_full_path}. "
                f"Response from Azure: {e}"
            ) from e

        logger.info(
            f"{source_full_path} successfully uploaded to {self.container_name}/{destination_full_path}"
        )

    def download(self, file_name: str, destination_file_name: str) -> None:
        """
        Download a selected file from Google Cloud Storage to local storage in
        the current working directory.

        Args:
            file_name (str): The name of the file to be downloaded
            destination_file_name (str): The name of the file to be saved as
        """

        local_path = os.path.normpath(f"{os.getcwd()}/{destination_file_name}")
        blob = self.container.get_blob_client(file_name)

        with open(destination_file_name, "wb") as new_blob:
            blob_data = blob.download_blob()
            blob_data.readinto(new_blob)

        logger.info(
            f"{self.container_name}/{file_name} successfully downloaded to {local_path}"
        )

    def find_blob_file_names(self, prefix="") -> list[str]:
        """
        Fetched all the files in the bucket which are returned in a list as
        Google Blob objects
        """
        logger.debug(
            f"Attempting to find all files in {self.container_name} with prefix {prefix}..."
        )
        blob_list = self.container.list_blobs(name_starts_with=prefix)
        logger.bebug(
            f"Found {len(blob_list)} files in {self.container_name} with prefix {prefix}"
        )
        return [blob.name for blob in blob_list]

    def remove(self, file_name: str) -> None:
        """
        Delete Blob from Azure cloud storage

        Args:
            file_name (str): The name of the file to be deleted

        Raises:
            exceptions.DeleteError: If the delete operation fails
        """
        try:
            blob = self.container.get_blob_client(file_name)
            blob.delete_blob()
            logger.info(
                f"{self.container_name}/{file_name} delete function successfully ran"
            )
        except ExitCodeException:
            raise
        except:
            raise exceptions.DeleteError(
                f"{file_name} delete failed to run. The file {file_name} could not be found. Ensure that the name of "
                f"the file is correct"
            )
