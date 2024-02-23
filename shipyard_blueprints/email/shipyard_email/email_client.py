import os
import ssl
import smtplib

from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from shipyard_templates import Messaging, ShipyardLogger
from shipyard_email.exceptions import (
    MessageObjectCreationError,
    InvalidFileInputError,
    InvalidCredentialsError,
    InvalidInputError,
    handle_exceptions,
)

TLS_FALLBACK_WARNING = """TLS connection unsuccessful. Fallback to SSL (Secure Sockets Layer) initiated. Note: SSL is 
an older legacy protocol, but it's being used to ensure message delivery where TLS is not supported."""
SSL_SUCCESS_WARNING = """Connection successfully established using SSL (Secure Sockets Layer). To bypass the initial 
TLS attempt, you can update the send method from its default setting. Additionally, for enhanced security in future 
communications, we recommend upgrading the server to support TLS (Transport Layer Security), the more advanced and 
secure protocol."""
SSL_FALLBACK_WARNING = """SSL connection unsuccessful. Fallback to TLS (Transport Layer Security) initiated. TLS is 
the preferred protocol for its advanced encryption and security features."""
TLS_SUCCESS_WARNING = """SSL connection unsuccessful. Fallback to TLS (Transport Layer Security) initiated. TLS is 
the preferred protocol for its advanced encryption and security features."""
FAILED_FALLBACK_ERROR = (
    """Failed to establish connection with TSL or SSL. Check your credentials."""
)

TIMEOUT = 10
logger = ShipyardLogger().get_logger()


class EmailClient(Messaging):
    def __init__(
        self,
        smtp_host: str = None,
        smtp_port: int = None,
        username: str = None,
        password: str = None,
        send_method: str = "tls",
    ) -> None:
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.send_method = send_method.lower() or "tls"
        self.email_server = None

    def connect(self):
        """Establishes a connection to the SMTP server with fallback."""
        if self.send_method not in {"tls", "ssl"}:
            logger.authtest(
                f"Invalid send method provided: {self.send_method}. Valid options are 'tls' or 'ssl'."
                f"Preferably 'tls'"
            )
            return 1

        try:
            self.connect_with_fallback()
            return 0
        except InvalidCredentialsError:
            logger.authtest(
                "Invalid credentials provided. Please check your username and password."
            )
            return 1

    @handle_exceptions
    def _connect_tls(self):
        """Attempts to connect using TLS.

        Raises:
            InvalidCredentialsError: Raised if the credentials provided are invalid.
        """
        context = ssl.create_default_context()
        self.email_server = smtplib.SMTP(
            self.smtp_host, self.smtp_port, timeout=TIMEOUT
        )
        self.email_server.starttls(context=context)
        self.email_server.login(self.username, self.password)
        logger.info("Successfully connected to the SMTP server using TLS.")

    @handle_exceptions
    def _connect_ssl(self):
        """Attempts to connect using SSL.

        Raises:
            InvalidCredentialsError: Raised if the credentials provided are invalid.
        """
        context = ssl.create_default_context()
        self.email_server = smtplib.SMTP_SSL(
            self.smtp_host, self.smtp_port, context=context, timeout=TIMEOUT
        )
        self.email_server.login(self.username, self.password)
        logger.info("Successfully connected to the SMTP server using SSL.")

    def connect_with_fallback(self):
        """Attempts to connect using the specified send method, with fallback to the other method if unsuccessful.

        Raises:
            InvalidCredentialsError: Raised if the credentials provided are invalid.
        """
        if self.send_method == "tls":
            try:
                logger.debug("Attempting to connect using TLS...")
                self._connect_tls()
            except Exception:
                logger.warning(TLS_FALLBACK_WARNING)
                try:
                    self._connect_ssl()
                    logger.warning(SSL_SUCCESS_WARNING)
                except Exception as e:
                    raise InvalidCredentialsError(FAILED_FALLBACK_ERROR) from e

        elif self.send_method == "ssl":
            try:
                logger.debug("Attempting to connect using SSL...")
                self._connect_ssl()
            except Exception:
                logger.warning(SSL_FALLBACK_WARNING)
                try:
                    self._connect_tls()
                    logger.warning(TLS_SUCCESS_WARNING)
                except Exception as e:
                    raise InvalidCredentialsError(FAILED_FALLBACK_ERROR) from e

    @handle_exceptions
    def send_message(
        self,
        message: str,
        sender_address: str = None,
        sender_name: str = None,
        to: str = None,
        cc: str = None,
        bcc: str = None,
        subject: str = None,
        attachment_file_paths: list = None,
    ):
        """

        Args:
            sender_address: The email address of the sender.
            message: The body of the email message.
            sender_name: The name of the sender.
            to: The email address of the recipient.
            cc: The email address of the recipient to be copied.
            bcc: The email address of the recipient to be blind copied.
            subject: The subject of the email message.
            attachment_file_paths: The file path of the attachment to be included in the email message.

        Raises:
            MessageObjectCreationError: Raised if the message object cannot be created.
            InvalidFileInputError: Raised if the file path provided is invalid.
        """
        if not self.email_server:
            logger.info("No SMTP connection established. Attempting to connect...")
            self.connect()
        if not to and not cc and not bcc:
            raise InvalidInputError(
                "Email requires at least one recipient using --to, --cc, or --bcc"
            )

        message_object = self._create_message_object(
            message,
            sender_address,
            sender_name,
            to,
            cc,
            bcc,
            subject,
            attachment_file_paths,
        )
        self.email_server.send_message(message_object)
        logger.info("Email message successfully sent.")

    def close_connection(self):
        """Closes the SMTP connection if it's open."""
        if self.email_server:
            self.email_server.quit()
            self.email_server = None
            logger.info("SMTP connection closed.")

    def _create_message_object(
        self,
        message,
        sender_address,
        sender_name=None,
        to=None,
        cc=None,
        bcc=None,
        subject=None,
        file_path=None,
    ):
        """
        Create a Message object, msg, by using the provided send parameters.
        """
        logger.debug("Creating the message object..")
        try:
            message_obj = MIMEMultipart()

            message_obj["Subject"] = subject
            message_obj["From"] = f"{sender_name}<{sender_address}>"
            message_obj["To"] = to
            message_obj["Cc"] = cc
            message_obj["Bcc"] = bcc

            message_obj.attach(MIMEText(message, "html"))

            if file_path:
                message_obj = self._add_attachment_to_message_object(
                    message_obj, file_path
                )
        except Exception as e:
            raise MessageObjectCreationError(
                f"Failed to create the message object. {e}"
            ) from e
        else:
            logger.debug("Message object created successfully.")
            return message_obj

    def _add_attachment_to_message_object(self, message_obj, file_paths):
        """
        Add source_file_path as an attachment to the message object.
        """
        if isinstance(file_paths, str):
            file_paths = [file_paths]

        logger.debug("Attaching the file(s) to the message object..")
        for file in file_paths:
            try:
                message_obj = self._attach_file(message_obj, file)
            except Exception as e:
                raise MessageObjectCreationError(e) from e

        return message_obj

    @staticmethod
    def _attach_file(message_obj, file_path):
        if not os.path.exists(file_path):
            raise InvalidFileInputError(
                f"File not found at the provided path: {file_path}"
            )

        try:
            upload_record = MIMEBase("application", "octet-stream")
            upload_record.set_payload((open(file_path, "rb").read()))
            encoders.encode_base64(upload_record)
            upload_record.add_header(
                "Content-Disposition",
                "attachment",
                filename=os.path.basename(file_path),
            )
            message_obj.attach(upload_record)
        except Exception as e:
            raise MessageObjectCreationError(e) from e
        else:
            logger.debug("File attached to the message object successfully.")
            return message_obj
