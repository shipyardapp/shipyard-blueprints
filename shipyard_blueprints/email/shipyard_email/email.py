import argparse
import smtplib
import ssl
import os
import re
from email.message import EmailMessage
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
import shipyard_bp_utils as shipyard
from shipyard_templates import Messaging
import sys


class EmailClient(Messaging):
    EXIT_CODE_INVALID_METHOD = 200

    def __init__(
        self,
        smtp_host: str = None,
        smtp_port: str = None,
        username: str = None,
        password: str = None,
        send_method: str = "tls",
    ) -> None:
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.send_method = send_method.lower()
        super().__init__(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            username=username,
            password=password,
            send_method=send_method,
        )

    def connect(self):
        context = ssl.create_default_context()
        if self.send_method == "tls":
            try:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
                server.starttls(context=context)
                server.login(self.username, self.password)
                self.logger.info("Successfully connected via tls")
            except Exception as e:
                self.logger.error(
                    "Could not successfully connect via tls. Ensure that the host, port, and credentials are correct"
                )
                return 1
            else:
                return 0

        elif self.send_method == "ssl":
            try:
                with smtplib.SMTP_SSL(
                    self.smtp_host, self.smtp_port, context=context
                ) as server:
                    server.login(self.username, self.password)
                    self.logger.info("Successfully connected via ssl")
            except Exception as e:
                self.logger.error(
                    "Could not successfully connect via ssl. Ensure that the host, port, and credentials are correct"
                )
                return 1
            else:
                return 0
        else:
            self.logger.error("Signin method provided was not tls or ssl")
            return 1

    def _has_file(self, message: str) -> bool:
        """Returns true if a message string has the {{file.txt}} pattern

        Args:
            message (str): The message

        Returns:
            bool:
        """
        pattern = r"\{\{[^\{\}]+\}\}"
        res = re.search(pattern, message)
        if res is not None:
            return True
        return False

    def _extract_file(self, message: str) -> str:
        pattern = r"\{\{[^\{\}]+\}\}"
        res = re.search(pattern, message).group()
        file_pattern = re.compile(r"[{}]+")
        return re.sub(file_pattern, "", res)

    def create_message_object(
        self,
        sender_address: str,
        message: str,
        sender_name: str = None,
        to: str = None,
        cc: str = None,
        bcc: str = None,
        subject: str = None,
    ) -> MIMEMultipart:
        """Create a message object that is populated with the provided parameters

        Args:
            sender_address (str): Email address of the sender
            message (str): The email message itself
            sender_name (str, optional): The name of the sender
            to (str, optional): Email address of the recipient . Defaults to None.
            cc (str, optional): Email address of anyone cc'd. Defaults to None.
            bcc (str, optional): Email address of anyone bcc'd. Defaults to None.
            subject (str, optional): Title of the subject. Defaults to None.
        """
        # Check to see if the
        if self._has_file(message):
            file = self._extract_file(message)
            with open(file, "r") as f:
                content = f.read()
                f.close()
            pattern = r"\{\{[^\{\}]+\}\}"
            message = f"{re.sub(pattern, '', message)} \n {content}"

        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = f"{sender_name}<{sender_address}>"
        msg["To"] = to
        msg["Cc"] = cc
        msg["Bcc"] = bcc
        msg.attach(MIMEText(message, "html"))

        return msg

    def add_attachment_to_message(self, msg: MIMEMultipart, file_path: str):
        try:
            upload_record = MIMEBase("application", "octet-stream")
            upload_record.set_payload((open(file_path, "rb").read()))
            encoders.encode_base64(upload_record)
            upload_record.add_header(
                "Content-Disposition",
                "attachment",
                filename=os.path.basename(file_path),
            )
            msg.attach(upload_record)
            return msg
        except Exception as e:
            print(e)
            self.logger.exception("Could not attach the files to the email")

    def send_message(self, msg: MIMEMultipart) -> None:
        """Sends an email message based on the method (either tls or ssl) defined
        at the instantiation of the object.

        Args:
            msg (MIMEMultipart): The email message constructed.
        """
        context = ssl.create_default_context()
        if self.send_method == "tls":
            try:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
                server.starttls(context=context)
                server.login(self.username, self.password)
                server.send_message(msg)
                server.quit()
                self.logger.info("Message successfully sent")
            except Exception as e:
                self.logger.exception("Message could not be sent")
                raise (e)
        elif self.send_method == "ssl":
            try:
                with smtplib.SMTP_SSL(
                    self.smtp_host, self.smtp_port, context=context
                ) as server:
                    server.login(self.username, self.password)
                    server.send_message(msg)
                    self.logger.info("Message successfully sent")
            except Exception as e:
                self.logger.exception("Message could not be sent")
                raise (e)

    def add_shipyard_link_to_message(
        self, message: MIMEMultipart, shipyard_link: str
    ) -> MIMEMultipart:
        """
        Create a "signature" at the bottom of the email that links back to Shipyard.
        """
        message = f"{message}<br><br>---<br>Sent by <a href=https://www.shipyardapp.com> Shipyard</a> | <a href={shipyard_link}>Click Here</a> to Edit"
        return message

    @staticmethod
    def determine_file_to_upload(
        source_file_name_match_type: str, source_folder_name: str, source_file_name: str
    ):
        """
        Determine whether the file name being uploaded to email
        will be named archive_file_name or will be the source_file_name provided.
        """
        if source_file_name_match_type == "regex_match":
            file_names = shipyard.files.find_all_local_file_names(source_folder_name)
            matching_file_names = shipyard.files.find_all_file_matches(
                file_names, re.compile(source_file_name)
            )

            files_to_upload = matching_file_names
        else:
            source_full_path = shipyard.files.combine_folder_and_file_name(
                folder_name=source_folder_name, file_name=source_file_name
            )
            files_to_upload = [source_full_path]
        return files_to_upload

    @staticmethod
    def should_message_be_sent(
        conditional_send, source_full_path, source_file_name_match_type
    ):
        """
        Determine if an email message should be sent based on the parameters provided.
        """
        if source_file_name_match_type == "exact_match":
            if (
                (
                    conditional_send == "file_exists"
                    and os.path.exists(source_full_path[0])
                )
                or (
                    conditional_send == "file_dne"
                    and not os.path.exists(source_full_path[0])
                )
                or (conditional_send == "always")
            ):
                return True
            else:
                return False
        elif source_file_name_match_type == "regex_match":
            if (
                (conditional_send == "file_exists" and len(source_full_path) > 0)
                or (conditional_send == "file_dne" and len(source_full_path) == 0)
                or (conditional_send == "always")
            ):
                return True
            else:
                return False

