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
import shipyard_utils as shipyard
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
        self.send_method = send_method.lower() if send_method else "tls"
        super().__init__(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            username=username,
            password=password,
            send_method=send_method,
        )

    def connect_with_tls(self):
        self.logger.info(
            "Attempting to establish a connection using TLS (Transport Layer Security). TLS is the preferred protocol for its advanced encryption and security features."
        )
        try:
            context = ssl.create_default_context()

            server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            server.starttls(context=context)
            server.login(self.username, self.password)
        except Exception:
            self.logger.error("Failed connect via tls.")
            raise
        else:
            self.logger.info(
                "Connection successfully established using TLS (Transport Layer Security)."
            )
            self.send_method = "tls"

    def connect_with_ssl(self):
        self.logger.info(
            "Attempting to establish a connection using SSL (Secure Sockets Layer)."
        )

        try:
            context = ssl.create_default_context()
            server = smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, context=context)
            server.login(self.username, self.password)
        except Exception:
            self.logger.error("Failed to connect via ssl")
            raise
        else:
            self.logger.warning(
                "Note: SSL is an older legacy protocol. Consider upgrading the server to support TLS (Transport Layer Security), the more advanced and secure protocol."
            )
            self.send_method = "ssl"

    def connect_with_fallback(self):
        if self.send_method == "tls":
            try:
                self.connect_with_tls()
            except Exception:
                self.logger.warning(
                    "TLS connection unsuccessful. Fallback to SSL (Secure Sockets Layer) initiated. Note: SSL is an older legacy protocol, but it's being used to ensure message delivery where TLS is not supported."
                )
                try:
                    self.connect_with_ssl()
                    self.logger.warning(
                        "Connection successfully established using SSL (Secure Sockets Layer). To bypass the initial TLS attempt, you can update the send method from its default setting. Additionally, for enhanced security in future communications, we recommend upgrading the server to support TLS (Transport Layer Security), the more advanced and secure protocol."
                    )
                except Exception:
                    self.logger.error("Failed to establish connection.")
                    raise
        elif self.send_method == "ssl":
            try:
                self.connect_with_ssl()
            except Exception:
                self.logger.warning(
                    "SSL connection unsuccessful. Fallback to TLS (Transport Layer Security) initiated. TLS is the preferred protocol for its advanced encryption and security features."
                )
                try:
                    self.connect_with_tls()
                    self.logger.warning(
                        "Connection successfully established using TLS (Transport Layer Security). To bypass the initial SSL attempt, you can update the send method to TLS."
                    )
                except Exception:
                    self.logger.error("Failed to establish connection.")
                    raise

    def connect(self):
        if self.send_method not in ["tls", "ssl"]:
            self.logger.error("Invalid send method provided")
            # return self.EXIT_CODE_INVALID_METHOD
            return 1
        try:
            self.connect_with_fallback()
        except Exception:
            self.logger.error(
                "Could not connect to the email server.Ensure that the host, port, and credentials are correct"
            )
            return 1
        else:
            return 0
