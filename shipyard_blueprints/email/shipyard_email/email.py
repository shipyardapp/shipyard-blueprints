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
        if send_method:
            self.send_method = send_method.lower()
        else:
            self.send_method = "tls"
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
