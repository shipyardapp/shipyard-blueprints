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
# import shipyard_utils as shipyard
import utils 
from templates.messaging import Messaging

class EmailClient(Messaging):
    def __init__(self, smtp_host:str = None, smtp_port:str = None, 
                 sender_address:str = None, sender_name:str = None, 
                 to:str = None, cc:str = None, bcc:str = None, username:str = None,
                 password:str = None, subject:str = None, message:str = None) -> None:
        super().__init__(smtp_host = smtp_host, smtp_port = smtp_port,username =username, password = password)

    def connect(self):
        pass