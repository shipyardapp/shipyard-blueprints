from shipyard_blueprints import EmailClient
from settings import Email
import pytest

user = Email.USER
pwd = Email.PWD
host = Email.HOST
port = Email.PORT

client = EmailClient(username= user, password= pwd, 
                        smtp_host= host, smtp_port= port)
def test_connection():
    def connection_helper():
        try:
            client.connect()
            return 0
        except Exception as e:
            return 1
    assert connection_helper() == 0


def test_send():
    sender = 'wes.poulsen@shipyardapp.com'
    to = 'wes.poulsen@shipyardapp.com'
    subject = 'THIS IS A TEST'
    text = 'Hello! This is a test message'
    try:
        msg = client.create_message_object(sender_address= sender, message = text,
                                       to = to, subject = subject)
        client.send_message(msg)
        assert 1 == 1
    except Exception as e:
        raise(e)

def test_send_with_response():
    sender = 'wes.poulsen@shipyardapp.com'
    to = 'wes.poulsen@shipyardapp.com'
    subject = 'THIS IS A TEST'
    text = 'Hello! Here is the contents of the file: {{sample.txt}}'
    try:
        msg = client.create_message_object(sender_address= sender, message = text,
                                       to = to, subject = subject)
        client.send_message(msg)
        assert 1 == 1
    except Exception as e:
        raise(e)

# def pytest_addoption(parser):
#     parser.addoption('--send-method', default = 'tls', store = 'send_method', )



# if __name__ == '__main__':
#     test_send_with_response()