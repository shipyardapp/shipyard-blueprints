from shipyard_blueprints import EmailClient
from settings import Email

user = Email.USER
pwd = Email.PWD
host = Email.HOST
port = Email.PORT

def test_connection():
    client = EmailClient(username= user, password= pwd, 
                         smtp_host= host, smtp_port= port)
    def connection_helper():
        try:
            client.connect()
            return 0
        except Exception as e:
            return 1
    assert connection_helper() == 0

