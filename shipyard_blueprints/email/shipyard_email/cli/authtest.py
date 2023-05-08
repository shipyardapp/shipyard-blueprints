import os
from shipyard_blueprints import EmailClient

def get_args():
    args = {}
    args['smtp_host'] = os.getenv('EMAIL_SMTP_HOST')
    args['smtp_port'] = os.getenv('EMAIL_SMTP_PORT')
    args['username'] = os.getenv('EMAIL_USERNAME')
    args['password'] = os.getenv('EMAIL_PASSWORD')
    return args


def main():
    args = get_args()
    client = EmailClient(**args)
    try:
        client.connect()
        return 0
    except Exception as e:
        return 1
    
if __name__ == '__main__':
    main()