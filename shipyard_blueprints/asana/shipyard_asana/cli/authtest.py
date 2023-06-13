import requests
import os

def get_args():
    args = {}
    args['access_token'] = os.getenv('ASANA_ACCESS_TOKEN')
    return args

def main():
    args = get_args()
    token = args['access_token']
    url = 'https://app.asana.com/api/1.0/users/me'
    headers = {"Authorization": f"Bearer {token}" }
    resp = requests.get(url = url, headers=  headers)
    if resp.status_code == 200:
        return 0
    else:
        return 1

if __name__  == '__main__':
    main()



