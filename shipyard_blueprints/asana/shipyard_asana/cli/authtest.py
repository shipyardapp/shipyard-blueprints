import requests
import os

def main():
    url = 'https://app.asana.com/api/1.0/users/me'
    headers = {"Authorization": f"Bearer {os.getenv('ASANA_ACCESS_TOKEN')}" }
    resp = requests.get(url = url, headers=  headers)
    if resp.status_code == 200:
        return 0
    else:
        return 1

if __name__  == '__main__':
    main()



