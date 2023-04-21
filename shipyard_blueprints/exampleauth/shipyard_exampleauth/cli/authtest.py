import os
import sys
# from shipyard_blueprints import ExampleClient


def get_args():
    args = {
            'password': os.getenv('EXAMPLEAUTH_PASSWORD'),
            'confirmPassword': os.getenv('EXAMPLEAUTH_CONFIRM_PASSWORD'),
            }
    return args


def main():
    args = get_args()

    code = 1
    if args['password'] is not None and args['password'] != '':
        if args['password'] == args['confirmPassword']:
            code = 0

    sys.exit(code)


if __name__ == '__main__':
    main()

