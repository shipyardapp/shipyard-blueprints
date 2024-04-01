import argparse
import contextlib
import os
import tempfile

from shipyard_templates import CloudStorage, ExitCodeException, ShipyardLogger

logger = ShipyardLogger().get_logger()


def setup_connection(args: argparse.Namespace) -> tuple:
    """
    Return connection arguments and the path to the private key file if it was created

    Args:
        args (argparse.Namespace): The parsed arguments
    Returns:
        tuple: A tuple containing the connection arguments and the path to the private key file
    """
    key_path = None
    if not args.password and not args.key:
        raise ExitCodeException(
            "Must specify a password or a private key",
            CloudStorage.EXIT_CODE_INVALID_CREDENTIALS,
        )
    connection_args = {"host": args.host, "port": args.port, "user": args.username}

    if args.password:
        connection_args["pwd"] = args.password

    if args.key:
        if not os.path.isfile(args.key):
            fd, key_path = tempfile.mkstemp()
            logger.info(f"Storing private temporarily at {key_path}")
            with os.fdopen(fd, "w") as tmp:
                tmp.write(args.key)
            connection_args["key"] = key_path
        else:
            connection_args["key"] = args.key
    return connection_args, key_path


def tear_down(key_path: str, sftp):
    """
    Remove the private key file if it was created

    Args:
        key_path (str): The path to the private key file
    """
    if key_path:
        logger.info(f"Removing temporary private key file {key_path}")
        os.remove(key_path)
    with contextlib.suppress(Exception):
        sftp.close()
