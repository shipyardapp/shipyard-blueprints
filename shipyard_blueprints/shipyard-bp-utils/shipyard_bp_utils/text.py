import hashlib
from shipyard_templates import ShipyardLogger

logger = ShipyardLogger.get_logger()


def hash_text(text_var: str, hash_algorithm="sha256"):
    """
    Hash the provided text with a specified hash_algorithm.
    """
    logger.debug(f"Hashing {text_var} with {hash_algorithm}...")

    hash_algorithms = {
        "sha256": hashlib.sha256,
        "sha512": hashlib.sha512,
        "md5": hashlib.md5,
    }

    if hash_algorithm not in hash_algorithms:
        logger.error(f"Hash algorithm {hash_algorithm} is not supported.")
        raise ValueError(f"Hash algorithm {hash_algorithm} is not supported.")

    hashed_text = hash_algorithms[hash_algorithm](text_var.encode("ascii")).hexdigest()
    logger.debug(f"Successfully hashed: {hashed_text}")

    return hashed_text
