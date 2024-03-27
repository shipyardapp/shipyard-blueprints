import tarfile
import pandas as pd
from zipfile import ZipFile
from typing import List
from shipyard_templates import ShipyardLogger
from shipyard_file_manipulation import errors


logger = ShipyardLogger.get_logger()


def compress(file_paths: List[str], target_path: str, compression: str):
    """
    Compress all of the matched files using the specified compression method.
    """
    try:
        if f".{compression}" in target_path:
            compressed_file_name = target_path
        else:
            compressed_file_name = f"{target_path}.{compression}"

        logger.debug(f"Files within the function are {file_paths}")
        if compression == "zip":
            with ZipFile(compressed_file_name, "w") as zip:
                for file in file_paths:
                    zip.write(file)
                    logger.info(f"Successfully compressed {file}")

        if compression == "tar.bz2":
            with tarfile.open(compressed_file_name, "w:bz2") as tar:
                for file in file_paths:
                    tar.add(file)
                    logger.info(f"Successfully compressed files")

        if compression == "tar":
            with tarfile.open(compressed_file_name, "w") as tar:
                for file in file_paths:
                    tar.add(file)
                    logger.info(f"Successfully compressed {file}")

        if compression == "tar.gz":
            with tarfile.open(compressed_file_name, "w:gz") as tar:
                for file in file_paths:
                    tar.add(file)
                    logger.info(f"Successfully compressed {file}")
    except Exception as e:
        raise errors.CompressionError(e)


def decompress(src_path: str, target_path: str, compression):
    """
    Decompress a given file, using the specified compression method.
    """

    try:
        if compression == "zip":
            with ZipFile(src_path, "r") as zip:
                zip.extractall(target_path)
            logger.info(
                f"Successfully extracted files from {src_path} to {target_path}"
            )

        if compression == "tar.bz2":
            file = tarfile.open(src_path, "r:bz2")
            file.extractall(path=target_path)
            logger.info(
                f"Successfully extracted files from {src_path} to {target_path}"
            )

        if compression == "tar":
            file = tarfile.open(src_path, "r")
            file.extractall(path=target_path)
            logger.info(
                f"Successfully extracted files from {src_path} to {target_path}"
            )

        if compression == "tar.gz":
            file = tarfile.open(src_path, "r:gz")
            file.extractall(path=target_path)
            logger.info(
                f"Successfully extracted files from {src_path} to {target_path}"
            )
    except Exception as e:
        raise errors.DecompressionError(e)


def convert(src_path: str, target_file_type: str, target_path: str):
    """
    Convert a file from one format to another.
    """
    try:
        input_df = pd.read_csv(src_path)
        extra_args = {"chunksize": 10000, "index": False}

        if target_file_type in ["tsv", "psv"]:
            if target_file_type == "tsv":
                input_df.to_csv(target_path, sep="\t", index=False)
            if target_file_type == "psv":
                input_df.to_csv(target_path, sep="|", index=False)
        if target_file_type == "xlsx":
            input_df.to_excel(target_path, engine="xlsxwriter", index=False)
        if target_file_type == "parquet":
            input_df.to_parquet(target_path, index=False)
        if target_file_type == "stata":
            input_df.to_stata(target_path)
        if target_file_type == "hdf5":
            store = pd.HDFStore(src_path)
            store.put(target_path, input_df)

        logger.info(f"Successfully converted {src_path} to {target_path}")
    except Exception as e:
        raise errors.ConversionError(e)
