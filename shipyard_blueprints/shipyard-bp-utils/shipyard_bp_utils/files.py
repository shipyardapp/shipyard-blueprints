import os
import re
import csv
import glob
import json
import tarfile
import fnmatch

from zipfile import ZipFile
from shipyard_templates import ShipyardLogger
from pandas import DataFrame

logger = ShipyardLogger.get_logger()


def enumerate_destination_file_name(destination_file_name: str, file_number: int = 1):
    """
    Append a number to the end of the provided destination file name, before the file extension.
    Only used when multiple files are matched to, preventing the destination file from being continuously overwritten.
    """
    if re.search(r"\.", destination_file_name):
        destination_file_name = re.sub(
            r"\.", f"_{file_number}.", destination_file_name, 1
        )
    else:
        destination_file_name = f"{destination_file_name}_{file_number}"
    logger.debug(f"Enumerated destination file name: {destination_file_name}")
    return destination_file_name


def determine_destination_file_name(
    *, source_full_path: str, destination_file_name: str, file_number: int = None
) -> str:
    """
    Determines the destination file name based on provided parameters.

    Args:
    source_full_path (str): The full path of the source file.
    destination_file_name (str): The initial name of the destination file.
    file_number (int, optional): The number to append to the file name if necessary.

    Returns:
    str: The determined destination file name.
    """
    if not destination_file_name:
        os.path.basename(source_full_path)
    if file_number:
        name, ext = os.path.splitext(destination_file_name)
        destination_file_name = f"{name}_{file_number}{ext}"
    return destination_file_name


def clean_folder_name(folder_name: str) -> str:
    """
    Cleans a given folder name by removing duplicate '/' and leading/trailing '/' characters.

    Args:
    folder_name (str): The folder name to be cleaned.

    Returns:
    str: The cleaned folder name.
    """
    folder_name = os.path.normpath(folder_name)
    logger.debug(f"Cleaned folder name: {folder_name}")
    return folder_name


def create_folder_if_dne(destination_folder_name: str) -> None:
    """
    Creates a folder and all required subfolders if it does not already exist.

    Args:
    destination_folder_name (str): The name of the destination folder to be created.

    Returns:
    None
    """
    os.makedirs(destination_folder_name, exist_ok=True)


def combine_folder_and_file_name(folder_name: str, file_name: str) -> str:
    """
    Combines a folder name and file name into a single path.

    Args:
    folder_name (str): The name of the folder.
    file_name (str): The name of the file.

    Returns:
    str: The combined folder and file name as a single path.
    """
    return os.path.join(folder_name, file_name)


def determine_destination_full_path(
    destination_folder_name: str,
    destination_file_name: str,
    source_full_path: str,
    file_number: int = None,
) -> str:
    """
    Determines the full destination path of a file based on provided parameters.

    Args:
    destination_folder_name (str): The name of the destination folder.
    destination_file_name (str): The name of the destination file.
    source_full_path (str): The full path of the source file.
    file_number (int, optional): A number to append to the file name if necessary.

    Returns:
    str: The full destination path of the file.
    """
    destination_file_name = determine_destination_file_name(
        destination_file_name=destination_file_name,
        source_full_path=source_full_path,
        file_number=file_number,
    )
    return combine_folder_and_file_name(destination_folder_name, destination_file_name)


def compress_files(file_paths: list, destination_full_path: str, compression: str):
    """
    Compresses a list of files into a single compressed file.

    Args:
    file_paths (list): A list of file paths to be compressed.
    destination_full_path (str): The destination path for the compressed file.
    compression (str): The compression method to use ('zip', 'tar.gz', 'tar.bz2').

    Returns:
    str: The path to the compressed file.
    """
    if f".{compression}" in destination_full_path:
        compressed_file_name = destination_full_path
    else:
        compressed_file_name = f"{destination_full_path}.{compression}"

    if compression == "zip":
        compress_with_zip(file_paths, compressed_file_name)

    if "tar" in compression:
        compress_with_tar(file_paths, compressed_file_name, compression)


def compress_with_zip(file_paths: list, compressed_file_name: str):
    """
    Compresses a list of files using zip compression.

    Args:
    file_paths (list): A list of file paths to be compressed.
    compressed_file_name (str): The name of the final compressed file.

    Returns:
    None
    """
    with ZipFile(compressed_file_name, "w") as zip_file:
        for file in file_paths:
            file = clean_folder_name(file.replace(os.getcwd(), ""))
            zip_file.write(file)
            logger.debug(f"Successfully compressed {file} into {compressed_file_name}")


def compress_with_tar(file_paths: list, compressed_file_name: str, compression: str):
    """
    Compresses a list of files using tar compression.

    Args:
    file_paths (list): A list of file paths to be compressed.
    compressed_file_name (str): The name of the final compressed file.
    compression (str): The compression method ('tar.gz', 'tar.bz2').

    Returns:
    None
    """
    write_method = determine_write_method(compression)

    with tarfile.open(compressed_file_name, write_method) as tar:
        for file in file_paths:
            file = clean_folder_name(file.replace(os.getcwd(), ""))
            tar.add(file)
            logger.debug(f"Successfully compressed {file} into {compressed_file_name}")


def determine_write_method(compression: str) -> str:
    """
    Determines the write method for tar compression based on the compression type.

    Args:
    compression (str): The compression type ('tar.gz', 'tar.bz2').

    Returns:
    str: The write method for tarfile.
    """
    if compression == "tar.bz2":
        return "w:bz2"
    elif compression == "tar.gz":
        return "w:gz"
    else:
        return "w"


def are_files_too_large(file_paths: str, max_size_bytes: int) -> bool:
    """
    Determine if the total size of all files in a list are too large for a specified limit.
    Used to conditionally compress a file.

    Args:
    file_paths (list): A list of file paths to be compressed.
    max_size_bytes (int): The maximum size in bytes that the files can be.

    Returns:
    bool: True if the total size of all files is greater than the max_size_bytes, else False.
    """
    total_size = sum(os.stat(file).st_size for file in file_paths)
    return total_size >= max_size_bytes


def find_all_local_file_names(source_folder_name: str = None) -> list:
    """
    Returns a list of all files that exist in the current working directory,
    filtered by source_folder_name if provided.

    Args:
    source_folder_name (str, optional): The name of the source folder to filter by.

    Returns:
    list: A list of all files that exist in the current working directory.
    """
    logger.debug(f"Finding all local file names in {source_folder_name}...")
    cwd = os.getcwd()
    cwd_extension = os.path.normpath(f"{cwd}/{source_folder_name}/**")
    all_paths = glob.glob(cwd_extension, recursive=True)
    logger.debug(f"Found {len(all_paths)}.")
    logger.debug(all_paths)
    return remove_directories_from_path_list(all_paths)


def remove_directories_from_path_list(path_list: list) -> list:
    """
    Given a list of paths, checks to see if the path is a file.
    If so, it gets added to a separate list and returned.

    Args:
    path_list (list): A list of paths to check.

    Returns:
    list: A list of paths that are files.
    """
    return [path for path in path_list if not os.path.isdir(path)]


def find_all_file_matches(file_names: list, file_name_re: str) -> list:
    """
    Return a list of all matching_file_names that matched the regular expression.

    Args:
    file_names (list): A list of file names to search.
    file_name_re (str): A regular expression to match against the file names.

    Returns:
    list: A list of all matching_file_names that matched the regular expression.
    """
    matching_file_names = [file for file in file_names if re.search(file_name_re, file)]

    logger.debug(f"Found {len(matching_file_names)} file matches.")
    logger.debug(matching_file_names)
    return matching_file_names


def find_matching_files(search_term: str, directory: str, match_type: str) -> list:
    """
    Search for files in a directory based on a search term and match type.

    Args:
    search_term (str): The search term to match against.
    directory (str): The directory to search in.
    match_type (str): The type of match to perform. Can be 'regex_match', 'exact_match', or 'glob_match'.

    Returns:
    list: A list of all matching_file_names that matched the regular expression.
    """
    if match_type == "exact_match":
        cwd = os.getcwd()
        full_path = os.path.normpath(f"{cwd}/{directory}/{search_term}")
        return [full_path] if os.path.exists(full_path) else []

    filenames = find_all_local_file_names(directory)
    if match_type == "regex_match":
        return find_all_file_matches(filenames, search_term)
    elif match_type == "glob_match":
        search_term = fnmatch.translate(search_term)
        return find_all_file_matches(filenames, search_term)

    else:
        logger.error(f"Match type {match_type} is not supported.")
        raise ValueError(f"Match type {match_type} is not supported.")


# Functions for Writing Files
def write_json_to_file(json_object: dict, file_name: str) -> None:
    """
    Write a JSON object to a file.

    Args:
    json_object (dict): The JSON object to be written to a file.
    file_name (str): The name of the file to be written to.

    Returns:
    None
    """
    logger.debug(f"Writing JSON file: {file_name}, JSON Object: {json_object}...")
    with open(file_name, "w") as f:
        f.write(json.dumps(json_object, ensure_ascii=False, indent=4))
    logger.info(f"JSON data stored at {file_name}")
    return


def read_json_file(filename: str) -> dict:
    """
    Read a JSON file and return the contents as a dictionary.

    Args:
    filename (str): The name of the file to be read.

    Returns:
    dict: The contents of the JSON file as a dictionary.
    """
    logger.debug(f"Reading JSON file: {filename}...")
    with open(filename, "r") as f:
        return json.load(f)


def open_file(file_path: str) -> str:
    """
    Open a file and return the contents as a string.

    Args:
    file_path (str): The path of the file to be opened.

    Returns:
    str: The contents of the file as a string.
    """
    logger.debug(f"Opening file: {file_path}...")
    with open(file_path, "r") as f:
        return f.read()


def write_file(file_path: str, contents: str) -> None:
    """
    Write contents to a file.

    Args:
    file_path (str): The path of the file to be written to.
    contents (str): The contents to be written to the file.

    Returns:
    None
    """
    logger.debug(f"Writing file: {file_path}...")
    with open(file_path, "w") as f:
        f.write(contents)
    logger.info(f"File written to {file_path}")
    return


def write_csv_to_file(csv_object: list, file_name: str) -> None:
    """
    Write a CSV object to a file.

    Args:
    csv_object (list): The CSV object to be written to a file.
    file_name (str): The name of the file to be written to.

    Returns:
    None
    """
    logger.debug(f"Writing CSV file: {file_name}...")
    with open(file_name, "w") as f:
        writer = csv.writer(f)
        writer.writerows(csv_object)
    logger.info(f"CSV data stored at {file_name}")
    return


def read_csv_file(filename: str) -> DataFrame:
    """
    Read a CSV file and return the contents as a DataFrame.

    Args:
    filename (str): The name of the file to be read.

    Returns:
    DataFrame: The contents of the CSV file as a DataFrame.
    """
    logger.debug(f"Reading CSV file: {filename}...")
    return DataFrame.from_csv(filename)
