import os
import re
import glob
import json
from zipfile import ZipFile
import tarfile
import logging

# Functions for Files


def extract_file_name_from_source_full_path(source_full_path: str):
    """
    Use the file name provided in the source_file_name variable. Should be run only
    if a destination_file_name is not provided.
    """
    destination_file_name = os.path.basename(source_full_path)
    return destination_file_name


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
    return destination_file_name


def determine_destination_file_name(
    *, source_full_path: str, destination_file_name: str, file_number: int = None
):
    """
    Determine what the destination_file_name should be.
    If the destination_file_name was provided use that name.
    If the destination_file_name is not provided, use the original source_file_name.
    If multiple files will be created, add file enumeration to the chosen method.
    """
    if destination_file_name:
        if file_number:
            destination_file_name = enumerate_destination_file_name(
                destination_file_name, file_number
            )
        else:
            destination_file_name = destination_file_name
    else:
        destination_file_name = extract_file_name_from_source_full_path(
            source_full_path
        )

    return destination_file_name


# Functions for Folders


def clean_folder_name(folder_name):
    """
    Cleans folder names by removing duplicate '/' as well as leading and trailing '/' characters.
    """
    folder_name = folder_name.strip("/")
    if folder_name != "":
        folder_name = os.path.normpath(folder_name)
    return folder_name


def create_folder_if_dne(destination_folder_name):
    """
    Checks to verify if the provided folder already exists.
    If not, the folder and all subfolders are created.
    """
    if not os.path.exists(destination_folder_name) and (destination_folder_name != ""):
        os.makedirs(destination_folder_name)


# Functions for mixing File + Folder


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}'
    )
    combined_name = os.path.normpath(combined_name)
    return combined_name


def determine_destination_full_path(
    destination_folder_name, destination_file_name, source_full_path, file_number=None
):
    """
    Determine the full destination path of the file to be uploaded or downloaded.
    """
    destination_file_name = determine_destination_file_name(
        destination_file_name=destination_file_name,
        source_full_path=source_full_path,
        file_number=file_number,
    )
    destination_full_path = combine_folder_and_file_name(
        destination_folder_name, destination_file_name
    )
    return destination_full_path


# Functions for handling large files


def compress_files(file_paths, destination_full_path, compression):
    """
    Given a list of files, compress all of them into a single file.
    Keeps the existing directory structure in tact.
    """
    if f".{compression}" in destination_full_path:
        compressed_file_name = destination_full_path
    else:
        compressed_file_name = f"{destination_full_path}.{compression}"

    if compression == "zip":
        compress_with_zip(file_paths, compressed_file_name, compression)

    if "tar" in compression:
        compress_with_tar(file_paths, compressed_file_name, compression)

    return compressed_file_name


def compress_with_zip(file_paths, compressed_file_name, compression):
    """
    Compress a list of files using the zip method.
    """
    write_method = determine_write_method(compression)

    with ZipFile(compressed_file_name, write_method) as zip:
        for file in file_paths:
            file = clean_folder_name(file.replace(os.getcwd(), ""))
            zip.write(file)
            print(f"Successfully compressed {file} into {compressed_file_name}")


def compress_with_tar(file_paths, compressed_file_name, compression):
    """
    Compress a list of files using the tar method.
    """
    write_method = determine_write_method(compression)

    with tarfile.open(compressed_file_name, write_method) as tar:
        for file in file_paths:
            file = clean_folder_name(file.replace(os.getcwd(), ""))
            tar.add(file)
            print(f"Successfully compressed {file} into {compressed_file_name}")


def determine_write_method(compression):
    """
    Given a specified compression type, choose the write method
    for generating the file.
    """
    if compression == "tar.bz2":
        write_method = "w:bz2"
    if compression == "tar.gz":
        write_method = "w:gz"
    else:
        write_method = "w"

    return write_method


def decompress_file(source_full_path, destination_full_path, compression):
    """
    Decompress a given file, using the specified compression method.
    """

    if compression == "zip":
        decompress_with_zip(source_full_path, destination_full_path, compression)

    if "tar" in compression:
        decompress_with_tar(source_full_path, destination_full_path, compression)


def decompress_with_zip(source_full_path, destination_full_path, compression):
    """Decompresses and extracts all the contents of a zip file

    Args:
        source_full_path (_type_): _description_
        destination_full_path (_type_): _description_
        compression (_type_): _description_
    """
    read_method = determine_read_method(compression)
    with ZipFile(source_full_path, read_method) as zipped:
        zipped.extractall(destination_full_path)
        print(
            f"Successfully extracted files from {source_full_path} to {destination_full_path}"
        )


def decompress_with_tar(source_full_path, destination_full_path, compression):
    read_method = determine_read_method(compression)
    file = tarfile.open(source_full_path, read_method)
    file.extractall(path=destination_full_path)
    print(
        f"Successfully extracted files from {source_full_path} to {destination_full_path}"
    )


def determine_read_method(compression):
    """
    Given a specified compression type, choose the read method
    for opening the file.
    """
    if compression == "tar.bz2":
        read_method = "r:bz2"
    if compression == "tar.gz":
        read_method = "r:gz"
    else:
        read_method = "r"

    return read_method


def is_file_too_large(file_path, max_size_bytes):
    """
    Determine if the file is too large for a specified limit.
    Used to conditionally compress a file.
    """
    if os.stat(file_path).st_size >= max_size_bytes:
        return True
    else:
        return False


def are_files_too_large(file_paths, max_size_bytes):
    """
    Determine if the total size of all files in a list are too large for a specified limit.
    Used to conditionally compress a file.
    """
    total_size = 0
    for file in file_paths:
        total_size += os.stat(file).st_size
    if total_size >= max_size_bytes:
        return True
    else:
        return False


# Functions for Regex Matching Logic


def find_all_local_file_names(source_folder_name=None):
    """
    Returns a list of all files that exist in the current working directory,
    filtered by source_folder_name if provided.
    """
    cwd = os.getcwd()
    cwd_extension = os.path.normpath(f"{cwd}/{source_folder_name}/**")
    all_paths = glob.glob(cwd_extension, recursive=True)
    file_names = remove_directories_from_path_list(all_paths)
    return file_names


def remove_directories_from_path_list(path_list):
    """
    Given a list of paths, checks to see if the path is a file.
    If so, it gets added to a separate list and returned.
    """
    only_files = []
    for path in path_list:
        if not os.path.isdir(path):
            only_files.append(path)
    return only_files


def find_all_file_matches(file_names, file_name_re):
    """
    Return a list of all matching_file_names that matched the regular expression.
    """
    matching_file_names = []
    for file in file_names:
        if re.search(file_name_re, file):
            matching_file_names.append(file)

    print(f"Found {len(matching_file_names)} file matches.")
    print(matching_file_names)
    return matching_file_names


def find_files_by_regex_or_exact_match(
    start_path=".",
    pattern=".*",
    exact_match=False,
    valid_extensions=None,
    verbose=False,
):
    """Find files in the given directory and its subdirectories that match the given regex pattern.

    Args:
        start_path (str): The starting path for the directory search. Defaults to the current directory.
        pattern (str): The regex pattern to match filenames against.
        exact_match (bool): Whether to match the pattern exactly or just contain it.
        valid_extensions (set): A set of valid file extensions to match against. ex: {".csv", ".xlsx", ".xls"}.

    Returns:
        list: A list of filenames (with their full paths) that match the given regex pattern.
    """

    if verbose:
        logging.basicConfig(level=logging.DEBUG)

    logging.debug("=== File Search Logs ===")
    if valid_extensions is None:
        valid_extensions = set()

    logging.debug('Validating that all extensions start with a "." (dot).')
    for extension in valid_extensions:
        if not extension.startswith("."):
            logging.debug(
                f"Extension {extension} does not start with a . (dot). Reformatting..."
            )
            valid_extensions.add(f".{extension}")
            valid_extensions.remove(extension)

    matching_files = []

    if exact_match:
        _, extension = os.path.splitext(pattern)
        if not bool(extension):
            raise ValueError(
                "When using exact_match, the pattern must include a file extension."
            )

        logging.debug(
            f"Searching for files matching {pattern} in directory: {start_path}."
        )
    else:
        logging.debug(
            f"Searching for files containing {pattern} in starting in directory: {start_path}"
        )

    for dirpath, _, filenames in os.walk(start_path):
        logging.debug(f"Searching directory: {dirpath}")
        for filename in filenames:
            if exact_match:
                is_match = re.fullmatch(pattern, filename) is not None
            else:  # Check if the filename contains the pattern
                is_match = re.search(pattern, filename) is not None

            # If there's a match, add the full path to the list
            if valid_extensions:
                if is_match and os.path.splitext(filename)[1] in valid_extensions:
                    logging.debug(f"Found matching file: {filename} in {dirpath}")
                    full_path = os.path.join(dirpath, filename)
                    matching_files.append(full_path)
            elif is_match:
                logging.debug(
                    f"Found matching file: {filename} in {dirpath}. No valid extensions restrictions provided."
                )
                full_path = os.path.join(dirpath, filename)
                matching_files.append(full_path)
    logging.debug("=== End File Search Logs ===")
    return matching_files


# Functions for Writing Files


def write_json_to_file(json_object, file_name):
    with open(file_name, "w") as f:
        f.write(json.dumps(json_object, ensure_ascii=False, indent=4))
    print(f"JSON data stored at {file_name}")
    return


def write_text_to_file(text_var, file_name):
    with open(file_name, "w") as f:
        f.write(text_var)
    print(f"Text data stored at {file_name}")
    return
