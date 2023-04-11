import os
import pickle
from . import files


def determine_base_artifact_folder(vendor):
    """
    Creates the base folder structure for storing artifacts.
    Uses USER to support local runs.
    """
    artifact_folder_default = f'{os.environ.get("USER")}-artifacts'
    base_artifact_folder = files.clean_folder_name(
        f'{os.environ.get("SHIPYARD_ARTIFACTS_DIRECTORY",artifact_folder_default)}/{vendor}-blueprints/')
    return base_artifact_folder


def determine_artifact_subfolders(base_artifact_folder):
    subfolders = ['artifacts', 'responses', 'logs', 'variables']
    subfolder_paths = {}
    for subfolder in subfolders:
        clean_subfolder_path = files.clean_folder_name(
            f'{base_artifact_folder}/{subfolder}')
        subfolder_paths[subfolder] = clean_subfolder_path
    return subfolder_paths


def create_artifacts_folders(subfolder_paths):
    """
    Creates all artifact subfolders so that generated logging files can be inserted in immediately.
    Returns a dictionary with the paths listed out.
    """
    for key, path in subfolder_paths.items():
        files.create_folder_if_dne(path)


def create_pickle_file(subfolder_paths, variable_name, variable_value):
    pickle_file_name = files.combine_folder_and_file_name(
        subfolder_paths['variables'], f'{variable_name}.pickle')
    with open(pickle_file_name, 'wb') as f:
        pickle.dump(variable_value, f)


def read_pickle_file(subfolder_paths, variable_name):
    pickle_file_name = files.combine_folder_and_file_name(
        subfolder_paths['variables'], f'{variable_name}.pickle')
    with open(pickle_file_name, 'rb') as f:
        variable_name = pickle.load(f)
    return variable_name
