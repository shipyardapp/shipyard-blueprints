from rudderstack import RudderStack
import sys
import argparse
import shipyard_utils as shipyard
import code


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--source-id', dest='source_id', required=False)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    access_token = args.access_token
    ## create the artifacts folder to save the run
    base_folder_name = shipyard.logs.determine_base_artifact_folder('rudderstack')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # get source id variable from user or pickel file if not inputted
    if args.source_id:
        source_id = args.source_id
    else:
        source_id = shipyard.logs.read_pickle_file(artifact_subfolder_paths,"source_id")
    
    
    rudderstack = RudderStack(vendor= "RudderStack", access_token= access_token)
    ## run check sync status
    sync_status_data = rudderstack._get_source_data(source_id)
    ## save sync run data as json file
    sync_run_data_file_name = shipyard.files.combine_folder_and_file_name(
        artifact_subfolder_paths['responses'],
        f'sync_run_{source_id}_response.json')
    shipyard.files.write_json_to_file(sync_status_data, sync_run_data_file_name)

    ## get sync status exit code and exit
    exit_code_status = rudderstack.determine_sync_status(sync_status_data, source_id)
    sys.exit(exit_code_status)


if __name__ == "__main__":
    main()



