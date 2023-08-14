import argparse
from shipyard_blueprints import EmailClient
from shipyard_blueprints import shipyard_utils as shipyard


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--send-method",
        dest="send_method",
        choices={"ssl", "tls"},
        default="tls",
        required=False,
    )
    parser.add_argument("--smtp-host", dest="smtp_host", required=True)
    parser.add_argument("--smtp-port", dest="smtp_port", default="", required=True)
    parser.add_argument(
        "--sender-address", dest="sender_address", default="", required=True
    )
    parser.add_argument("--sender-name", dest="sender_name", default="", required=False)
    parser.add_argument("--to", dest="to", default="", required=False)
    parser.add_argument("--cc", dest="cc", default="", required=False)
    parser.add_argument("--bcc", dest="bcc", default="", required=False)
    parser.add_argument("--username", dest="username", default="", required=False)
    parser.add_argument("--password", dest="password", default="", required=True)
    parser.add_argument("--subject", dest="subject", default="", required=False)
    parser.add_argument("--message", dest="message", default="", required=True)
    parser.add_argument(
        "--source-file-name", dest="source_file_name", default="", required=False
    )
    parser.add_argument(
        "--source-folder-name", dest="source_folder_name", default="", required=False
    )
    parser.add_argument(
        "--conditional-send",
        dest="conditional_send",
        default="always",
        required=False,
        choices={"file_exists", "file_dne", "always"},
    )
    parser.add_argument(
        "--source-file-name-match-type",
        dest="source_file_name_match_type",
        default="exact_match",
        choices={"exact_match", "regex_match"},
        required=False,
    )
    parser.add_argument(
        "--file-upload",
        dest="file_upload",
        default="no",
        required=True,
        choices={"yes", "no"},
    )
    parser.add_argument(
        "--include-shipyard-footer",
        dest="include_shipyard_footer",
        default="TRUE",
        choices={"TRUE", "FALSE"},
        required=False,
    )

    args = parser.parse_args()
    if not (args.to or args.cc or args.bcc):
        parser.error("Email requires at least one recepient using --to, --cc, or --bcc")
    return args


def main():
    args = get_args()
    send_method = args.send_method
    smtp_host = args.smtp_host
    smtp_port = int(args.smtp_port)
    sender_address = args.sender_address
    sender_name = args.sender_name
    to = args.to
    cc = args.cc
    bcc = args.bcc
    username = args.username
    password = args.password
    subject = args.subject
    message = args.message
    conditional_send = args.conditional_send
    source_file_name_match_type = args.source_file_name_match_type
    file_upload = args.file_upload
    include_shipyard_footer = shipyard.args.convert_to_boolean(
        args.include_shipyard_footer
    )

    if not username:
        username = sender_address

    source_file_name = args.source_file_name
    source_folder_name = shipyard.files.clean_folder_name(args.source_folder_name)

    file_paths = EmailClient.determine_file_to_upload(
        source_file_name_match_type=source_file_name_match_type,
        source_folder_name=source_folder_name,
        source_file_name=source_file_name,
    )
    client = EmailClient(
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        username=username,
        password=password,
        send_method=send_method,
    )

    if EmailClient.should_message_be_sent(
        conditional_send, file_paths, source_file_name_match_type
    ):
        if include_shipyard_footer:
            shipyard_link = shipyard.args.create_shipyard_link()
            message = client.add_shipyard_link_to_message(
                message=message, shipyard_link=shipyard_link
            )

        msg = client.create_message_object(
            sender_address=sender_address,
            message=message,
            sender_name=sender_name,
            to=to,
            cc=cc,
            bcc=bcc,
            subject=subject,
        )

        if file_upload == "yes":
            if shipyard.files.are_files_too_large(file_paths, max_size_bytes=10000000):
                compressed_file_name = shipyard.files.compress_files(
                    file_paths, destination_full_path="Archive.zip", compression="zip"
                )
                print(f"Attaching {compressed_file_name} to message.")
                msg = client.add_attachment_to_message_object(
                    msg=msg, file_path=compressed_file_name
                )
            else:
                for file in file_paths:
                    print(f"Attaching {file} to message.")
                    msg = client.add_attachment_to_message_object(
                        msg=msg, file_path=file
                    )

        client.send_message(msg)
        # if send_method == 'ssl':
        #     send_ssl_message(
        #         smtp_host=smtp_host,
        #         smtp_port=smtp_port,
        #         username=username,
        #         password=password,
        #         msg=msg)
        # else:
        #     send_tls_message(
        #         smtp_host,
        #         smtp_port=smtp_port,
        #         username=username,
        #         password=password,
        #         msg=msg)
    else:
        if conditional_send == "file_exists":
            # print('File(s) could not be found. Message not sent.')
            client.logger.error("File(s) could not be found. Message not sent")
        if conditional_send == "file_dne":
            # print(
            #     'File(s) were found, but message was conditional based on file not existing. Message not sent.')
            client.logger.error(
                "File(s) were found, but message was conditional based on file not existing. Message not sent"
            )


if __name__ == "__main__":
    main()
