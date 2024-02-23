import os
import sys
import argparse
import shipyard_bp_utils as shipyard

from shipyard_email.email_client import EmailClient
from shipyard_email.exceptions import (
    InvalidInputError,
    ConditionNotMetError,
    InvalidCredentialsError,
)
from shipyard_templates import ShipyardLogger, Messaging, ExitCodeException

MAX_SIZE_BYTES = 10000000

logger = ShipyardLogger.get_logger()


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--send-method", dest="send_method", default="tls", required=False
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
        raise InvalidInputError(
            "Email requires at least one recipient using --to, --cc, or --bcc"
        )
    return args


def main():
    try:
        args = get_args()
        send_method = args.send_method.lower() or "tls"
        sender_address = args.sender_address
        username = args.username
        message = args.message
        conditional_send = args.conditional_send
        file_upload = args.file_upload
        if args.include_shipyard_footer:
            include_shipyard_footer = shipyard.args.convert_to_boolean(
                args.include_shipyard_footer
            )
        else:
            logger.warning("include_shipyard_footer not set. Defaulting to TRUE.")
            include_shipyard_footer = True

        if not username:
            username = sender_address

        client = EmailClient(
            args.smtp_host, args.smtp_port, username, args.password, send_method
        )

        source_file_name = args.source_file_name
        source_folder_name = shipyard.files.clean_folder_name(args.source_folder_name)
        file_paths = shipyard.files.find_matching_files(
            source_file_name, source_folder_name, args.source_file_name_match_type
        )

        if conditional_send == "file_exists" and not file_paths:
            raise ConditionNotMetError(
                f'Condition not met: No files found matching "{source_file_name}" in "{source_folder_name}"'
            )
        elif conditional_send == "file_dne" and file_paths:
            raise ConditionNotMetError(
                f'Condition not met: Files found matching "{source_file_name}" in "{source_folder_name}"'
            )

        message = client.message_content_file_injection(message)

        if include_shipyard_footer:
            message = (
                f"{message}<br><br>---<br>Sent by <a href=https://www.shipyardapp.com> Shipyard</a> | "
                f"<a href={shipyard.args.create_shipyard_link()}>Click Here</a> to Edit"
            )

        if file_upload != "yes":
            file_paths = None
        elif file_upload == "yes" and shipyard.files.are_files_too_large(
            file_paths, max_size_bytes=MAX_SIZE_BYTES
        ):
            logger.info("Files are too large to attach. Compressing files.")
            compressed_file_name = shipyard.files.compress_files(
                file_paths,
                destination_full_path=os.path.join(os.getcwd(), "Archive"),
                compression="zip",
            )
            logger.info(f"Attaching {compressed_file_name} to message.")
            file_paths = [compressed_file_name]

        client.send_message(
            sender_address=sender_address,
            message=message,
            sender_name=args.sender_name,
            to=args.to,
            cc=args.cc,
            bcc=args.bcc,
            subject=args.subject,
            attachment_file_paths=file_paths,
        )
    except ExitCodeException as error:
        logger.error(error.message)
        sys.exit(error.exit_code)
    except Exception as e:
        logger.error(f"Failed to send email. {e}")
        sys.exit(Messaging.EXIT_CODE_UNKNOWN_ERROR)


if __name__ == "__main__":
    main()
