import os
import json

from shipyard_templates import Crm, ExitCodeException


def handle_request_errors(response):
    try:
        response_details = response.json()
    except json.decoder.JSONDecodeError:
        response_details = {}

    if response.status_code in {401, 403}:
        raise ExitCodeException(
            response_details.get("message", "Invalid credentials"),
            Crm.EXIT_CODE_INVALID_CREDENTIALS,
        )
    elif response.status_code == 429:
        raise ExitCodeException(
            response_details.get("message", "Rate limit exceeded"),
            Crm.EXIT_CODE_RATE_LIMIT,
        )
    elif response.status_code in {502, 504, 522, 524}:
        raise ExitCodeException(
            response_details.get("message", "Gateway timeout"), Crm.TIMEOUT
        )
    elif response.status_code in {503, 521, 523}:
        raise ExitCodeException(
            response_details.get("message", "Service unavailable"),
            Crm.EXIT_CODE_SERVICE_UNAVAILABLE,
        )
    elif response.status_code in {415, 400}:
        raise ExitCodeException(
            response_details.get("message", "Bad Request"),
            Crm.EXIT_CODE_BAD_REQUEST,
        )
    elif response.status_code in {409}:
        raise ExitCodeException(
            response_details.get("message", "Conflict"),
            Crm.EXIT_CODE_EXPORT_NOT_FINISHED,
        )
    else:
        raise ExitCodeException(response.text, Crm.EXIT_CODE_UNKNOWN_ERROR)


def connect(self):
    """
    Method for verifying connection to the Hubspot API
    """
    self.logger.debug("Verifying connection to Hubspot API")
    try:
        self._requests("crm/v3/imports/")
    except ExitCodeException:
        return 1
    else:
        self.logger.info("Successfully connected to Hubspot API")
        return 0


def validate_export_type(export_type):
    export_type = export_type.strip().lower()
    if export_type not in ["list", "view"]:
        raise ExitCodeException(
            "Invalid export type. Please choose between list or view",
            Crm.EXIT_CODE_INVALID_INPUT,
        )
    return export_type


def validate_import_operations(import_operation):
    import_operation = import_operation.strip().upper()
    if import_operation not in {"UPSERT", "CREATE", "UPDATE"}:
        raise ExitCodeException(
            "Invalid import operation. Please choose between UPSERT, CREATE, UPDATE",
            Crm.EXIT_CODE_INVALID_INPUT,
        )
    return import_operation


def validate_export_language(language):
    language = language.strip().upper()
    if language not in {
        "EN",
        "ES",
        "FR",
        "DE",
        "JA",
        "PT",
        "ZH",
        "NL",
        "PT_BR",
        "IT",
        "PL",
        "SV",
        "FI",
        "ZH_TW",
    }:
        raise ExitCodeException(
            "Invalid language. Please choose between EN, ES, FR, DE, JA, PT, ZH, NL, PT_BR, IT, PL, SV, FI, ZH_TW",
            Crm.EXIT_CODE_INVALID_INPUT,
        )
    return language


def validate_date_format(date_format):
    date_format = date_format.strip().upper()
    if date_format not in {"DAY_MONTH_YEAR", "MONTH_DAY_YEAR", "YEAR_MONTH_DAY"}:
        raise ExitCodeException(
            "Invalid date format. Please choose between DAY_MONTH_YEAR, MONTH_DAY_YEAR, YEAR_MONTH_DAY",
            Crm.EXIT_CODE_INVALID_INPUT,
        )
    return date_format


def validate_export_file_format(file_type):
    file_type = file_type.strip().upper()
    if file_type not in {"CSV", "XLSX", "XLS"}:
        raise ExitCodeException(
            "Invalid file type. Please choose between CSV, XLSX, XLS",
            Crm.EXIT_CODE_INVALID_INPUT,
        )
    return file_type


def column_to_hubspot(
    csv_column_name,
    hubspot_property_name,
    column_object_type_id="0-1",  # 0-1 is the default value for contacts
    column_type=None,
):
    if column_type:
        column_type = column_type.strip().upper()
        if column_type not in {"HUBSPOT_OBJECT_ID", "HUBSPOT_ALTERNATE_ID"}:
            raise ExitCodeException(
                "Invalid column type. Column type is used to specify that a column contains a unique identifier "
                "property Please choose between HUBSPOT_OBJECT_ID, HUBSPOT_ALTERNATE_ID",
                Crm.EXIT_CODE_INVALID_INPUT,
            )

        return {
            "columnObjectTypeId": column_object_type_id,
            "columnName": csv_column_name,
            "propertyName": hubspot_property_name,
            "columnType": column_type,
        }
    else:
        return {
            "columnObjectTypeId": column_object_type_id,
            "columnName": csv_column_name,
            "propertyName": hubspot_property_name,
        }


def validate_import_file_format(file_type):
    file_type = file_type.strip().upper()
    if file_type not in {"CSV", "SPREADSHEET"}:
        raise ExitCodeException(
            "Invalid file type. Please choose between CSV or SPREADSHEET: For CSV files, use a value of CSV. For Excel files, use a value of SPREADSHEET.",
            Crm.EXIT_CODE_INVALID_INPUT,
        )
    return file_type


def handle_import_file(
    filename, file_format="CSV", headers_match=True, hubspot_alternate_id="email"
):
    """
    Method for handling the import file.
    Currently only supports headers matching the Hubspot property names. Eventually will support mapping of headers to Hubspot property names.
    """

    if not os.path.exists(filename):
        raise ExitCodeException(
            f"File {filename} does not exist", Crm.EXIT_CODE_FILE_NOT_FOUND
        )
    if headers_match:
        mapping = []
        with open(filename, "r") as file:
            headers = file.readline().strip().split(",")

            for header in headers:
                if header == hubspot_alternate_id:
                    mapping.append(
                        column_to_hubspot(
                            header, header, column_type="HUBSPOT_ALTERNATE_ID"
                        )
                    )
                else:
                    mapping.append(column_to_hubspot(header, header))
            mapping = [column_to_hubspot(header, header) for header in headers]

    return {
        "fileName": os.path.basename(filename),
        "fileFormat": file_format,
        "fileImportPage": {"hasHeader": True, "columnMappings": mapping},
    }
