from shipyard_templates import ExitCodeException
from typing import Union

EXIT_CODE_COMPRESSION_ERROR = 101
EXIT_CODE_DECOMPRESSION_ERROR = 102
EXIT_CODE_CONVERSION_ERROR = 103
EXIT_CODE_NO_FILE_MATCHES = 104
EXIT_CODE_FILE_NOT_FOUND = 105
EXIT_CODE_UNKNOWN_ERROR = 249


class CompressionError(ExitCodeException):
    def __init__(self, message: Union[str, Exception]):
        self.message = f"Error in attempting to compress source file: {message}"
        self.exit_code = EXIT_CODE_COMPRESSION_ERROR


class DecompressionError(ExitCodeException):
    def __init__(self, message: Union[str, Exception]):
        self.message = f"Error in attempting to decompress source file: {message}"
        self.exit_code = EXIT_CODE_DECOMPRESSION_ERROR


class ConversionError(ExitCodeException):
    def __init__(self, message: Union[str, Exception]):
        self.message = f"Error in attempting to convert source file: {message}"
        self.exit_code = EXIT_CODE_CONVERSION_ERROR
