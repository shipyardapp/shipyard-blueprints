## error codes
EXIT_CODE_BAD_REQUEST = 206
EXIT_CODE_INVALID_PROJECT_ID = 201
EXIT_CODE_INVALID_RUN_ID = 202
EXIT_CODE_EXCESSIVE_REQUESTS = 203
EXIT_CODE_HEX_SERVER_ERROR = 204
EXIT_CODE_AUTHENTICATION_ERROR = 205
EXIT_CODE_UNKNOWN_ERROR = 3

## run status codes
## these are provided here: https://learn.hex.tech/docs/develop-logic/hex-api/api-reference#operation/GetRunStatus
EXIT_CODE_PENDING = 220
EXIT_CODE_RUNNING = 221
EXIT_CODE_ERRORED = 222
EXIT_CODE_COMPLETED = 0  ## success
EXIT_CODE_KILLED = 224
EXIT_CODE_UNABLE_TO_ALLOCATE_KERNEL = 225