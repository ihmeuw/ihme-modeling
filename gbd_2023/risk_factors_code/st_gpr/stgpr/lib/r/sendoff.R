# NOTE: This function should get removed, and the API function should be updated to use
# Reticulate.

library(stringr)

PYTHON_PATH <- Sys.getenv(
    "STGPR_PATH_TO_PYTHON_EXECUTABLE",
    unset = "FILEPATH"
)
SENDOFF_PATH <- Sys.getenv(
    "STGPR_PATH_TO_SENDOFF_CLI",
    unset = "FILEPATH"
)

stgpr_sendoff_helper <- function(run_id, project, log_path, nparallel) {
    # Build system call.
    args <- c(PYTHON_PATH, SENDOFF_PATH)
    if (!is.null(log_path)) {
        args <- append(args, c("--log_path", log_path))
    }
    if (!is.null(nparallel)) {
        args <- append(args, c("--nparallel", nparallel))
    }
    args <- append(args, c(run_id, project))
    call <- paste(args, collapse = " ")

    # Run sendoff and print output.
    result <- system(call, intern = TRUE)
    cat(result, sep = "\n")
}
