library(stringr)

PYTHON_PATH <- Sys.getenv(
    "STGPR_PATH_TO_PYTHON_EXECUTABLE",
    unset = "FILEPATH"
)
REGISTER_PATH <- Sys.getenv(
    "STGPR_PATH_TO_REGISTER_CLI",
    unset = "FILEPATH"
)

register_stgpr_model_helper <- function(path_to_config, model_index_id) {
    # Build system call.
    args <- c(PYTHON_PATH, REGISTER_PATH)
    if (!is.null(path_to_config)) {
        args <- append(args, c(path_to_config))
    }
    if (!is.null(model_index_id)) {
        args <- append(args, c("--model_index_id", model_index_id))
    }
    call <- paste(args, collapse = " ")

    # Run registration and print output.
    result <- system(call, intern = TRUE)
    cat(result, sep = "\n")

    # Check for run ID in last line of stdout.
    # Return it if present. Otherwise return NA.
    run_id <- NA
    for (line in result) {
        match = str_match(line, "Created ST-GPR version with ID (\\d+)")
        if (any(!is.na(match))) {
            run_id <- as.integer(match[2])
        }
    }
    return(run_id)
}
