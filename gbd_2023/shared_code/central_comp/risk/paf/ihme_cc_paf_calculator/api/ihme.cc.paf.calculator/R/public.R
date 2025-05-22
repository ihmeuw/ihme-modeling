# Top-level public functions. Reticulate pass-throughs of public Python functions

#' Launch the PAF Calculator
#'
#' Launches the PAF Calculator for a given risk, creating PAF estimates and optionally
#' running `save_results`.
#'
#' See docs for more information: 
#'
#' @returns a PAF model_version ID for the run. This model version ID will be the
#'     epi model version ID if save_results is run on the resulting PAF
#' @seealso [get_paf_model_status()]
#' @export
launch_paf_calculator <- function(
    rei_id,
    cluster_proj,
    release_id,
    year_id = NULL,
    n_draws = paf_calculator$lib$constants$DEFAULT_N_DRAWS,
    codcorrect_version_id = NULL,
    como_version_id = NULL,
    rei_set_id = paf_calculator$lib$constants$DEFAULT_REI_SET_ID,
    cause_set_id = paf_calculator$lib$constants$DEFAULT_CAUSE_SET_ID,
    skip_save_results = paf_calculator$lib$constants$DEFAULT_SKIP_SAVE_RESULTS,
    resume = paf_calculator$lib$constants$DEFAULT_RESUME,
    model_version_id = NULL,
    root_run_dir = NULL,
    test = paf_calculator$lib$constants$DEFAULT_TEST,
    description = NULL
) {
    # Explicitly convert arguments to ints if not NULL since R and Python have different
    # default numeric types, number in R is considered floating point in Python otherwise
    convert_to_int <- function(x) {
        if (is.null(x)) { return(NULL) } else { return(as.integer(x)) }
    }

    result <- tryCatch({
        paf_calculator$launch_paf_calculator(
            rei_id = convert_to_int(rei_id),
            cluster_proj = cluster_proj,
            release_id = convert_to_int(release_id),
            year_id = convert_to_int(year_id),
            n_draws = convert_to_int(n_draws),
            codcorrect_version_id = convert_to_int(codcorrect_version_id),
            como_version_id = convert_to_int(como_version_id),
            rei_set_id = convert_to_int(rei_set_id),
            cause_set_id = convert_to_int(cause_set_id),
            skip_save_results = skip_save_results,
            resume = resume,
            model_version_id = convert_to_int(model_version_id),
            root_run_dir = root_run_dir,
            test = test,
            description = description
        )
    }, error = function(e) {
        python_trace <- reticulate::py_last_error()
        if (length(python_trace) > 0) {
            print(python_trace)
            reticulate::py_clear_last_error()
        } else {
            cat("Error: ", conditionMessage(e), "\n")
        }
        return()
    })
    return(result)
}


#' Get PAF model status
#'
#' Get PAF model status. Either success (0), running (1), or failed (2).
#'
#' It is not recommended to call this function more than once a minute to avoid
#' unnecessary database calls. Raises an error if no PAF model metadata is found for
#' given model_version_id.
#' 
#' @param model_version_id PAF model version ID to get status for
#' @returns 0 if PAF model finished successfully; 1 if the model is still running, or 2
#'     if the model failed.
#' @seealso [launch_paf_calculator()]
#' @export
get_paf_model_status <- function(model_version_id) {
    return(paf_calculator$get_paf_model_status(as.integer(model_version_id)))
}
