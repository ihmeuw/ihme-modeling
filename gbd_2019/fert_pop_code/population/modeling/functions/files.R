#' Load draw file from file path
#'
#' @param fpath Path to location files are saved.
#' @return data.table of draws.
load_hdf_draws <- function(fpath, by_vals = 0:999) {
  library(rhdf5)
  library(mortcore, lib = "FILEPATH/r-pkg")
  # load hdf file by draw
  draws <- lapply(by_vals, function(i) {
    mortcore::load_hdf(filepath = fpath, by_val = i)
  })
  draws <- rbindlist(draws)
  return(draws)
}

#' Save draw file to file path indexed by 'by_var'
#'
#' @param draws Draws to save.
#' @param fpath_draws Path to location files are saved.
save_hdf_draws <- function(draws, fpath_draws, by_var = "draw") {
  library(rhdf5)
  library(mortcore, lib = "FILEPATH/r-pkg")

  if (file.exists(fpath_draws)) file.remove(fpath_draws)
  rhdf5::h5createFile(fpath_draws)
  by_vals <- sort(unique(draws[[by_var]]))
  lapply(by_vals, function(i) {
    mortcore::save_hdf(data = draws, filepath = fpath_draws, by_var = by_var, by_val = i, level = 2)
  })
  rhdf5::H5close()
}

#' Save draw file and estimates for a given data.table of draws
#'
#' @param draws A data.table with id columns, 'by_var' column and value column.
#' @param fpath_summary Path to location summary files should be saved.
#' @param fpath_draws Path to location draw files should be saved.
#' @param id_vars Character vector of id variables that does not include the by_var.
#' @param by_var Variable to save the draws by ('draw').
#' @param value_var Character for the value variable that a summary will be calculated for
#' @return None.
save_outputs <- function(draws, fpath_summary, fpath_draws,
                         id_vars, by_var = "draw",
                         value_var = 'value') {
  library(mortcore, lib = "FILEPATH/r-pkg")

  if(!any(class(draws) == "data.table")) stop("'draws' argument must be a data.table")
  if (by_var %in% id_vars) stop("'by_var' can not be included in the set of 'id_vars'")
  if (!identical(sort(c(id_vars, by_var, value_var)), sort(names(draws)))) stop("'draws data.table must contain columns only for 'id_vars', 'by_var', and 'value_var'")

  # prep for saving
  setkeyv(draws, c(id_vars, by_var))

  # save summary file
  summary <- mortcore::summarize_data(draws, id_vars = id_vars, outcome_var = value_var, metrics = c("mean", "lower", "upper"))
  readr::write_csv(summary, path = fpath_summary)

  # save hdf file
  save_hdf_draws(draws, fpath_draws, by_var)
}
