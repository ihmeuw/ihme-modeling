#' Load draw file from file path.
#'
#' @param fpath Path to location files are saved.
#' @param by_values All by_values that are expected.
#' @return data.table of draws.
load_hdf_draws <- function(fpath, by_vals = 0:999) {
  library(rhdf5)
  library(mortcore, lib = "FILEPATH")

  # load hdf file by draw
  draws <- lapply(by_vals, function(i) {
    mortcore::load_hdf(filepath = fpath, by_val = i)
  })
  draws <- rbindlist(draws)
  return(draws)
}

#' Save draw file to file path indexed by 'by_var'.
#'
#' @param draws Draws to save.
#' @param fpath_draws Path to location files are saved.
save_hdf_draws <- function(draws, fpath_draws, by_var = "draw") {
  library(rhdf5)
  library(mortcore, lib = "FILEPATH")

  if (file.exists(fpath_draws)) file.remove(fpath_draws)
  rhdf5::h5createFile(fpath_draws)
  by_vals <- sort(unique(draws[[by_var]]))
  lapply(by_vals, function(i) {
    mortcore::save_hdf(data = draws, filepath = fpath_draws, by_var = by_var, by_val = i, level = 2)
  })
  rhdf5::H5close()
}

#' Check contents of hdf5 file for all by_values that are expected.
#'
#' @param fpath Path to location files is saved.
#' @param by_values All by_values that are expected.
#' @return whether or not the file is misformatted and doesn't have all expected contents.
check_h5_by_vars <- function(fpath, by_values) {

  library(rhdf5)
  library(assertable)

  # list content of hdf5 file
  content <- data.table(rhdf5::h5ls(fpath))
  content <- content[, list(by_var = as.numeric(name), dimension = as.numeric(dim))]

  # check that all possible by vars exist for each hdf5 file
  check_loc <- suppressWarnings(assertable::assert_ids(content, list(by_var = by_values), warn_only = T, quiet = T))
  misformatted <- !is.null(check_loc)

  return(misformatted)
}
