#' @title fill missing dates in time series
#'
#' @description Add row of NA's for dates that are not present in the data time
#'   series
#'
#' @param dt \[`data.table()`\]\cr
#'   Dataset with column "date"
#'
#' @return \[`data.table()`\]\cr
#'   Dataset where rows for missing dates have been added

fill_missing_dates <- function(dt) {
  dt <- copy(dt)
  missing_dates <- data.table()
  for(loc in unique(dt$location_id)) {
    min_date <- range(dt[location_id == loc]$date)[1]
    max_date <- range(dt[location_id == loc]$date)[2]
    temp <- data.table(
      "location_id" = loc,
      "date" = seq(as.IDate(min_date), as.IDate(max_date), by="days")
    )
    missing_dates <- rbind(missing_dates, temp)
  }
  dt <- merge(dt, missing_dates, by = c("location_id", "date"), all = T)
  return(dt)
}

#' @title Check time series for squareness by date
#'
#' @description Takes time series and makes sure there are not more dates
#'   between start to end of the time series than there are rows
#'
#' @param dt \[`data.table()`\]\cr
#'   Dataset with column "date"
#'
#' @return \[`data.table()`\]\cr
#'   Invisibly returns `dt` but throws error if inputs are not formatted
#'   correctly.

check_square <- function(dt) {
  dt <- copy(dt)
  dt[, ":=" (nrows = .N, ndates = (max(date) - min(date))), by = location_id]
  not_square <-  unique(dt[nrows < ndates, location_id])
  assertthat::assert_that(
    length(not_square) == 0,
    msg = paste0("Some locations in the covid dataset are not square: ",
                 paste(not_square, collapse = ", "))
  )
  dt <- dt[, -c("nrows","ndates")]
  return(invisible(dt))
}

#' @title Validate downloaded data
#'
#' @description Takes dataset and does basic checks before saving.
#'   Checks:
#'   1. Warn if missing locations
#'   2. Error if na values in critical columns
#'
#' @param dt \[`data.table()`\]\cr
#'   Dataset to check with "location_id"
#' @param process_locations \[`data.table()`\]\cr
#'   Dataset of locations to check if present in dataset that includes columns
#'   "location_id" and "is_estimate_1"
#' @param not_na \[`character()`\]\cr
#'   Columns in `dt` to produce error if contain NA values
#'
#' @return \[`data.table()`\]\cr
#'   Invisibly returns `dt` but throws warning or error if inputs are not
#'   formatted correctly.

validate_download_data <- function(dt,
                                   process_locations,
                                   not_na) {

  dt <- copy(dt)

  # check missing locations
  missing_locations <- setdiff(
    process_locations[(is_estimate_1), location_id],
    unique(dt$location_id)
  )
  if (length(missing_locations) > 0){
    warning( paste0("Missing data or import error for these locations: ",
                    paste(missing_locations, collapse = ", ")))
  }

  # Check not_na columns do not have na values
  assertable::assert_values(
    dt, not_na, test = "not_na", quiet = T
  )

  return(invisible(dt))
}

#' @title Validate downloaded data
#'
#' @description Takes location dataset and does basic checks before saving.
#'   Checks:
#'   1. Checks column values are sensible
#'   2. Checks age groups for consistency and presence of all-age aggregate
#'   3. Checks data is unique on id columns
#'   4. Checks column names are present and subsets if needed
#'
#' @param dt \[`data.table()`\]\cr
#'   Dataset to check with data columns all present. Must include covariates
#'   needed for em modeling.
#' @param id_cols \[`character()`\]\cr
#'   Columns that uniquely identify rows of `dt`. Used to check data is unique
#'   by these columns.
#' @param data_cols \[`character()`\]\cr
#'   Columns needed in `dt`. Used to check data contains these columns.
#'
#' @return \[`data.table()`\]\cr
#'   Invisibly returns `dt` but throws warning or error if inputs are not
#'   formatted correctly.

validate_em_data <- function(dt,
                             id_cols,
                             data_cols) {

  dt <- copy(dt)

  # check column values
  assertable::assert_values(
    dt[!is.na(deaths_covid)], "deaths_covid", "gte", 0, quiet = T
  )
  assertable::assert_values(
    dt[!is.na(idr_reference)], "idr_reference", test = "gte", 0, quiet = T
  )
  assertable::assert_values(
    dt[!is.na(person_years)], "person_years", test = "gte", 0, quiet = T
  )
  # check age groups
  test_age_groups <- dt[, .N, by = "age_name"]
  assertthat::assert_that(
    length(unique(test_age_groups$N)) == 1,
    msg = "Check inconsistency of age groups over time"
  )
  assertthat::assert_that(
    "0 to 125" %in% unique(dt$age_name),
    msg = "Missing all-age aggregates."
  )
  # for now check that data is unique on `id_cols`
  demUtils::identify_non_unique_dt(dt, c(id_cols))
  demUtils::assert_is_unique_dt(dt, c(id_cols))
  # check column names
  assertable::assert_colnames(
    dt, colnames = data_cols, only_colnames = T, quiet = T
  )
  dt <- dt[, .SD, .SDcols = data_cols]

  return(invisible(dt))

}
