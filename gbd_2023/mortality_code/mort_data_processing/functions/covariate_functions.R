#' @title Generate covariates
#'
#' @description Generate flatfiles of covariates to merge onto the handoffs
#'
#' @param covs \[`data.table()`\]\cr
#'   list of covariate_ids to merge together
#'
#' @param year_start \[`data.table()`\]\cr
#'   year to prep covariates back in time to
#'
#' @param release_id \[`data.table()`\]\cr
#'   release_id for GBD round

generate_covariates <- function(covs, year_start, release_id) {

  source("FILEPATH")
  source("FILEPATH")

  covariates <- get_ids("covariate")

  cov_ids <- c()
  for (cov in covs) {

    covariate_id <- covariates[covariate_name_short == cov]$covariate_id
    cov_ids <- c(cov_ids, covariate_id)

  }

  cov_dt <- lapply(cov_ids, function(cov) {

    print(cov)

    get_covariate_estimates(
      covariate_id = cov,
      release_id = release_id
    )

  })

  saveRDS(
    cov_dt,
    fs::path("FILEPATH")
  )

}

#' @title Merge covariates
#'
#' @description Merge on covariates to the handoffs
#'
#' @param dt \[`data.table()`\]\cr
#'   Dataset without covariates merged on
#'
#' @param cov_dt \[`data.table()`\]\cr
#'   List of covariate_ids to merge on
#'
#' @param year_start \[`data.table()`\]\cr
#'   year to prep covariates back in time to
#'
#' @return \[`data.table()`\]\cr
#'   Dataset with covariates merged on

merge_covariates <- function(dt, cov_dt, year_start) {

  id_cols <- intersect(names(dt), names(cov_dt))

  # remove ids if cov is all age or all sex
  if (length(unique(cov_dt$sex_id)) == 1) {
    id_cols <- id_cols[id_cols != "sex_id"]
  }

  if (length(unique(cov_dt$age_group_id)) == 1) {
    id_cols <- id_cols[id_cols != "age_group_id"]
  }

  cov_dt <- cov_dt[year_id >= year_start]

  # rename value column and subset to only needed columns
  cov_name <- unique(cov_dt$covariate_name_short)
  setnames(
    cov_dt,
    "mean_value",
    cov_name,
    skip_absent = TRUE
  )

  keep_cols <- c(id_cols, cov_name)

  cov_dt <- cov_dt[, ..keep_cols]

  # add in implied 0s
  if (min(cov_dt$year_id) > year_start) {

    cov_dt_implied <- CJ(
      year_id = year_start:(min(cov_dt$year_id) - 1),
      location_id = unique(dt$location_id),
      age_group_id = unique(cov_dt$age_group_id),
      sex_id = unique(cov_dt$sex_id),
      mean_value = 0
    )
    setnames(cov_dt_implied, "mean_value", cov_name)

    cov_dt <- rbind(cov_dt_implied, cov_dt)

  }

  # merge
  dt <- merge(
    dt,
    cov_dt,
    by = id_cols,
    all.x = TRUE
  )

  return(dt)

}
