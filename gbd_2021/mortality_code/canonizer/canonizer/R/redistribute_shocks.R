#' Redistribute shock deaths
#'
#' To avoid negative with-shock mortality while still preserving total shock
#' deaths in the standard abridged GBD ages, redistribute shock deaths between
#' the detailed single-year age groups within each abridged age based on the
#' distribution of with-HIV mortality.
#'
#' For example, if a location/year/sex would have negative with-shock mortality
#' in ages 15, 16, and 19, then shocks in ages 15 through 19 would be
#' redistributed such that the resulting with-shock mortality is positive for
#' all ages while keeping the sum of shocks in 15-19 constant.
#'
#' This might not be possible for some abridged ages, in which case the
#' best-effort redistribution is applied, giving each age a proportionate amount
#' of negative with-shock mortality.
#'
#' @param dt Table with columns for with-hiv and shock-specific mx, plus ID
#'   columns including age group starts.
#' @param dt_pop Table of detailed (single year) population estimates.
#' @param age_map Table of standard abridged age groups.
#' @param tol_shock_diff Maximum allowable difference between original and
#'   redistributed shocks for an abridged age.
#'
#' @return List of tables containing new shock-specific mx for successful
#'   (`success`) and non-successful (`partial`) redistribution. Partially
#'   successful attempts will still have new shock-specific mx representing
#'   the distribution that minimizes negative with-shock mx across the detailed
#'   ages.
#'
#' @export
redistribute_shocks <- function(dt, dt_pop, age_map, tol_shock_diff = 1e-10) {

  id_cols <- c("location_id", "year_id", "sex_id", "draw", "age_start")
  id_cols_abr <- c(setdiff(id_cols, "age_start"), "age_abr")
  value_cols <- c("mx_with_hiv", "mx_shock")

  stopifnot(all(c(id_cols, value_cols) %in% names(dt)))

  min_redis_age <- age_map[age_end - age_start > 1, min(age_start)]

  # Get IDs ----

  dt_redis_ids <- unique(
    dt[(mx_with_hiv + mx_shock) <= 0 & age_start >= min_redis_age, ..id_cols]
  )

  dt_redis_ids[
    age_map,
    `:=`(
      age_start_abr = i.age_start,
      age_length = i.age_end - i.age_start
    ),
    on = .(age_start >= age_start)
  ]

  # For ages at or above the standard abridged age map's terminal age,
  # redistribute shocks among all older ages
  dt_redis_ids[is.infinite(age_length), age_length := 1 + max(dt$age) - age_start_abr]

  dt_redis_ids[, age_start := NULL]
  data.table::setnames(dt_redis_ids, "age_start_abr", "age_start")
  dt_redis_ids <- unique(dt_redis_ids)

  dt_redis_ids <- dt_redis_ids[
    j = .(age_detailed = seq(from = age_start, by = 1, length.out = age_length)),
    by = id_cols
  ]
  data.table::setnames(
    dt_redis_ids,
    c("age_start", "age_detailed"),
    c("age_abr", "age_start")
  )

  # Get data for redistribution ----

  dt_redis <- dt[
    dt_redis_ids,
    c(..id_cols, "age_abr", "mx_with_hiv", "mx_shock"),
    on = id_cols
  ]

  dt_redis[dt_pop, pop := i.mean, on = setdiff(id_cols, "draw")]
  stopifnot("Missing population" = nrow(dt_redis[is.na(pop)]) == 0)

  dt_redis[, `:=`(
    deaths_with_hiv = mx_with_hiv * pop,
    deaths_shock = mx_shock * pop
  )]

  # Redistribute shocks ----

  dt_redis[
    j = deaths_shock_new := sum(deaths_shock) * deaths_with_hiv / sum(deaths_with_hiv),
    by = id_cols_abr
  ]

  # Validate ----

  check_redis <- dt_redis[
    j = .(
      deaths_with_hiv = sum(deaths_with_hiv),
      deaths_shock_new = sum(deaths_shock_new),
      shock_diff = sum(deaths_shock_new) - sum(deaths_shock),
      success = (sum(deaths_with_hiv) + sum(deaths_shock_new)) > 0
    ),
    by = id_cols_abr
  ]

  # Check that total shocks in the abridged age group didn't change
  stopifnot(
    "Total abridged shocks changed during redistribution" = nrow(check_redis[abs(shock_diff) > tol_shock_diff]) == 0
  )

  # Check for locations that could not be fully redistributed
  if (nrow(check_redis[!(success)]) > 0) {
    warning(paste(
      nrow(check_redis[!(success)]), "abridged age sets could not be fully redistributed."
    ))
  }

  dt_redis[
    check_redis,
    status := factor(
      fifelse(i.success, "success", "partial"),
      levels = c("success", "partial")
    ),
    on = id_cols_abr
  ]

  # Clean and return ----

  dt_redis[, mx_shock_new := deaths_shock_new / pop]
  remove_cols <- c(
    "pop", "deaths_with_hiv", "deaths_shock", "deaths_shock_new",
    value_cols
  )
  dt_redis[, (remove_cols) := NULL]

  return(split(dt_redis, by = "status", keep.by = FALSE))

}
