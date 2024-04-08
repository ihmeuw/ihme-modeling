#' Absorb shock mortality in HIV
#'
#' Preserve shock-specific mortality while preventing with-shock mortality from
#' becoming too high by reducing the non-shock mortality rates by the shock
#' specific mortality. Then ensure that all mortality is positive by increasing
#' non-shock mortality, if needed.
#'
#' @param dt Table with HIV-free, with-HIV and shock specific mortality to
#'   adjust.
#' @param dt_pop Population values used to convert mortality rates to deaths.
#'
#' @return Table with non-shock mortality adjusted to absorb shock-specific
#'   mortality.
#' @export
absorb_shocks <- function(dt, dt_pop) {

  id_cols <- c("location_id", "year_id", "sex_id", "age_start")
  value_cols <- c("mx_hiv_free", "mx_with_hiv", "mx_shock")

  stopifnot(all(c(id_cols, value_cols) %in% names(dt)))

  # Get rows to absorb ----

  dt_absorb <- dt[(mx_with_hiv + mx_shock) <= 0]

  # Convert to death space ----

  dt_absorb[dt_pop, pop := i.mean, on = id_cols]
  stopifnot("Missing population" = nrow(dt_absorb[is.na(pop)]) == 0)

  cols_mx <- grep("^mx", names(dt), value = TRUE)
  cols_deaths <- gsub("^mx", "deaths", cols_mx)

  dt_absorb[, (cols_deaths) := lapply(.SD, \(x) x * pop), .SDcols = cols_mx]

  # Absorb shocks ----

  dt_absorb[, `:=`(
    deaths_hiv_free_new = deaths_hiv_free - deaths_shock,
    deaths_with_hiv_new = deaths_with_hiv - deaths_shock
  )]

  # Return to rate space ----

  dt_absorb[, `:=`(
    mx_hiv_free_new = deaths_hiv_free_new / pop,
    mx_with_hiv_new = deaths_with_hiv_new / pop
  )]

  # Validate ----

  stopifnot(
    "New with-shock mx < 0" = dt_absorb[(mx_with_hiv_new + mx_shock) < 0, .N] == 0,
    "All new with-shock mx is 0" = dt_absorb[, all((mx_with_hiv_new + mx_shock) > 0)]
  )

  # Ensure positive mortality by bumping up non-shock
  if (nrow(dt_absorb[(mx_with_hiv_new + mx_shock) == 0]) > 0) {

    dt_min_mx <- dt_absorb[
      (mx_with_hiv_new + mx_shock) > 0,
      list(min_mx = min(mx_with_hiv_new + mx_shock)),
      by = id_cols
    ]

    dt_absorb[dt_min_mx, min_mx := i.min_mx, on = id_cols]
    dt_absorb[is.na(min_mx), min_mx := 1e-6]

    dt_absorb[`:=`(
        mx_hiv_free_new = mx_hiv_free_new + i.min_mx,
        mx_with_hiv_new = mx_with_hiv_new + i.min_mx
    )]

  }

  return(dt_absorb[, -c("pop", ..cols_deaths, "deaths_hiv_free_new", "deaths_with_hiv_new")])

}
