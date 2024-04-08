#' Load HIV deaths
#'
#' Load draw-level HIV team results based on location HIV group and calculate
#' detailed under-1 HIV.
#'
#' @param loc_id Location to load
#' @param group HIV group for `loc_id`
#' @param year_start First year required for estimation
#' @param dt_pop Table of mean-level abriged population estimates
#' @param path_spectrum File path to spectrum input
#' @param path_stgpr File path to ST-GPR input
#' @param agg_u1_hiv Optionally include the aggregate under 1 age group in the
#'   results.
#'
#' @return Table of HIV-specific mortality rates and HIV-free ratios.
#'
#' @details # HIV groups
#'
#' Spectrum results are used for:
#'
#' * Group 1 (GEN)
#' * Group 2B (CON incomplete VR)
#' * Group 2C (CON no data)
#'
#' Non-HIV deaths are only used for Group 1 and aren't accurate for
#' ENN/LNN/PNN (they represent under-1 deaths, need to be split by envelope).
#'
#' ST-GPR results are used for:
#'
#' * Group 2A (CON complete VR)
#' * Group 2B
#'
#' We trust the VR systems in these locations so we take straight GPR results.
#'
#' @details # uncertainty for ST-GPR data
#'
#' The ST-GPR results file provided summary parameters of the distribution so
#' those parameters are used to simulate draws from that distribution. This
#' approach underestimates uncertainty by not accounting for covariance in
#' age and time.
#'
#' @export
load_hiv <- function(loc_id,
                     group,
                     year_start,
                     dt_pop,
                     path_spectrum,
                     path_stgpr,
                     agg_u1_hiv = FALSE) {

  # Load data ----

  if (group %in% c("1A", "1B", "2C")) {

    spec_draws <- fread(path_spectrum)
    spec_draws[, draw := run_num - 1]
    spec_draws[, c("non_hiv_deaths_prop", "run_num") := NULL]
    data.table::setnames(spec_draws, "hiv_deaths", "mx_spec_hiv")

    # Remove extra row ID column, if present
    if ("V1" %in% names(spec_draws)) spec_draws[, V1 := NULL]

  } else if (group %in% c("2A", "2B")) {

    dt_stgpr <- fread(path_stgpr)[
      location_id == loc_id & !is.na(gpr_mean),
      list(year_id, age_group_id, sex_id, gpr_mean, gpr_var)
    ]

    stopifnot(
      nrow(dt_stgpr) > 0,
      !anyNA(dt_stgpr)
    )

    spec_draws <- generate_stgpr_draws(dt_stgpr)

    # HIV ratios are only used for under-15 Group 1, fill with nonsense val.
    spec_draws[, non_hiv_deaths := 99999]

  } else {

    stop("Invalid HIV group")

  }

  # Fill early years with zero ----

  min_hiv_year <- min(spec_draws$year_id)

  if (year_start < min_hiv_year) {

    years_fill <- year_start:(min_hiv_year - 1)

    dt_zero <- data.table::CJ(
      year_id = year_start:(min_hiv_year - 1),
      age_group_id = unique(spec_draws$age_group_id),
      sex_id = unique(spec_draws$sex_id),
      draw = unique(spec_draws$draw),
      mx_spec_hiv = 0,
      non_hiv_deaths = 2
    )

    spec_draws <- data.table::rbindlist(
      list(dt_zero, spec_draws),
      use.names = TRUE
    )

  }

  # Extract under-1 results ----

  if (isTRUE(agg_u1_hiv)) {

    hiv_deaths_u1 <- spec_draws[age_group_id %in% c(2, 3, 388, 389)]
    hiv_deaths_u1[dt_pop, pop := i.mean, on = c("year_id", "sex_id", "age_group_id")]
    hiv_deaths_u1[, `:=`(
      mx_spec_hiv = mx_spec_hiv * pop,
      non_hiv_deaths = non_hiv_deaths * pop
    )]

    hiv_deaths_u1 <- hiv_deaths_u1[
      j = list(
        age_group_id = 28,
        mx_spec_hiv = sum(mx_spec_hiv) / sum(pop),
        non_hiv_deaths = sum(non_hiv_deaths) / sum(pop)
      ),
      by = c("year_id", "sex_id", "draw")
    ]

    spec_draws <- data.table::rbindlist(
      list(spec_draws, hiv_deaths_u1),
      use.names = TRUE
    )

  }

  # Calculate HIV-free ratio ----

  spec_draws[, hiv_free_ratio := non_hiv_deaths / (non_hiv_deaths + mx_spec_hiv)]

  # Clean and check ----

  spec_draws[is.na(hiv_free_ratio), hiv_free_ratio := 1]

  stopifnot(
    spec_draws[is.na(hiv_free_ratio), .N] == 0,
    all(spec_draws[, mx_spec_hiv >= 0]),
    all(spec_draws[age_group_id %in% c(2,3), mx_spec_hiv == 0])
  )

  spec_draws[, location_id := loc_id]
  data.table::setcolorder(spec_draws, "location_id")

  return(spec_draws)


}



#' Generate draws of ST-GRP modeled HIV
#'
#' Given the mean and variance distribution parameters, simulate draws from
#' a distribution.
#'
#' @param dt table with columns for the distribution mean and variance
#' @param n_draws Number of draws to generate
#'
#' @return table with draws of HIV from the simulated distribution.
generate_stgpr_draws <- function(dt, n_draws = 1000) {

  spec_draws <- data.table::copy(dt)

  # Generate draws by location/year/age/sex
  # Use Delta Method to transform into real space before making draws
  spec_draws[
    gpr_mean == 0,
    `:=`(zero = 1, gpr_sd = 0)
  ]
  spec_draws[
    gpr_mean != 0,
    `:=`(
      gpr_var = (1 / gpr_mean)^2 * gpr_var,
      gpr_sd = sqrt(gpr_var),
      gpr_mean = log(gpr_mean)
    )
  ]

  # Convert to real numbers then divide by 100 since the death rate is in
  # rate per capita * 100
  # Create normal distribution around the logged mean/sd
  spec_draws <- spec_draws[
    j = list(
      draw = seq_len(n_draws) - 1,
      mx_spec_hiv = exp(rnorm(n = n_draws, mean = gpr_mean, sd = gpr_sd)) / 100
    ),
    by = c("year_id", "age_group_id", "sex_id", "zero")
  ]

  spec_draws[zero == 1, mx_spec_hiv := 0]
  spec_draws[, zero := NULL]

  spec_draws_copy <-
    c(2, 3) |>
    lapply(\(x) {
      dt_copy <- spec_draws[age_group_id == 6]
      dt_copy[, `:=`(age_group_id = x, mx_spec_hiv = 0)]
    }) |>
    data.table::rbindlist()

  spec_draws <- rbind(spec_draws, spec_draws_copy)
  spec_draws[mx_spec_hiv < 0, mx_spec_hiv := 0]

  spec_draws[, list(year_id, age_group_id, sex_id, draw, mx_spec_hiv)]

}
