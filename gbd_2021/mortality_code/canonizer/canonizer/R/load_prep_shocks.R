#' Load and prep shock deaths
#'
#' Load all-cause shocks, convert to rates, extend terminal age, and
#' interpolate to single-year ages.
#'
#' @param loc Location ID to prep
#' @param pop Abridged population estimates
#' @param map_age Mapping of abridged (column name `output`) to single year LT
#'   age groups (column name `output`)
#' @param term_age_id Terminal age group of input shocks
#' @param ext_age_ids Age groups to extend terminal age of shocks to
#' @param dir_input Input directory for shock files
#'
#' @details # Extension and Interpolation
#'
#' Shocks deaths are provided in the standard GBD age groups. In order to be
#' compatible with the canonical life table, values must be created for
#' single-year ages up to and beyond the normal terminal age provided by
#' shocks (see `term_age_id` and `ext_age_ids` parameters). The method for
#' creating these additional ages is to assume constant rates within the less
#' detailed age groups.
#'
#' @return A table of single year shock rates, with most detailed under 1 ages
#'   and up to the life table terminal age.
#'
#' @export
load_prep_shocks <- function(loc,
                             pop,
                             map_age,
                             term_age_id,
                             ext_age_ids,
                             dir_input) {

  id_cols <- c("location_id", "year_id", "sex_id", "age_group_id")
  stopifnot(all(c("input", "output") %in% names(map_age)))

  # Load and format ----

  dt_shocks <-
    fread(
      fs::path(dir_input, paste0("shocks_", loc, ".csv"))
    )[cause_id == 294, -"cause_id"] |>
    melt(
      id.vars = id_cols,
      variable.name = "draw",
      variable.factor = FALSE,
      value.name = "deaths"
    )

  dt_shocks[, draw := as.integer(gsub("draw_", "", draw))]

  # Convert to rates ----

  dt_shocks[pop, mx := deaths / i.mean, on = id_cols]
  dt_shocks[, deaths := NULL]

  # Extend terminal age ----

  dt_shocks_old <- replicate_age(dt_shocks, term_age_id, ext_age_ids)

  dt_shocks <- rbindlist(
    list(dt_shocks[age_group_id != term_age_id], dt_shocks_old),
    use.names = TRUE
  )

  # Extend to single year ages ----

  dt_shocks_single <- expand_age(dt_shocks, map_age)

  dt_shocks <- data.table::rbindlist(
    list(
      dt_shocks[!age_group_id %in% unique(map_age$input)],
      dt_shocks_single
    ),
    use.names = TRUE
  )

  return(dt_shocks)

}
