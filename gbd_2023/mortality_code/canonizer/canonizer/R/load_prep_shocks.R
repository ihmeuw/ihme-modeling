#' Load and prep shock deaths
#'
#' Load all-cause shocks, convert to rates, extend terminal age, and
#' interpolate to single-year ages.
#'
#' @param loc Location ID to prep
#' @param load_years (Optional) integer vector of year IDs to load. By default
#'   all available years are returned. Year IDs not in `load_years` will be
#'   removed, but it is not guaranteed that all `load_years` will be present
#' @param pop Abridged population estimates
#' @param map_age Mapping of abridged (column name `input`) to single year LT
#'   age groups (column name `output`)
#' @param term_age_id (Optional) terminal age group of input shocks. Requires
#'   `ext_age_ids`
#' @param ext_age_ids (Optional) Age groups to extend terminal age of shocks to.
#'   Requires `term_age_id`
#' @param dir_input Input directory for shock files
#'
#' @details # Interpolation and Extension
#'
#' Shocks deaths are provided in the standard GBD age groups. In order to be
#' compatible with the canonical life table, values must be created for
#' single-year ages. Additionally, if both `term_age_id` and `ext_age_ids` are
#' supplied the shocks terminal age group will be extended to a higher age. The
#' method for creating these additional ages is to assume constant rates within
#' the less detailed age groups. If extension is performed, it happens before
#' interpolation to single year age groups.
#'
#' @return A table of single year shock rates, with most detailed under 1 ages
#'   and optionally extended up to a higher terminal age group.
#'
#' @export
load_prep_shocks <- function(loc,
                             load_years = NULL,
                             pop,
                             map_age,
                             term_age_id = NULL,
                             ext_age_ids = NULL,
                             dir_input) {

  id_cols <- c("location_id", "year_id", "sex_id", "age_group_id")
  stopifnot(
    all(c("input", "output") %in% names(map_age)),
    "Both `term_age_id` and `ext_age_id` must not be NULL for terminal age extension" =
      sum(c(is.null(term_age_id), is.null(ext_age_ids))) != 1,
    is.null(load_years) || is.integer(load_years)
  )

  extend_terminal_age <- !is.null(term_age_id) && !is.null(ext_age_ids)

  # Load and format ----

  dt_shocks <- fread(
    fs::path(dir_input, paste0("shocks_", loc, ".csv"))
  )[cause_id == 294, -"cause_id"]

  if (!is.null(load_years)) {
    dt_shocks <- dt_shocks[year_id %in% load_years]
  }

  dt_shocks <- melt(
    dt_shocks,
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

  if (extend_terminal_age) {

    dt_shocks_old <- replicate_age(dt_shocks, term_age_id, ext_age_ids)

    dt_shocks <- rbindlist(
      list(dt_shocks[age_group_id != term_age_id], dt_shocks_old),
      use.names = TRUE
    )

  }

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
