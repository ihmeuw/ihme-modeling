#' Collapse Deaths and Population to Common Terminal Ages
#'
#' For the deaths and population data from DDM that the ELT processes uses as
#' input, there are cases where the terminal deaths and population age groups
#' don't match up for a given source-year. This function resolves this by
#' collapsing data for these source-years to a lower common terminal age group.
#'
#' @param dt `data.table` of DDM input deaths and VR prepped in `01_elt_setup.R`
#'   and formatted with separate columns for `age`, `age_end`, `deaths`,
#'   `measure`, and other source-year identification colums. The `measure`
#'   column must contain only "deaths" or "population".
#' @param id_cols Column names of `dt` used to uniquely identify a source-year
#'
#' @return A data.table with new collapsed age group ranges for source-years
#'   with originally mismatched terminal ages.
#' @export
#'
#' @import data.table
#'
#' @examples
collapse_terminal_ages <- function(dt, id_cols) {

  setkeyv(dt, c(id_cols, "measure"))

  # Subset to all present terminal values
  dt_terminal <- dt[
    !is.na(value) & is.na(age_end),
    .(terminal_ages = list(age)),
    by = c(id_cols, "measure")
    ]

  dt_terminal_wide <- dcast(dt_terminal, ...~measure, value.var = "terminal_ages")

  # Get minimum terminal age between deaths and population for each source year
  # where there isn't a common terminal age
  uncommon_terminal <- dt_terminal_wide[purrr::map2_lgl(deaths, population, ~!any(intersect(.x, .y)))]
  uncommon_terminal[, collapse_terminal_age := purrr::map2_int(deaths, population, ~min(union(.x, .y)))]

  # Determine which source-year measures need to be collapsed to a lower
  # terminal age
  collapse_sy_measure <- melt(
    uncommon_terminal,
    measure.vars = c("deaths", "population"),
    variable.name = "measure",
    variable.factor = FALSE,
    value.name = "age"
  )

  collapse_sy_measure <- collapse_sy_measure[
    purrr::map2_lgl(collapse_terminal_age, age, ~!any(intersect(.x, .y))),
    -"age"
    ]

  setkeyv(collapse_sy_measure, c(id_cols, "measure"))

  # Collapse values to new terminal age
  new_terminal_values <- dt[collapse_sy_measure][
    age >= collapse_terminal_age & !is.na(value),
    .(value = sum(value), age = min(age), age_end = NA),
    by = c(id_cols, "measure")
    ]

  return(new_terminal_values)

}
