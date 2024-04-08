#' Summarize HIV adjustment measures over age
#'
#' Calculate HIV adjustment measures for aggregate age groupings under 5,
#' 5 to 14 years, 15 to 49 years, and 50+ years.
#'
#' @param dt Table with columns `id_cols` plus `value_cols` to summarize.
#' @param dt_pop Table of summary population values.
#' @param id_cols Columns in `dt` to group over, plus `age_start`.
#' @param value_cols Columns in `dt` to summarize.
#'
#' @return Table of HIV adjustment measures for aggregate age groups.
#'
#' @export
summarize_hiv_adjustment <- function(dt, dt_pop, id_cols, value_cols) {

  stopifnot(
    all(c(id_cols, value_cols) %in% names(dt)),
    all(setdiff(id_cols, "draw") %in% names(dt_pop))
  )

  dt_input_hiv <- dt[age_start <= 94, c(..id_cols, ..value_cols)]
  dt_input_hiv[dt_pop, pop := i.mean, on = setdiff(id_cols, "draw")]
  stopifnot(dt_input_hiv[is.na(pop), .N] == 0)

  # Make aggregate age groupings ----

  dt_input_hiv[age_start < 5, age_group_id := 1] # Under-5
  dt_input_hiv[age_start %between% c(5, 14), age_group_id := 23] # 5-14 years
  dt_input_hiv[age_start %between% c(15, 49), age_group_id := 24] # 15-49 years
  dt_input_hiv[age_start >= 50, age_group_id := 40] # 50+ years

  # Sum over aggregate ages ----

  dt_agg <- dt_input_hiv[
    j = lapply(.SD, \(x) sum(x * pop)),
    by = c(setdiff(id_cols, "age_start"), "age_group_id"),
    .SDcols = value_cols
  ]

  return(dt_agg)

}
