#' Replicate age group
#'
#' Create duplicate rows of a table differing only in age groups given an input
#' age group to duplicate
#'
#' @param dt Table to duplicate
#' @param input_age_id Age group to duplicate
#' @param output_ages_ids Output age groups
#'
#' @return Table with duplicated age groups.
#'
#' @export
replicate_age <- function(dt, input_age_id, output_ages_ids) {

  stopifnot(
    "age_group_id" %in% names(dt),
    input_age_id %in% unique(dt$age_group_id)
  )

  dt_rep <-
    output_ages_ids |>
    rlang::set_names() |>
    lapply(\(x) dt[age_group_id == input_age_id, -"age_group_id"]) |>
    data.table::rbindlist(idcol = "age_group_id")

  dt_rep[, age_group_id := as.integer(age_group_id)]

  return(dt_rep)

}
