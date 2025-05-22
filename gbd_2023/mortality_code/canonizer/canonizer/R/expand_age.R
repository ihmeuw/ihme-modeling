#' Expand age groups to more detailed ages
#'
#' Given a table with an age group column and a mapping of those age groups to
#' other age groups, replicate each subset of the table, giving the effect of
#' constant interpolation to more detailed age groups.
#'
#' @param dt table with `age_group_id` column
#' @param mapping table with columns `input` and `output`, defining the present
#'   ages and the desired output ages respectively
#'
#' @return table with `age_group_id`s for each age group in `mapping$output`,
#'   duplicates of the input age groups.
#'
#' @export
expand_age <- function(dt, mapping) {

  stopifnot(
    all(c("input", "output") %in% names(mapping)),
    all(unique(mapping$input) %in% unique(dt$age_group_id))
  )

  dt_expanded <-
    mapping[, c("input", "output")] |>
    split(by  = "input", keep.by = FALSE) |>
    lapply(\(dt) dt$output) |>
    purrr::imap(\(x, y) replicate_age(dt, as.integer(y), x)) |>
    data.table::rbindlist()

  stopifnot(all(mapping$output %in% unique(dt_expanded$age_group_id)))

  return(dt_expanded)

}
