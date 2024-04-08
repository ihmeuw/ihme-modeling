#' Set a floor on qx
#'
#' Given a threshold qx value, modify data in place to make qx at least the
#' minimum qx value in the data greater than the threshold value, by year, age,
#' and sex. mx and px are updated to stay consistent.
#'
#' @param dt table to update
#' @param thresh qx threshold
#' @param years years to update
#'
#' @return Updated `dt`, returned invisibly
#'
#' @export
set_qx_threshold <- function(dt, thresh = 1e-14, years = 2020:2022) {

  dt_threshold <- dt[
    qx > thresh & year_id %in% years,
    .(min_qx = min(qx)),
    by = .(year_id, age, sex_id)
  ]

  check <- data.table::fsetequal(
    unique(dt_threshold[, -"min_qx"]),
    data.table::CJ(year_id = years, age = unique(dt$age), sex_id = 1:2)
  )

  stopifnot("Missing required IDs" = check)

  dt[
    dt_threshold,
    `:=`(
      qx = i.min_qx,
      px = 1 - i.min_qx,
      mx = demCore::qx_ax_to_mx(i.min_qx, ax, age_length)
    ),
    on = .(year_id, age, sex_id, qx < min_qx)
  ]

  invisible(dt)

}
