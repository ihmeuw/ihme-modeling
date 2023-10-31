#' Get lowest age before a decrease in qx
#'
#' @param dt_empir `data.table()` set of empirical life tables with qx.
#' @param id_vars columns identifying a unique life table in `dt_empir`.
#' @param age_threshold lowest age group start to begin checking for qx decreases.
#'
#' @return `data.table()` with `id_vars` for each life table with a drop in qx
#'   after the `age_threshold`, and an additional column of the lowest age start
#'   prior to a drop in qx.
get_min_qx_drop_age <- function(dt_empir, id_vars, age_threshold = 65) {

  setorderv(dt_empir, cols = c(id_vars, "age"))

  dt_empir[
    age > age_threshold,
    j = .(
      age,
      delta_qx = shift(qx, 1, type = "lead") - qx
    ),
    by = id_vars
  ][
    delta_qx < 0,
    j = .(
      min_age_qx_change_neg = min(age)
    ),
    by = id_vars
  ]

}
