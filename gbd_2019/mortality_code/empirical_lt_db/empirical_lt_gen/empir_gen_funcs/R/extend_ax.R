#' Extend ax values for ages 75-105 using HMD regression parameters
#'
#' @param empir_lt data.table with columns: ihme_loc_id, sex, year, age (numeric), mx, ax
#' @param hmd_ax_results data.table with variables par_mx, par_smx, par_con, sex, age
#'
#' @return returns empir_lt with the same variables, but modified values for ax for ages 75 and above
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable

extend_ax <- function(empir_lt, hmd_ax_results) {
    empir_lt <- copy(empir_lt)
    empir_lt <- merge(empir_lt, hmd_ax_results, by = c("sex", "age"), all.x = T)
    empir_lt[!is.na(par_mx), ax := par_mx * mx + par_smx * (mx ^ 2) + par_con]
    empir_lt[, c("par_mx", "par_smx", "par_con") := NULL]
    return(empir_lt)
}
