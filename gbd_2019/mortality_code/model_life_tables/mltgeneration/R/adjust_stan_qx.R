#' Apply logit regression coefficients to logit post-standard LT qx values (and standard 45q15 and 5q0),
#' along with input target (HIV-free, except for ZAF which is with-HIV) logit 45q15/5q0 to generate adjusted age-specific values
#'
#' Beta1x  and Beta2x are coefficients that vary by age x and which measure the impact of differences in child
#' and adult mortality rates between a target life table and the standard life table on the estimated age pattern of mortality.
#' In other words, both coefficients determine how much the estimated age pattern of mortality deviates from the standard by age and from linearity.
#'
#' @param qx_stan data.table with the variables ihme_loc_id, sex, year, sim, age, stan_qx, sq5, sq45.
#'         Results of calc_stan_lt to generate collapsed standard lifetables
#' @param qx_target data.table with variables ihme_loc_id, year, sex, sim, target_45q15, target_5q0.
#'         Original target qx values, used as inputs into calc_stan_lt
#' @param modelpar_sims data.table with variables sex, age, sim, sim_difflogit5, sim_difflogit45.
#'          Regression coefficients based on logit differences between standard LT and collapsed comparator LTs
#' @param id_vars character vector of id variables (e.g. ihme_loc_id, sex, year, sim)
#'
#' @export
#' @import data.table
#' @import assertable

adjust_stan_qx <- function(qx_stan, qx_target, modelpar_sims, id_vars) {
    est_id_vars <- c(id_vars, "age")

    ## Merge in the standard and calculated HIV-free qx
    stan_rows <- nrow(qx_stan)
    dt <- merge(qx_stan, qx_target, by = id_vars, all = T)
    if(nrow(dt) != stan_rows) stop("Merging standard qx and HIV-free qx resulted in different row numbers")

    ## Convert all columns to logit-space for application of betas
    value_cols <- c("stan_qx", "sq5", "sq45", "target_45q15", "target_5q0")
    dt[, (value_cols) := lapply(.SD, logit_qx), .SDcols = value_cols]

    ## Apply betas to all draws
    dt <- merge(dt, modelpar_sims, by = c("sex", "age", "sim"), all = T)
    if(nrow(dt) != stan_rows) stop("Merging standard qx and model parameter sims resulted in different row numbers")
    dt[, stan_qx := stan_qx + sim_difflogit5 * (target_5q0 - sq5) + sim_difflogit45 * (target_45q15 - sq45)]

    ## Convert qx back to real-space
    inv_value_cols <- c("stan_qx")
    dt[, (inv_value_cols) := lapply(.SD, invlogit_qx), .SDcols = inv_value_cols]

    ## Create 5q0 and 45q15 for the adjusted lifetable
    dt <- dt[, .SD, .SDcols = c(est_id_vars, "stan_qx")]
    setkeyv(dt, id_vars)
    dt[, px := 1-stan_qx]
    sq5 <- dt[age <= 1, list(sq5 = 1 - prod(px)), by = id_vars]
    sq45 <- dt[age >= 15 & age <= 55, list(sq45 = 1 - prod(px)), by = id_vars]

    dt <- merge(dt, sq5, by = id_vars)
    dt <- merge(dt, sq45, by = id_vars)

    ## Format, assert, and output dataset
    assert_values(dt[age <= 80], "stan_qx", "not_na", quiet=T)

    return(dt)
}
