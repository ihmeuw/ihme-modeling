#' Extend ax values for ages 75-105 using HMD regression parameters
#'
#' @param empir_lt data.table with columns: ihme_loc_id, sex, year, age (numeric), mx, ax
#' @param hmd_qx_results data.table with variables sex, start, par_cons, pari_logitqx60(5)95, par_Iage_65(5)100
#' @param by_vars character vector containing the variable names of all variables that uniquely identify the observations (except for age)
#'
#' @return returns empir_lt with the same variables, but modified values for ax for ages 75 and above
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable

extend_qx <- function(empir_lt, hmd_qx_results, by_vars) {

    empir_lt <- copy(empir_lt)

    ## remove any old age groups with qx > 1
    empir_lt[age >= 65 & qx > 1, age_qx_over_1 := min(age), by = by_vars]
    empir_lt[is.na(age_qx_over_1), age_qx_over_1 := 999]
    empir_lt[, min_age_qx_over_1 := min(age_qx_over_1, na.rm=T), by = by_vars]
    empir_lt <- empir_lt[age < min_age_qx_over_1]

    ## Drop old age groups after qx starts to decrease, if this happens
    empir_lt[age >= 65, qx_change := shift(qx, 1, type = "lead") - qx, by = by_vars]
    empir_lt[age >= 65 & qx_change < 0, min_age_qx_change_neg := min(age), by= by_vars]
    change_neg <- empir_lt[age == min_age_qx_change_neg, c("age", "min_age_qx_change_neg", by_vars), with = F]
    change_neg <- change_neg[,c("age") := NULL]
    empir_lt[, min_age_qx_change_neg := NULL]
    if(nrow(change_neg) > 0){
      empir_lt <- merge(empir_lt, change_neg, by = by_vars, all.x=T)
      empir_lt <- empir_lt[is.na(min_age_qx_change_neg) | age < min_age_qx_change_neg]
    }

    ## Start should now be the last age that you have data for
    empir_lt[, start_age := max(age), by = by_vars]

    ## Drop all data where the max age in the series is less than 65 years -- need those years to do the rest of the extension
    empir_lt <- empir_lt[start_age >= 65]

    ## Expand grid to get all the old ages
    baseline_grid <- unique(empir_lt[, .SD, .SDcols = c(by_vars, "start_age")], by = by_vars)
    baseline <- list()

    new_ages <- c(0, 1, seq(5, 105, 5))
    for(new_age in new_ages) {
        list_item <- paste0(new_age)
        baseline[[list_item]] <- copy(baseline_grid)
        baseline[[list_item]][, age := new_age]
    }

    baseline_grid <- rbindlist(baseline)

    ## Merge everything together to get consistent age-series throughout
    empir_lt <- merge(baseline_grid, empir_lt, by = c(by_vars, "age", "start_age"), all.x = T)

    ## Generate logit qx
    empir_lt[, logit_qx := logit(qx)]

    ## Generate base-qx as the start-age value of logit qx, within each by_var segment
    empir_lt[, base_logit_qx := logit_qx[start_age == age], by = by_vars]

    ## Format HMD regression parameters long instead of wide
    logit_vars <- colnames(hmd_qx_results)[grepl("logit", colnames(hmd_qx_results))]

    hmd_qx_results <- melt(hmd_qx_results, id = c("sex", "start", "par_cons", logit_vars))
    hmd_qx_results <- separate(hmd_qx_results, "variable", into = c("drop1", "drop2", "target_age"))
    setDT(hmd_qx_results)
    hmd_qx_results[, target_age := as.integer(target_age)]
    setnames(hmd_qx_results, c("value"), c("ind_age_par"))

    for(seq_age in seq(60, 100, 5)) {
        hmd_qx_results[start == seq_age, logit_qx_start_par := get(paste0("pari_logitqx", seq_age))]
    }

    hmd_qx_results[is.na(ind_age_par) & start == target_age, ind_age_par := 0]

    hmd_qx_results[, (c("drop1", "drop2", logit_vars)) := NULL]
    setnames(hmd_qx_results, c("start", "target_age"), c("start_age", "age"))

    empir_lt <- merge(empir_lt, hmd_qx_results, by = c("sex", "start_age", "age"), all.x = T)
    empir_lt[, pred_diff := par_cons + ind_age_par + logit_qx_start_par * base_logit_qx]

    empir_lt <- empir_lt[, .SD, .SDcols = c(by_vars, "age", "mx", "ax", "qx", "lx", "dx", "pred_diff", "start_age")]

    ## Generate base-qx as the start-age value of logit qx, within each by_var segment
    empir_lt[, temp_qx := qx[start_age == age], by = by_vars]
    empir_lt[age >= start_age, cum_pred_diff := cumsum(pred_diff), by = by_vars]

    setorderv(empir_lt, c(by_vars, "age"))

    ## The temporary qx is the logit of the baseline start-age qx plus the sum of the predicted difference variables
    empir_lt[, temp_qx := invlogit(logit(temp_qx) + shift(cum_pred_diff, type = "lag"))]

    ## Replace qx with temp_qx only if it's actually within the eligible places to tweak
    empir_lt[age > start_age, qx := temp_qx]
    empir_lt[, c("pred_diff", "temp_qx", "cum_pred_diff", "start_age") := NULL]

    ## Regenerate lx
    setkeyv(empir_lt, c(by_vars, "age"))
    qx_to_lx(empir_lt, assert_na = F)

    ## Format and output
    return(empir_lt)
}

