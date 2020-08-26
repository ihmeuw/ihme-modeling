#' Smooth post-standard LT matching lifetables for selected countries
#'
#' To adjust for high-variance standard life table results over time, smooth the output qx values over years
#' (but within id_vars such as age, country, sex, etc.)
#'
#' @param dt data.table with variables: ihme_loc_id, year, sex, age, sim, stan_qx
#' @param id_vars character vector of id variables (should not include year), E.g. c("ihme_loc_id", "sex", "age", "sim")
#'
#' @return data.table with variables: ihme_loc_id, year, sex, age, sim, stan_qx
#'         Contains smoothed qx values, should be same nrows as dt
#' @export
#'
#' @examples
#' test <- CJ(ihme_loc_id = "USA", year = c(1950:2016), sex = c("male", "female"), age = c(0, 1, seq(5, 5, 110)), sim = 1:3)
#' test[, qx := (age/110) + 1950/year-.5]
#' results <- smooth_stan_lts(test, id_vars = c("ihme_loc_id", "sex", "age", "sim"))
#'
#' @import data.table
#' @import assertable

smooth_stan_lts <- function(stan_qx, id_vars) {
    if("year" %in% id_vars) stop("year must not be in id_vars")
    est_id_vars <- c(id_vars[id_vars != "age"], "year")
    
    collapse_stan_lts <- function(ref_year, dt, id_vars) {
        dt <- copy(dt)
        if(ref_year == min(dt$year)) {
            ref_dt <- dt[year < (ref_year + 10)]
        } else ref_dt <- dt[abs(year - ref_year) <= 6]

        ref_dt <- ref_dt[, list(stan_qx = mean(stan_qx)),
                         by = id_vars]
        ref_dt[, year := ref_year]
        return(ref_dt)
    }

    target_years <- min(stan_qx$year):max(stan_qx$year)
    smoothed_qx <- rbindlist(lapply(target_years, collapse_stan_lts, dt = stan_qx, id_vars = id_vars))
    if(nrow(smoothed_qx) != nrow(stan_qx)) stop(paste0("Issue: Smoothed qx has ", nrow(smoothed_qx), 
                                                       " rows but input qx has ", nrow(stan_qx), " rows"))

    setkeyv(smoothed_qx, est_id_vars)
    smoothed_qx[, px := 1-stan_qx]
    sq5 <- smoothed_qx[age <= 1, list(sq5 = 1 - prod(px)), by = est_id_vars]
    sq45 <- smoothed_qx[age >= 15 & age <= 55, list(sq45 = 1 - prod(px)), by = est_id_vars]
    smoothed_qx <- merge(smoothed_qx, sq5, by = est_id_vars)
    smoothed_qx <- merge(smoothed_qx, sq45, by = est_id_vars)

    assert_values(smoothed_qx[age <= 80,], "stan_qx", "not_na", quiet=T)
    return(smoothed_qx)
}
