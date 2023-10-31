#' Title
#'
#' @param exp_dt
#' @param location_id
#' @param year_id
#' @param sex_id
#' @param gbd_round_id
#' @param decomp_step
#' @param n_draws
#'
#' @return
calc_metab_bmi_adult_exp_sd <- function(exp_dt, location_id, year_id, sex_id,
                                        gbd_round_id, decomp_step, n_draws) {

    #--PULL OBESITY/OVERWEIGHT PREV--------------------------------------------
    ov_ob_mes <- c(24742, 24743)
    ov_ob <- get_draws(gbd_id_type = c("modelable_entity_id", "modelable_entity_id"), gbd_id = ov_ob_mes,
                       measure_id = 18, location_id = location_id, year_id = year_id,
                       sex_id = sex_id, gbd_round_id = gbd_round_id,
                       decomp_step = decomp_step, source = "epi", n_draws = n_draws,
                       downsample = TRUE)
    if (!all(year_id %in% unique(ov_ob$year_id))) {
        stop("Not all provided year_id(s) are present in obesity/overweight prevalence models ",
             "(modelable entity IDs ", paste(ov_ob_mes, collapse=", "), ")")
    }
    ov_ob <- melt(ov_ob, id.vars = c("modelable_entity_id", "age_group_id", "sex_id", "location_id", "year_id"),
                  measure.vars = paste0("draw_", 0:(n_draws - 1)),
                  variable.name = "draw", value.name = "prev")
    ov_ob[, draw := as.numeric(gsub("draw_", "", draw))]
    ov_ob <- dcast(ov_ob, age_group_id + sex_id + location_id + year_id + draw ~ modelable_entity_id, value.var = "prev")
    setnames(ov_ob, c("24742", "24743"), c("over_prev", "obes_prev"))
    dt <- merge(exp_dt, ov_ob, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))
    rm(ov_ob)

    #--CALC EXPOSURE SD----------------------------------------------------------
    source("./ensemble/edensity.R")
    Rcpp::sourceCpp("./custom/integ_bmi.cpp")

    wlist <- fread(paste0("FILEPATH/metab_bmi_adult.csv"))
    fit_bmi_ensemble <- function(b, over, obese, weights, mean) {
        tryCatch({
            fx <- NULL
            fx <- get_edensity(weights, mean, Vectorize(b), 10, 50)
            out <- NULL
            out <- integ_bmi(fx$x, fx$fx)
            ((out$over-over)^2 + (out$obese-obese)^2)
        }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
    }
    fit_bmi_ensemble <- cmpfun(fit_bmi_ensemble)
    calc_exp_sd <- function(i){
        weights <- wlist[age_group_id == dt[i, ]$age_group_id & sex_id == dt[i, ]$sex_id,
        ][, c("age_group_id", "sex_id", "year_id", "location_id") := NULL][1, ]
        optPARAMS=list()
        optVALS=list()
        for(p in seq(1,10,by=2)) {
            SOPT <- nlminb(start=dt[i, ]$exp_mean/p,objective = fit_bmi_ensemble,
                           over=dt[i, ]$over_prev, obese=dt[i, ]$obes_prev, weights=weights, mean=dt[i, ]$exp_mean,
                           lower=dt[i, ]$exp_mean*.01, upper=dt[i, ]$exp_mean*1.5,
                           control=list(iter.max=3, eval.max=3))
            optPARAMS = rbind(optPARAMS, SOPT$par)
            optVALS = rbind(optVALS, SOPT$objective)
        }
        optPARAMS[which.min(optVALS),][[1]]
    }
    calc_exp_sd <- compiler::cmpfun(calc_exp_sd)
    exp_sds <- mclapply(1:nrow(dt), calc_exp_sd, mc.cores = 6)
    dt <- cbind(dt, exp_sd = unlist(exp_sds))
    dt[, c("over_prev", "obes_prev") := NULL]
    rm(exp_sds)
    return(dt)
}
