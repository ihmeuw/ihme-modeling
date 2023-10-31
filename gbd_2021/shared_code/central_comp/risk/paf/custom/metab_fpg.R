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
calc_metab_fpg_exp_sd <- function(exp_dt, location_id, year_id, sex_id,
                                  gbd_round_id, decomp_step, n_draws) {
    #--PULL DIABETES PREV-------------------------------------------------------
    diab_me <- 26931
    diab <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = diab_me,
                      measure_id = 5, location_id = location_id, year_id = year_id,
                      sex_id = sex_id, gbd_round_id = gbd_round_id,
                      decomp_step = decomp_step, source = "epi",
                      n_draws = n_draws, downsample = TRUE)
    if (!all(year_id %in% unique(diab$year_id))) {
        stop("Not all provided year_id(s) are present in diabetes prevalence model ",
             "(modelable entity ID ", diab_me, ")")
    }
    diab <- melt(diab, id.vars = c("age_group_id", "sex_id", "location_id", "year_id"),
                 measure.vars = paste0("draw_", 0:(n_draws - 1)),
                 variable.name = "draw", value.name = "diab_prev")
    diab[, draw := as.numeric(gsub("draw_", "", draw))]
    dt <- merge(exp_dt, diab, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))

    # fpg data came from people w/o diabetes, regression of mean fpg in all pop against
    # pop that doesn't have diabetes to shift mean fpg exposure
    load("./custom/fpg_diab_correction.rda")
    draws_fpg_fe <- rmvnorm(n=n_draws, reg_fpg$coefficients, vcov(reg_fpg))
    draws_fpg_fe <- draws_fpg_fe %>% data.table
    draws_fpg_fe[, draw := .I-1]
    setnames(draws_fpg_fe, "(Intercept)", "intercept")
    dt <- merge(dt, draws_fpg_fe, by = "draw")
    dt[, exp_mean := exp(intercept + log_fpg_non_diab*log(exp_mean) + prev_diab*diab_prev)]
    dt[, c("intercept", "log_fpg_non_diab", "prev_diab") := NULL]

    #--CALC EXPOSURE SD---------------------------------------------------------
    source("./ensemble/edensity.R")
    Rcpp::sourceCpp("./custom/integ_fpg.cpp")

    wlist <- fread(paste0("/FILEPATH/metab_fpg.csv"))
    fit_fpg_ensemble <- function(b, diab, MEAN, WEIGHTS) {
        tryCatch({
            fx <- NULL
            fx <- get_edensity(WEIGHTS, MEAN, b, .min=2.5, .max=30)
            out <- NULL
            out <- integ_fpg(fx$x, fx$fx)
            ((diab-out$diabetic)^2)
        }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
    }
    fit_fpg_ensemble <- cmpfun(fit_fpg_ensemble)
    calc_exp_sd <- function(i){
        weights <- wlist[age_group_id == dt[i, ]$age_group_id & sex_id == dt[i, ]$sex_id,
        ][, c("age_group_id", "sex_id", "year_id", "location_id") := NULL][1, ]
        optPARAMS=list()
        optVALS=list()
        for(p in seq(1,10,by=2)) {
            SOPT <-stats::nlminb(start=dt[i, ]$exp_mean/p, objective = fit_fpg_ensemble,
                                 diab=dt[i, ]$diab_prev, WEIGHTS=weights, MEAN=dt[i, ]$exp_mean,
                                 lower=dt[i, ]$exp_mean*.01, upper=dt[i, ]$exp_mean*1.5,
                                 control=list(iter.max=3,eval.max=3))
            optPARAMS = rbind(optPARAMS, SOPT$par)
            optVALS = rbind(optVALS, SOPT$objective)
        }
        optPARAMS[which.min(optVALS),][[1]]
    }
    calc_exp_sd <- compiler::cmpfun(calc_exp_sd)
    exp_sds <- mclapply(1:nrow(dt), calc_exp_sd, mc.cores = 6)
    dt <- cbind(dt, exp_sd = unlist(exp_sds))
    dt[, diab_prev := NULL]
    rm(exp_sds)
    return(dt)
}
