source("./custom/metab_bmi_adult.R")
source("./custom/metab_fpg.R")

# pull exposures
get_exp <- function(rei_id, location_id, year_id, sex_id, gbd_round_id,
                    decomp_step, n_draws) {
    exp <- get_draws(gbd_id_type = "rei_id", gbd_id = rei_id,
                     location_id = location_id, year_id = year_id,
                     sex_id = sex_id, gbd_round_id = gbd_round_id,
                     decomp_step = decomp_step,
                     n_draws = n_draws, downsample = TRUE,
                     source = "exposure")
    if (!"parameter" %in% names(exp)) exp[, parameter := "continuous"]
    # drop resid category get draws adds for LBWSGA
    if (rei_id == 339) exp <- exp[parameter != "cat125", ]
    # dismod models have multiple measures but we just want prevalence/proportion
    if (rei_id  %in% c(96, 341)) exp <- exp[measure_id %in% c(5, 18), ]
    exp <- melt(exp, id.vars = c("age_group_id", "sex_id", "location_id", "year_id",
                                 "parameter"),
                measure.vars = paste0("draw_", 0:(n_draws - 1)),
                variable.name = "draw", value.name = "exp_mean")
    exp[, draw := as.numeric(gsub("draw_", "", draw))]
    if (rei_id == 341) {
        # adjust ikf to have a correct residual cat bc measures are different
        max_categ <- "cat5"
        exp <- exp[parameter != max_categ, ]
        exp <- rbind(exp,
                     exp[, .(exp_mean = 1-sum(exp_mean), parameter = max_categ),
                         by = c("age_group_id", "sex_id", "location_id", "year_id", "draw")])
    }
    return(exp)
}

# pull exposure sd
get_exp_sd <- function(rei_id, location_id, year_id, sex_id, gbd_round_id,
                       decomp_step, n_draws) {
    if (rei_id == 95) {
        exp_sd <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 10488,
                            measure_id = 19, location_id = location_id,
                            year_id = year_id, sex_id = sex_id, gbd_round_id = gbd_round_id,
                            decomp_step = decomp_step,
                            n_draws = n_draws, downsample = TRUE,
                            source = "epi")
    } else {
        exp_sd <- get_draws(gbd_id_type = "rei_id", gbd_id = rei_id,
                            location_id = location_id, year_id = year_id,
                            sex_id = sex_id, gbd_round_id = gbd_round_id,
                            decomp_step = decomp_step,
                            n_draws = n_draws, downsample = TRUE,
                            source = "exposure_sd")
    }
    exp_sd <- melt(exp_sd, id.vars = c("age_group_id", "sex_id", "location_id", "year_id"),
                   measure.vars = paste0("draw_", 0:(n_draws - 1)),
                   variable.name = "draw", value.name = "exp_sd")
    exp_sd[, draw := as.numeric(gsub("draw_", "", draw))]
    return(exp_sd)
}

# pull tmrels (draws or makes draws from upper and lower) most risks don't have
# draws, we generate them from and upper and lower using a uniform distribtion.
# try get_draws first, and generate if there's nothing returned
# NOTE -- only works for one location at a time, bc I fix the location id column
#   to match what's getting pulled so merges work later on.
get_tmrel <- function(rei_id, location_id, year_id, sex_id, gbd_round_id,
                      decomp_step, n_draws, tmrel_lower = NULL, tmrel_upper = NULL,
                      ages = NULL) {
    if (length(location_id) > 1) stop("Can only pull TMRELs for one location at a time")
    if (is.null(tmrel_lower) | is.null(tmrel_upper) | is.null(ages)) {
        tmrel <- get_draws(gbd_id_type = "rei_id", gbd_id = rei_id,
                           location_id = location_id, year_id = year_id,
                           sex_id = sex_id, gbd_round_id = gbd_round_id,
                           decomp_step = decomp_step, n_draws = n_draws,
                           downsample = TRUE, source = "tmrel")
    } else {
        tmrel <- expand.grid(location_id = location_id, year_id = year_id,
                             sex_id = sex_id, age_group_id = ages) %>% data.table
        tmrel[, paste0("draw_", 0:(n_draws - 1)) := as.list(runif(n_draws, tmrel_lower, tmrel_upper))]
    }
    tmrel$location_id <- location_id
    tmrel <- melt(tmrel, id.vars = c("age_group_id", "sex_id", "location_id", "year_id"),
                  measure.vars = paste0("draw_", 0:(n_draws - 1)),
                  variable.name = "draw", value.name = "tmrel")
    tmrel[, draw := as.numeric(gsub("draw_", "", draw))]
    return(tmrel)
}


get_exposure_and_tmrel <- function(
    rei_id, location_id, year_id, sex_id, gbd_round_id,
    decomp_step, n_draws, cont, has_tmrel_draws, tmrel_lower, tmrel_upper,
    update_bmi_fpg_exp_sd = FALSE
) {
    id_cols <- c("location_id", "year_id", "age_group_id", "sex_id", "draw")
    # Pull exposure and exposure SD if continuous
    dt <- get_exp(
        rei_id=rei_id, location_id=location_id, year_id=year_id, sex_id=sex_id,
        gbd_round_id=gbd_round_id, decomp_step=decomp_step, n_draws=n_draws
    )
    if (cont) {
        if (update_bmi_fpg_exp_sd & rei_id %in% c(370, 105)) {
            # BMI and FPG exposure SD calculation is central to the PAF code,
            # if requested, re-calculate using exposure and proportion model
            # rather than pulling current model marked best
            if (rei_id == 370) {
                dt <- calc_metab_bmi_adult_exp_sd(
                    exp_dt=dt, location_id=location_id, year_id=year_id, sex_id=sex_id,
                    gbd_round_id=gbd_round_id, decomp_step=decomp_step, n_draws=n_draws
                )
            } else if (rei_id == 105) {
                dt <- calc_metab_fpg_exp_sd(
                    exp_dt=dt, location_id=location_id, year_id=year_id, sex_id=sex_id,
                    gbd_round_id=gbd_round_id, decomp_step=decomp_step, n_draws=n_draws
                )
            }
        } else {
            exp_sd <- get_exp_sd(
                rei_id=rei_id, location_id=location_id, year_id=year_id, sex_id=sex_id,
                gbd_round_id=gbd_round_id, decomp_step=decomp_step, n_draws=n_draws
            )
            if (rei_id == 125) exp_sd[, exp_sd := exp_sd * .5]
            dt <- merge(dt, exp_sd, by = id_cols)
        }
    } else {
        # If sum of exposure categories != 1, this is an error
        dt[, exp_tot := sum(exp_mean), by = id_cols]
        tot_above_1 <- nrow(dt[round(exp_tot, digits=0) > 1])
        if (tot_above_1 != 0) {
            print(head(dt[round(exp_tot, digits=0) > 1][
                order(location_id, year_id, age_group_id, sex_id, draw, parameter)]))
            stop("Exposure categories sum to > 1")
        }
        dt[, exp_tot := NULL]
    }

    # Add TMREL
    if (cont) {
        # pull or generate if continuous
        if (has_tmrel_draws) {
            tmrel <- get_tmrel(
                rei_id=rei_id, location_id=location_id, year_id=year_id, sex_id=sex_id,
                gbd_round_id=gbd_round_id, decomp_step=decomp_step, n_draws=n_draws
            )
        } else {
            tmrel <- get_tmrel(
                rei_id=rei_id, location_id=location_id, year_id=year_id, sex_id=sex_id,
                gbd_round_id=gbd_round_id, decomp_step=decomp_step, n_draws=n_draws,
                tmrel_lower=tmrel_lower, tmrel_upper=tmrel_upper, ages=unique(dt$age_group_id)
            )
        }
        dt <- merge(dt, tmrel, by = id_cols)
    } else {
        # 0/1 if categorical
        max_categ <- mixedsort(unique(dt$parameter)) %>% tail(., n = 1)
        dt[, tmrel := ifelse(parameter == max_categ, 1, 0)]
    }

    if (exists("exp_sd")) rm(exp_sd)
    if (exists("tmrel")) rm(tmrel)
    return(dt)
}

# pull RRs
# NOTE
# -- only works for one location at a time, bc location_id is dropped so merges work later on.
# -- If long is TRUE, RR draws will be melted to long format by draw
get_rr <- function(rei_id, location_id, year_id, sex_id, gbd_round_id,
                   decomp_step, n_draws, cause_id=NULL, long=TRUE, version_id=NULL,
                   by_year_id=TRUE) {
    if (length(location_id) > 1) stop("Can only pull RRs for one location at a time")

    # pull draws and optionally filter by cause
    if (is.null(cause_id)) {
        gbd_id_type <- "rei_id"
        gbd_id <- rei_id
    } else {
        gbd_id_type <- c("rei_id", "cause_id")
        gbd_id <- c(rei_id, cause_id)
    }
    rr <- get_draws(gbd_id_type = gbd_id_type, gbd_id = gbd_id, location_id = location_id,
                    sex_id = sex_id, year_id = year_id, gbd_round_id = gbd_round_id,
                    decomp_step = decomp_step, version_id=version_id,
                    n_draws = n_draws, downsample = TRUE, source = "rr")
    rr[, location_id := NULL]
    # set parameter column where exposure is continuous
    rr_parameter <- unique(rr$parameter)
    if (length(rr_parameter) == 1 & !any(rr_parameter %like% "cat"))
        rr[, parameter := "continuous"]

    # convert RRs to long by draw
    if (long) rr <- rr_to_long(rr)
    # if RRs don't vary by year, drop year_id as a column
    if (!by_year_id) {
        if (length(year_id) != 1)
            stop("If RRs don't vary by year, only a single year should be pulled!")
        rr[, year_id := NULL]
    }

    return(rr)
}

# Return a df of RRs that are long by draw. Function is intended only for per unit
# log-linear and categorical RRs and will fail for exposure-dependent RRs.
rr_to_long <- function(rr) {
    if (!all(is.na(rr$exposure))) {
        stop("RR data is expected to be log-linear or categorical but ",
             "contains exposure values.")
    }
    id_cols <- c("age_group_id", "sex_id", "cause_id", "mortality", "morbidity", "parameter")
    if ("year_id" %in% names(rr)) id_cols <- c(id_cols, "year_id")
    rr <- melt(rr, id.vars = id_cols,
               measure.vars = paste0("draw_", 0:(n_draws - 1)),
               variable.name = "draw", value.name = "rr")
    rr[, draw := as.numeric(gsub("draw_", "", draw))]
    return(rr)
}
