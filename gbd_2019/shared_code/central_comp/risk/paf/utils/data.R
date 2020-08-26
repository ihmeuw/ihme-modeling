# pull exposures
get_exp <- function(rei_id, location_id, year_id, sex_id, gbd_round_id,
                    decomp_step, n_draws) {
    exp <- get_draws(gbd_id_type = "rei_id", gbd_id = rei_id, location_id = location_id,
                     year_id = year_id, sex_id = sex_id, gbd_round_id = gbd_round_id,
                     decomp_step = decomp_step,
                     n_draws = n_draws, downsample = TRUE,
                     source = "exposure")
    if(!"parameter" %in% names(exp)) exp[, parameter := "continuous"]
    # dismod models have multiple measures but we just want prevalence/proportion
    if (rei_id %in% c(142)) exp <- exp[measure_id == 5]
    if (rei_id  == 341) exp <- exp[measure_id %in% c(5, 18), ]
    exp <- melt(exp, id.vars = c("age_group_id", "sex_id", "location_id", "year_id",
                                 "parameter"),
                measure.vars = paste0("draw_", 0:(n_draws - 1)),
                variable.name = "draw", value.name = "exp_mean")
    exp[, draw := as.numeric(gsub("draw_", "", draw))]
    if (rei_id == 341) { # adjust ikf to have a correct residual cat bc measures are different
        exp <- exp[parameter != "cat5", ]
        exp <- rbind(exp,
                     exp[, .(exp_mean = 1-sum(exp_mean), parameter = "cat5"),
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
get_tmrel <- function(rei_id, location_id, year_id, sex_id, gbd_round_id,
                      decomp_step, n_draws, tmrel_lower, tmrel_upper, ages) {
    if (length(location_id) > 1)
        stop("Can only pull TMRELs for one location at a time")
    if (inherits(try(tmrel <- get_draws(gbd_id_type = "rei_id", gbd_id = rei_id,
                                        location_id = location_id, year_id = year_id,
                                        sex_id = sex_id, gbd_round_id = gbd_round_id,
                                        decomp_step = decomp_step,
                                        n_draws = n_draws, downsample = TRUE,
                                        source = "tmrel"),
                     silent = TRUE), "try-error")) {
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

# pull RRs
get_rr <- function(rei_id, location_id, year_ids, sex_id, gbd_round_id,
                   decomp_step, n_draws) {
    if (length(location_id) > 1)
        stop("Can only pull RRs for one location at a time")
    rr <- get_draws(gbd_id_type = "rei_id", gbd_id = rei_id, location_id = location_id,
                    sex_id = sex_id, gbd_round_id = gbd_round_id,
                    decomp_step = decomp_step,
                    n_draws = n_draws, downsample = TRUE,
                    source = "rr")
    rr$location_id <- location_id
    rr <- melt(rr, id.vars = c("age_group_id", "sex_id", "location_id", "year_id",
                               "cause_id", "mortality", "morbidity", "parameter"),
               measure.vars = paste0("draw_", 0:(n_draws - 1)),
               variable.name = "draw", value.name = "rr")
    rr[, draw := as.numeric(gsub("draw_", "", draw))]
    if (length(unique(rr$parameter)) == 1 & !any(unique(rr$parameter) %like% "cat")) {
        rr[, parameter := "continuous"]
    }
    # most rrs don't vary by year so if user requests more years than are returned,
    # see if the rrs are constant over time and if so, expand. otherwise throw an error
    missing_yrs <- setdiff(year_ids, unique(rr$year_id))
    if (length(missing_yrs) > 0) {
        rr[, rr_mean := mean(rr), by = c("age_group_id", "sex_id", "location_id",
                                         "cause_id", "mortality", "morbidity", "parameter", "draw")]
        if(all(rr$rr==rr$rr_mean)) {
            rr <- rr[, .(year_id = year_ids), by =c("age_group_id", "sex_id", "location_id",
                                                    "cause_id", "mortality", "morbidity", "parameter", "draw", "rr")]
        } else {
            stop("RRs for ", paste(year_ids, collapse = ", "),
                 " were requested, but RRs for this risk vary across time and year(s) ",
                 paste(missing_yrs, collapse = ", "), " were not found. Re-upload your RRs and try again!")
        }
    }
    rr <- rr[year_id %in% year_ids, ]
    return(rr)
}

