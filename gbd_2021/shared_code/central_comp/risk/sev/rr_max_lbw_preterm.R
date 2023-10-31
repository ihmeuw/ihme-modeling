get_lbw_preterm_rr <- function(rei, gbd_round_id, decomp_step, n_draws) {
    source("FILEPATH/data.R")
    source("FILEPATH/db.R")

    # pull global exposure for year corresponding to GBD round
    global_yr <- query(paste0(
        "SELECT gbd_round FROM shared.gbd_round WHERE gbd_round_id = ", gbd_round_id), "shared")$gbd_round
    dt <- get_exp(rei_id = 339, location_id=1, year_id=global_yr, sex_id=c(1, 2),
                  gbd_round_id=gbd_round_id, decomp_step=decomp_step, n_draws=n_draws)
    setnames(dt, "exp_mean", "global_exp_mean")
    dt[, c("location_id", "year_id") := NULL]

    # pull RR for same location year, keep only mortality
    rr <- get_rr(rei_id=339, location_id=1, year_id=global_yr, sex_id=c(1, 2),
                 gbd_round_id=gbd_round_id, decomp_step=decomp_step,
                 n_draws=n_draws, by_year_id=FALSE)
    rr <- rr[mortality == 1, .(cause_id, age_group_id, sex_id, parameter, draw, rr)]
    if (!all(unique(rr$cause_id) == 1061)) {
        if (1061 %in% unique(rr$cause_id))
            stop(
                "LBW/SG outcomes (cause ID 1061) if present should be the only ",
                "cause in RRs. Other causes found: ", paste(unique(rr$cause_id), collapse = ", ")
            )
        # Prior to end of GBD 2020, RRs are the same across cause, set to Neonatal disorders cause
        rr[, cause_id := 380]
        rr <- unique(rr)
        if (any(duplicated(rr[, .(age_group_id, sex_id, parameter, draw)])))
            stop("LBW/SG RRs are not duplicates across cause as expected")
    }
    dt <- merge(dt, rr, by = c("age_group_id", "sex_id", "parameter", "draw"))

    # pull TMREL
    tmrel <- get_rei_mes(
        rei_id = 339, gbd_round_id = gbd_round_id, decomp_step = decomp_step
    )[draw_type=="exposure"]
    tmrel[, `:=` (preterm=as.numeric(tstrsplit(tstrsplit(modelable_entity_name, "- \\[")[[2]], ",")[[1]]),
                  lbw=as.numeric(tstrsplit(tstrsplit(modelable_entity_name, "wks, \\[")[[2]], ",")[[1]]))]
    tmrel <- tmrel[, .(parameter=exp_categ, preterm, lbw)]
    dt <- merge(dt, tmrel, by = "parameter")

    # rescale exposures to sum to 1
    residual_categ <- unique(dt[preterm == 40 & lbw == 4000]$parameter)
    dt <- dt[parameter != residual_categ]
    dt[, total_global_exp := sum(global_exp_mean), by = c("age_group_id", "sex_id", "cause_id", "draw")]
    dt[total_global_exp > 1, global_exp_mean := global_exp_mean/total_global_exp][, total_global_exp := NULL]
    dt <- rbind(dt[, .(cause_id, age_group_id, sex_id, parameter, draw, global_exp_mean, rr)],
                dt[, .(global_exp_mean = 1 - sum(global_exp_mean),
                       rr = 1, parameter = residual_categ),
                   by = c("cause_id", "age_group_id", "sex_id", "draw")])
    dt <- merge(dt, tmrel, by = "parameter")

    # calc lbw and preterm individual rrmax
    calc_child_rr_max <- function(child, child_name, gbd_round_id) {
        setkeyv(child, c("age_group_id", "sex_id", "cause_id", child_name, "draw"))
        # collapse exposures (sum) and rrs (weighted product by global exposure)
        # across opposite dimension
        child <- child[, .(global_exp_mean=sum(global_exp_mean),
                           rr = sum(rr*global_exp_mean/sum(global_exp_mean))),
                       by=c("age_group_id", "sex_id", "cause_id", child_name, "draw")]
        # TMREL is set to two bins. rescale all RR dividing by highest RR in TMREL
        child[, tmrel := 0]
        if (child_name == "preterm") {
            child[preterm == 38, tmrel := 1]
            child[preterm %in% c(38, 40), min_rr := max(rr),
                  by=c("age_group_id", "sex_id", "cause_id", "draw")]
        } else if (child_name == "lbw") {
            child[lbw == 3500, tmrel := 1]
            child[lbw %in% c(3500, 4000), min_rr := max(rr),
                  by=c("age_group_id", "sex_id", "cause_id", "draw")]
        }
        child[, min_rr := mean(min_rr, na.rm = T),
              by=c("age_group_id", "sex_id", "cause_id", "draw")]
        child[, rr := rr / min_rr][, min_rr := NULL]
        child[rr < 1, rr := 1]
        # determine Nth percentile
        exp_pctile <- ifelse(gbd_round_id < 7, 0.99, 0.95)
        child_mean <- child[, .(global_exp_mean=mean(global_exp_mean)), by=child_name]
        child_mean <- child_mean[order(-get(child_name))]
        child_mean[, cum_sum_exp := cumsum(global_exp_mean)]
        child_mean[cum_sum_exp >= exp_pctile, exp_max := max(get(child_name))]
        exp_max <- names(sort(table(child_mean$exp_max), decreasing = T, na.last = T))[1]
        child[, parameter := paste0("cat", get(child_name))][, c(child_name) := NULL]
        # keep only Nth and reformat wide by draw
        child <- child[parameter == paste0("cat", exp_max), ]
        child <- dcast(child, cause_id + age_group_id + sex_id ~ draw, value.var = "rr")
        setnames(child, paste(0:(n_draws-1)), paste0("draw_", 0:(n_draws-1)))
        return(list(dt=child, exp_max=as.integer(exp_max)))
    }
    preterm_rr_max <- calc_child_rr_max(dt, "preterm", gbd_round_id = gbd_round_id)
    lbw_rr_max <- calc_child_rr_max(dt, "lbw", gbd_round_id = gbd_round_id)
    dt <- dt[lbw == lbw_rr_max$exp_max & preterm == preterm_rr_max$exp_max, ]
    dt <- dcast(dt, cause_id + age_group_id + sex_id ~ draw, value.var = "rr")
    setnames(dt, paste(0:(n_draws-1)), paste0("draw_", 0:(n_draws-1)))
    if (rei == "nutrition_preterm") {
        return(preterm_rr_max$dt)
    } else if (rei == "nutrition_lbw") {
        return(lbw_rr_max$dt)
    } else if (rei == "nutrition_lbw_preterm") {
        return(dt)
    }
}
