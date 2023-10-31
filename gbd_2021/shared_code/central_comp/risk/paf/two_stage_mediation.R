adjust_salt_sbp_delta <- function(dt, gbd_round_id, decomp_step, n_draws) {
    # calculate prevalence of hypertension
    source("./ensemble/edensity.R")
    sbp_weights <- fread("FILEPATH/_weights/metab_sbp.csv")
    calc_prev <- function(i) {
        weights <- sbp_weights[age_group_id == dt[i, ]$age_group_id & sex_id == dt[i, ]$sex_id,
        ][, c("age_group_id", "sex_id", "year_id", "location_id") := NULL][1, ]
        fx <- get_edensity(weights, dt[i, ]$med_exp_mean, dt[i, ]$med_exp_sd)
        dens <- approxfun(fx$x, fx$fx, yleft=0, yright=0)
        total_integ <- integrate(dens, fx$XMIN, fx$XMAX)$value
        prev <- integrate(dens, 140, fx$XMAX)$value/total_integ
        return(prev)
    }
    calc_prevc <- compiler::cmpfun(calc_prev)
    prevs <- mclapply(1:nrow(dt), calc_prevc, mc.cores = 6)
    dt <- cbind(dt, hyper_prev = unlist(prevs))

    # read flat file shifts by age and super region
    super_region <- get_location_metadata(
        location_set_id = 35, gbd_round_id = gbd_round_id, decomp_step = decomp_step
    )[location_id == unique(dt$location_id), ]$super_region_id
    sbp_shift <- fread(
        "FILEPATH/diet_salt_rr_shift.csv")[super_region_id == super_region, ]
    sbp_shift <- melt(sbp_shift, id.vars = c("age_group_id", "sbp_shift"),
                      measure.vars = paste0("draw_", 0:(n_draws - 1)),
                      variable.name = "draw", value.name = "shift")
    sbp_shift[, draw := as.numeric(gsub("draw_", "", draw))]
    sbp_shift <- dcast(sbp_shift, age_group_id + draw ~ sbp_shift, value.var = "shift")

    # shift = prev * > 140 + (1 - prev) * < 140
    dt <- merge(dt, sbp_shift, by = c("age_group_id", "draw"))
    dt[, delta := (hyper_prev * more140 + (1 - hyper_prev) * less140) / 10 / 2.299]
    dt[, `:=` (hyper_prev=NULL, more140=NULL, less140=NULL)]
    return(dt)
}


# Continuous distribution. This function handles ensemble, normal, and lognormal exposure
# distributions and both log-linear and exposure-dependent relative risks. The function
# deals with a single cause at a time and handles RRs differently based on their RRs type.
two_stage_cont_paf <- function(dt, n_draws, rr, rr_type, rr_scalar,
                               distal_dist, distal_wlist, distal_exp_percentiles,
                               med_dist, med_wlist, med_exp_percentiles,
                               med_inv_exp, distal_inv_exp, absolute_risk_curve = FALSE) {
    if (length(unique(rr$cause_id)) > 1) stop("Relative risk data contains more than one cause")
    all_rr_zero <- ifelse(all(rr[, paste0("draw_", 0:(n_draws - 1)), with = F] == 0), TRUE, FALSE)

    exp_dependent <- ifelse(rr_type == 3, TRUE, FALSE)
    rr_demo_cols <- c("age_group_id", "sex_id")
    if ("year_id" %in% names(rr)) rr_demo_cols <- c(rr_demo_cols, "year_id")
    if (exp_dependent) {
        # expand by cause, morbidity, mortality
        dt <- merge(
            dt, unique(rr[, c("cause_id", "morbidity", "mortality", rr_demo_cols), with=FALSE]),
            by=rr_demo_cols, allow.cartesian=TRUE)
    } else {
        # log-linear per-unit RRs: convert to long if not already, merge
        if (!"draw" %in% colnames(rr)) rr <- rr_to_long(rr)
        dt <- merge(dt, rr, by = c(rr_demo_cols, "parameter", "draw"))
    }
    # if all RR draws are 0, won't be able to integrate, set PAF manually to 0
    if (all_rr_zero) { dt[, paf := 0]; return(dt) }

    # Group rows by demographic and loop over one demographic at a time
    demo_cols <- c("location_id", "year_id", "age_group_id", "sex_id", "morbidity", "mortality")
    dt[, demo_id := .GRP, by=demo_cols]
    demo_ids <- unique(dt$demo_id)
    out <- vector("list", length(demo_ids))

    source("./ensemble/edensity.R")
    for(demo in demo_ids) {
        demo_dt <- dt[demo_id == demo, ]

        # for exposure-dependent RR, make RR functions
        if (exp_dependent) make_rr_funcs(rr, demo_dt, demo_cols, n_draws)

        # make exposure density functions
        distal_lower <- distal_exp_percentiles[age_group_id==demo_dt$age_group_id[1]]$lower
        distal_upper <- distal_exp_percentiles[age_group_id==demo_dt$age_group_id[1]]$upper
        med_lower <- med_exp_percentiles[age_group_id==demo_dt$age_group_id[1]]$lower
        med_upper <- med_exp_percentiles[age_group_id==demo_dt$age_group_id[1]]$upper
        if (distal_dist == "ensemble") {
            # Build density given w=data.frame of weights, M = Mean, S=SD
            weights <- distal_wlist[age_group_id == demo_dt$age_group_id[1] & sex_id == demo_dt$sex_id[1],
            ][, c("age_group_id", "sex_id", "year_id", "location_id") := NULL][1, ]
            distal_dens <- mclapply(0:(n_draws-1), function(d)
                get_edensity(weights=weights, mean=demo_dt[draw == d, ]$distal_exp_mean[1],
                             sd=demo_dt[draw == d, ]$distal_exp_sd[1],
                             .min = distal_lower, .max = distal_upper
                ), mc.cores = 6)
            if (any(lapply(distal_dens, class) == "try-error"))
                stop("Exposure distribution (get_edensity) resulted in try-error below, ",
                     "please submit a ticket!\n", distal_dens[[1]])
        } else {
            distal_dens <- NULL
        }
        if (med_dist == "ensemble") {
            # Build density given w=data.frame of weights, M = Mean, S=SD
            weights <- med_wlist[age_group_id == demo_dt$age_group_id[1] & sex_id == demo_dt$sex_id[1],
            ][, c("age_group_id", "sex_id", "year_id", "location_id") := NULL][1, ]
            med_dens <- mclapply(0:(n_draws-1), function(d)
                get_edensity(weights=weights, mean=demo_dt[draw == d, ]$med_exp_mean[1],
                             sd=demo_dt[draw == d, ]$med_exp_sd[1],
                             .min = med_lower, .max = med_upper
                ), mc.cores = 6)
            if (any(lapply(med_dens, class) == "try-error"))
                stop("Exposure distribution (get_edensity) resulted in try-error below, ",
                     "please submit a ticket!\n", med_dens[[1]])
            med_cap <- mclapply(1:n_draws, function(d)
                if (med_inv_exp) med_dens[[d]]$XMIN else med_dens[[d]]$XMAX, mc.cores = 6)
        } else {
            med_dens <- NULL
            med_cap <- ifelse(med_inv_exp, med_lower, med_upper)
        }

        # create the PAF function
        paf_func <- function(i, dist, med_dist, dt, dens, med_dens, med_cap,
                             inv_exp, rr_scalar, distal_inv_exp, absolute_risk_curve) {
            # split up the dt columns for creating rr and exposure functions
            value_cols <- c("exp_mean", "exp_sd", "tmrel")
            distal_dt <- copy(dt[, -c(paste0("med_", ..value_cols))])
            setnames(distal_dt, paste0("distal_", value_cols), value_cols)
            med_dt <- copy(dt[, -c(paste0("distal_", ..value_cols))])
            setnames(med_dt, paste0("med_", value_cols), value_cols)

            # create the relative risk function
            rr_pdf <- create_rr_func(
                i=i, dt=med_dt, dist=med_dist, exp_dependent=exp_dependent, rr_env=environment(),
                cap=med_cap, inv_exp=inv_exp, rr_scalar=rr_scalar
            )
            calc_rr_ac_b <- function(a, rr_bc, delta, pb, a0, distal_inv_exp,
                                     absolute_risk_curve) {
                if (distal_inv_exp) {
                    delta_term <- delta * min(a - a0, 0.0)
                } else {
                    delta_term <- delta * max(a + a0, 0.0)
                }
                # if we have an absolute risk curve for RRbc, there likely will
                # be zeros (zero prevalence when exposure below definitional threshold),
                # so integrate the numerator and denominator separately
                if (absolute_risk_curve) {
                    return(pracma::trapz(pb$x, rr_bc(pb$x + delta_term) * pb$fx) /
                               pracma::trapz(pb$x, rr_bc(pb$x) * pb$fx)
                    )
                } else {
                    return(pracma::trapz(pb$x, rr_bc(pb$x + delta_term) * pb$fx / rr_bc(pb$x)))
                }
            }
            rr_ac_b <- sapply(distal_dens[[dt[i, ]$draw[1] + 1]]$x, calc_rr_ac_b,
                              rr_bc = rr_pdf,
                              delta = distal_dt[i, ]$delta,
                              pb = med_dens[[dt[i, ]$draw[1] + 1]],
                              a0 = distal_dt[i, ]$tmrel,
                              distal_inv_exp = distal_inv_exp,
                              absolute_risk_curve = absolute_risk_curve
            )
            denom <- pracma::trapz(distal_dens[[dt[i, ]$draw[1] + 1]]$x,
                                   rr_ac_b * distal_dens[[dt[i, ]$draw[1] + 1]]$fx)
            if (denom == 0) return(0)
            return(1 - 1 / denom)
        }
        paf_func <- compiler::cmpfun(paf_func)

        # Calculate PAFs
        pafs <- mclapply(1:nrow(demo_dt), paf_func, dist, med_dist, demo_dt, dens,
                         med_dens, med_cap, med_inv_exp, rr_scalar, distal_inv_exp,
                         absolute_risk_curve, mc.cores = 6)
        pafs <- unlist(pafs)
        if (any(lapply(pafs, class) == "try-error"))
            message("Calculating PAFs (paf_func) resulted in try-error below, please ",
                    "submit a ticket!\n", pafs[class(pafs) == "try-error"][1])

        demo_dt <- cbind(demo_dt, paf = as.numeric(pafs))
        out[[demo]] <- demo_dt
        rm(list=ls(pattern="^rr_[0-9]"))
        rm(demo_dt); rm(pafs)
    }
    dt <- rbindlist(out, fill=T, use.names=T)
    return(dt)
}
