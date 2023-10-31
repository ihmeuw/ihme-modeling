# Create the exposure function and upper/lower bounds based on exposure distribution
create_exp_func <- function(i, dt, dist, dens=NULL) {
    if (dist == "ensemble") {
        exp_pdf <- approxfun(dens[[dt[i, ]$draw[1] + 1]]$x,
                             dens[[dt[i, ]$draw[1] + 1]]$fx,
                             yleft=0, yright=0)
        lower <- dens[[dt[i, ]$draw[1] + 1]]$XMIN
        upper <- dens[[dt[i, ]$draw[1] + 1]]$XMAX
    } else if (dist == "normal") {
        exp_pdf <- function(x) dnorm(x, dt[i, ]$exp_mean, dt[i, ]$exp_sd)
        lower <- 0
        upper <- qnorm(0.999999, dt[i, ]$exp_mean, dt[i, ]$exp_sd)
    } else if (dist == "lognormal") {
        exp_mean <- dt[i, ]$exp_mean
        exp_sd <- dt[i, ]$exp_sd
        mu <- log(exp_mean/sqrt(1 + (exp_sd^2/(exp_mean^2))))
        exp_sd <- sqrt(log(1 + (exp_sd^2/exp_mean^2)))
        exp_mean <- mu
        lower <- 0
        upper <- qlnorm(0.999999, exp_mean, exp_sd)
        # check to make sure the integrate function is not erroring
        # silently due to very small standard error
        i <- 1
        while (integrate(function(x) dlnorm(x, exp_mean, exp_sd), lower, upper,
                         stop.on.error = FALSE)$value < 0.99) {
            exp_sd <- exp_sd * 1.1
            print(paste("adding 10% buffer to exp_sd,",i,",",exp_sd))
            upper <- qlnorm(0.999999, exp_mean, exp_sd)
            i <- i+1
        }
        exp_pdf <- function(x) dlnorm(x, exp_mean, exp_sd)
    } else {
        stop("Distribution ", dist, " not currently implemented.")
    }
    return(list(exp_pdf=exp_pdf, lower=lower, upper=upper))
}

# Create approximation functions to interpolate RR as a function of exposure.
# This function expects a single demographic group. There will be one function per draw.
make_rr_funcs <- function(rr, demo_dt, demo_cols, n_draws, env=parent.frame()) {
    # get subset of RRs for this demographic group and sort by exposure
    rr_demo_cols <- intersect(names(rr), demo_cols)
    for (col in rr_demo_cols) {
        rr <- rr[get(col) == demo_dt[1, get(col)], ][order(exposure)]
    }
    for (draw in 0:(n_draws-1)) {
        assign(paste0("rr_", draw),
               approxfun(x=rr$exposure,
                         y=log(rr[[paste0("draw_", draw)]]),
                         rule=2,
                         ties="ordered"),
               envir=env)
    }
}

apply_mediation <- function(mediation, rr, cid, d) {
    mediation_factor <- mediation[cause_id == cid & draw == d, ]$mediation
    return((rr - 1) * (1 - mediation_factor) + 1)
}

# Create the relative risk function.
# For log-linear RRs, expects rr as a column in the dt.
create_rr_func <- function(i, dt, dist, exp_dependent, rr_env, cap, inv_exp, rr_scalar,
                           mediation=NULL) {
    if (exp_dependent) {
        # Exposure-dependent relative risk
        cause_id <- dt[i, ]$cause_id
        draw <- dt[i, ]$draw
        tmrel <- dt[i, ]$tmrel
        rr_pdf <- get(paste0("rr_", draw), envir=rr_env)
        rr_base_func <- function(x) {
            # if protective exposure is greater than tmrel or harmful exposure
            # is less than TMREL, find RR at TMREL instead
            if (inv_exp == 1) x[x > tmrel] <- tmrel
            if (inv_exp == 0) x[x < tmrel] <- tmrel
            return(exp(rr_pdf(x)))
        }
        if (is.null(mediation)) {
            rr_func <- rr_base_func
        } else {
            rr_func <- function(x) {
                return(apply_mediation(mediation=mediation,
                                       rr=rr_base_func(x),
                                       cid=cause_id,
                                       d=draw))
            }
        }
        return(rr_func)
    } else {
        # Log-linear relative risk
        rr_power <- function(exposure, exposure_cap, tmrel, inv_exp, rr_scalar) {
            if (inv_exp == 0) {
                return((((exposure - tmrel + abs(exposure - tmrel))/2) -
                            (exposure - exposure_cap + abs(exposure - exposure_cap))/2)/rr_scalar)
            } else if (inv_exp == 1) {
                return((((tmrel - exposure + abs(tmrel - exposure))/2) -
                            (exposure_cap - exposure + abs(exposure_cap - exposure))/2)/rr_scalar)
            }
        }
        if (dist == "ensemble") {
            exposure_cap <- cap[[dt[i, ]$draw[1] + 1]]
        } else {
            exposure_cap <- cap
        }
        rr_func <- function(x) {
            return(dt[i, ]$rr ^ rr_power(
                exposure=x,
                exposure_cap=exposure_cap,
                tmrel=dt[i, ]$tmrel,
                inv_exp=inv_exp,
                rr_scalar=rr_scalar))
        }
    }
    return(rr_func)
}

# Continuous distribution. This function handles ensemble, normal, and lognormal exposure
# distributions and both log-linear and exposure-dependent relative risks. The function
# deals with a single cause at a time and handles RRs differently based on their RRs type.
cont_paf <- function(dt, rr, dist, rr_type, wlist, rr_scalar, inv_exp, n_draws,
                     exp_percentiles, mediation=NULL) {
    if (length(unique(rr$cause_id)) > 1) {
        stop("Relative risk data supplied to cont_paf contains more than one cause")
    }
    all_rr_zero <- ifelse(all(rr[, paste0("draw_", 0:(n_draws - 1)), with = F] == 0), TRUE, FALSE)

    source("./ensemble/edensity.R")
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
        # apply mediation if present to RRs
        if (!is.null(mediation)) rr <- mediate_rr(rr, mediation)
        dt <- merge(dt, rr, by = c(rr_demo_cols, "parameter", "draw"))
    }
    # if all RR draws are 0, won't be able to integrate, set PAF manually to 0
    if (all_rr_zero) { dt[, paf := 0]; return(dt) }

    # Group dt rows by demographic
    demo_cols <- c("location_id", "year_id", "age_group_id", "sex_id",
                   "morbidity", "mortality")
    dt[, demo_id := .GRP, by=demo_cols]
    demo_ids <- unique(dt$demo_id)
    out <- vector("list", length(demo_ids))

    # Handle one demographic at a time
    for(demo in demo_ids) {
        demo_dt <- dt[demo_id == demo, ]

        lower <- exp_percentiles[age_group_id==demo_dt$age_group_id[1]]$lower
        upper <- exp_percentiles[age_group_id==demo_dt$age_group_id[1]]$upper

        if (dist == "ensemble") {
            # Build density given w=data.frame of weights, M = Mean, S=SD
            weights <- wlist[age_group_id == demo_dt$age_group_id[1] & sex_id == demo_dt$sex_id[1],
            ][, c("age_group_id", "sex_id", "year_id", "location_id") := NULL][1, ]
            dens <- mclapply(0:(n_draws-1), function(d)
                get_edensity(weights, demo_dt[draw == d, ]$exp_mean[1],
                             demo_dt[draw == d, ]$exp_sd[1],
                             .min = lower, .max = upper
                ), mc.cores = 6)
            if (any(lapply(dens, class) == "try-error"))
                stop("Exposure distribution (get_edensity) resulted in try-error below, ",
                     "please submit a ticket!\n", dens[[1]])
            cap <- mclapply(1:n_draws, function(d)
                if (inv_exp) dens[[d]]$XMIN else dens[[d]]$XMAX, mc.cores = 6)
        } else {
            dens <- NULL
            cap <- ifelse(inv_exp, lower, upper)
        }

        # For exposure-dependent RR, create approximation functions
        if (exp_dependent) make_rr_funcs(rr, demo_dt, demo_cols, n_draws)

        # Create the PAF function
        paf_func <- function(i, dist, dt, dens, cap, inv_exp, rr_scalar) {
            # Create the relative risk function
            rr_pdf <- create_rr_func(i, dt, dist, exp_dependent, environment(),
                                     cap, inv_exp, rr_scalar, mediation)
            tmrel_rr <- rr_pdf(dt[i, ]$tmrel)

            if (dist == "ensemble") {
                rr_fx <- rr_pdf(dens[[dt[i, ]$draw[1] + 1]]$x)
                if (all(rr_fx == tmrel_rr)) return(0)
                denom <- pracma::trapz(
                    dens[[dt[i, ]$draw[1] + 1]]$x,
                    dens[[dt[i, ]$draw[1] + 1]]$fx * rr_fx
                )
            } else {
                # Create the exposure function and upper/lower bounds for integral
                exp_params <- create_exp_func(i, dt, dist, dens)
                denom <- integrate(
                    function(x) {
                        return(exp_params$exp_pdf(x) * rr_pdf(x))
                    },
                    lower=exp_params$lower,
                    upper=exp_params$upper,
                    stop.on.error=FALSE)$value
            }
            if (denom == 0) return(0)
            return((denom - tmrel_rr) / denom)
        }
        paf_func <- compiler::cmpfun(paf_func)

        # Calculate PAFs
        pafs <- mclapply(1:nrow(demo_dt), paf_func, dist, demo_dt, dens, cap,
                         inv_exp, rr_scalar, mc.cores = 6)
        if (any(lapply(pafs, class) == "try-error"))
            stop("Calculating PAFs (paf_func) resulted in try-error below, please ",
                 "submit a ticket!\n", pafs[[1]])

        demo_dt <- cbind(demo_dt, paf = as.numeric(unlist(pafs)))
        out[[demo]] <- demo_dt
        rm(list=ls(pattern="^rr_[0-9]"))
        rm(demo_dt); rm(pafs)
        if (exists("weights")) rm(weights)
        if (exists("dens")) rm(dens)
        if (exists("cap")) rm(cap)
    }
    dt <- rbindlist(out, fill=T, use.names=T)
    return(dt)
}

# categorical PAF
categ_paf <- function(dt) {
    # expand mortality and morbidity
    dt <- rbind(dt[!(mortality == 1 & morbidity == 1), ],
                dt[mortality == 1 & morbidity == 1,
                   .(mortality = c(0, 1), morbidity = c(1, 0)),
                   by = c("location_id", "year_id", "age_group_id", "sex_id", "cause_id",
                          "parameter", "draw", "exp_mean", "rr", "tmrel")])
    # PAF = (SUM(exp * rr) - SUM(tmrel * rr)) / SUM(exp * rr)
    dt <- dt[, .(exp_mean=sum(exp_mean * rr), tmrel=sum(tmrel * rr)),
             by = c("location_id", "year_id", "age_group_id", "sex_id", "cause_id",
                    "mortality", "morbidity", "draw")]
    dt[, paf := (exp_mean - tmrel)/exp_mean]
    return(dt)
}
