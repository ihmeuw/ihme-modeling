# continous distribtion (normal or lognormal) with no cap
cont_paf_nocap <- function(dt, lower, upper, rr_scalar, dist, inv_exp) {
    calc_paf <- function(lower, upper, exp_mean, exp_sd, rr, tmrel, rr_scalar,
                         dist, inv_exp) {
        if (dist == "normal") {
            bound <- qnorm(0.999999, exp_mean, exp_sd)
            if (inv_exp == 0) {
                denom <- integrate(function(x) dnorm(x, exp_mean, exp_sd) * rr^((x - tmrel + abs(x - tmrel))/2/rr_scalar),
                                   lower, bound, stop.on.error = FALSE)$value
            } else if (inv_exp == 1) {
                denom <- integrate(function(x) dnorm(x, exp_mean, exp_sd) * rr^((tmrel - x + abs(tmrel - x))/2/rr_scalar),
                                   lower, bound, stop.on.error = FALSE)$value
            }
        } else if (dist == "lognormal") {
            mu <- log(exp_mean/sqrt(1 + (exp_sd^2/(exp_mean^2))))
            exp_sd <- sqrt(log(1 + (exp_sd^2/exp_mean^2)))
            exp_mean <- mu
            bound <- qlnorm(0.999999, exp_mean, exp_sd)
            if (inv_exp == 0) {
                denom <- integrate(function(x) dlnorm(x, exp_mean, exp_sd) * rr^((x - tmrel + abs(x - tmrel))/2/rr_scalar),
                                   lower, bound, stop.on.error = FALSE)$value
            } else if (inv_exp == 1) {
                denom <- integrate(function(x) dlnorm(x, exp_mean, exp_sd) * rr^((tmrel - x + abs(tmrel - x))/2/rr_scalar),
                                   lower, bound, stop.on.error = FALSE)$value
            }
        } else {
            stop("Distribution ", dist, " not currently implemented.")
        }
        return((denom-1)/denom)
    }
    dt[, paf := {
        if (inherits(try(ans <- calc_paf(lower=lower,
                                         upper=upper,
                                         exp_mean=exp_mean,
                                         exp_sd=exp_sd,
                                         rr=rr,
                                         tmrel=tmrel,
                                         rr_scalar=rr_scalar,
                                         dist=dist,
                                         inv_exp=inv_exp),silent=TRUE),"try-error"))
            as.numeric(NA)
        else
            ans
    }, by=1:nrow(dt)]
    return(dt)
}

# continous distribtion (normal or lognormal) with cap
cont_paf_cap <- function(dt, lower, upper, rr_scalar, dist, inv_exp, cap) {
    calc_paf <- function(lower, upper, exp_mean, exp_sd, rr, tmrel, rr_scalar,
                         dist, inv_exp, cap) {
        if (dist == "normal") {
            bound <- qnorm(0.999999, exp_mean, exp_sd)
            if (inv_exp == 0) {
                denom <- integrate(function(x) dnorm(x, exp_mean, exp_sd) * rr^((((x - tmrel + abs(x - tmrel))/2) - ((x - cap) + abs(x - cap))/2)/rr_scalar),
                                   lower, bound, stop.on.error = FALSE)$value
            } else if (inv_exp == 1) {
                denom <- integrate(function(x) dnorm(x, exp_mean, exp_sd) * rr^((((tmrel - x + abs(tmrel - x))/2) - ((cap - x) + abs(cap - x))/2)/rr_scalar),
                                   lower, bound, stop.on.error = FALSE)$value
            }
        } else if (dist == "lognormal") {
            mu <- log(exp_mean/sqrt(1 + (exp_sd^2/(exp_mean^2))))
            exp_sd <- sqrt(log(1 + (exp_sd^2/exp_mean^2)))
            exp_mean <- mu
            bound <- qlnorm(0.999999, exp_mean, exp_sd)
            i <- 1
            while (integrate(function(x) dlnorm(x, exp_mean, exp_sd), lower, bound, stop.on.error = FALSE)$value < 0.99){
                exp_sd <- exp_sd * 1.1
                print(paste("adding 10% buffer to exp_sd,",i,",",exp_sd))
                bound <- qlnorm(0.999999, exp_mean, exp_sd)
                i <- i+1
            }
            if (inv_exp == 0) {
                denom <- integrate(function(x) dlnorm(x, exp_mean, exp_sd) * rr^((((x - tmrel + abs(x - tmrel))/2) - ((x - cap) + abs(x - cap))/2)/rr_scalar),
                                   lower, bound, stop.on.error = FALSE)$value
            } else if (inv_exp == 1) {
                denom <- integrate(function(x) dlnorm(x, exp_mean, exp_sd) * rr^((((tmrel - x + abs(tmrel - x))/2) - ((cap - x) + abs(cap - x))/2)/rr_scalar),
                                   lower, bound, stop.on.error = FALSE)$value
            }
        } else {
            stop("Distribution ", dist, " not currently implemented.")
        }
        return((denom-1)/denom)
    }
    dt[,paf := {
        if (inherits(try(ans <- calc_paf(lower=lower,
                                         upper=upper,
                                         exp_mean=exp_mean,
                                         exp_sd=exp_sd,
                                         rr =rr,
                                         tmrel=tmrel,
                                         rr_scalar=rr_scalar,
                                         dist =dist,
                                         inv_exp=inv_exp,
                                         cap=cap),silent=TRUE),"try-error"))
            as.numeric(NA)
        else
            ans
    }, by = 1:nrow(dt)]
    return(dt)
}

# continous distribtion (ensemble) with cap
cont_paf_ensemble <- function(dt, wlist, rr_scalar, inv_exp, n_draws, use_cpp=TRUE) {
    source("./ensemble/edensity.R")
    if (use_cpp) {
      # load ensemble PAF function from C++
      Rcpp::sourceCpp("./ensemble/ensemble.cpp")
      calc_paf <- function(i) {
        tryCatch({
          ensemble(dens[[demo_dt[i, ]$draw[1] + 1]]$x, dens[[demo_dt[i, ]$draw[1] + 1]]$fx, demo_dt[i, ]$tmrel,
                   demo_dt[i, ]$rr, rr_scalar, inv_exp, cap[[demo_dt[i, ]$draw[1] + 1]])$paf
        }, error = function(cond) {
          as.numeric(NA)
        })
      }
    } else {
      rr_power <- function(exposure, exposure_cap, tmrel, inv_exp, rr_scalar) {
        if (inv_exp == 0) {
          return((((exposure - tmrel + abs(exposure - tmrel))/2) - (exposure - exposure_cap + abs(exposure - exposure_cap))/2)/rr_scalar)
        } else if (inv_exp == 1) {
          return((((tmrel - exposure + abs(tmrel - exposure))/2) - (exposure_cap - exposure + abs(exposure_cap - exposure))/2)/rr_scalar)
        }
      }
      calc_paf <- function(i) {
        tryCatch({
          exp_pdf <- approxfun(dens[[demo_dt[i, ]$draw[1] + 1]]$x,
                               dens[[demo_dt[i, ]$draw[1] + 1]]$fx,
                               yleft=0, yright=0)
          denom <- integrate(function(x)
            exp_pdf(x) * demo_dt[i, ]$rr ^ rr_power(x, cap[[demo_dt[i, ]$draw[1] + 1]], demo_dt[i, ]$tmrel, inv_exp, rr_scalar),
            lower=dens[[demo_dt[i, ]$draw[1] + 1]]$XMIN,
            upper=dens[[demo_dt[i, ]$draw[1] + 1]]$XMAX, stop.on.error = FALSE)$value
          return((denom-1)/denom)
        }, error = function(cond) {
          return(as.numeric(NA))
        })
      }
    }
    calc_paf <- compiler::cmpfun(calc_paf)
    dt[, demo_id := .GRP, by=c("location_id", "year_id", "age_group_id", "sex_id")]
    demo_ids <- unique(dt$demo_id)
    out <- vector("list", length(demo_ids))
    for(demo in demo_ids) {
        demo_dt <- dt[demo_id == demo, ]
        # Build density given w=data.frame of weights, M = Mean, S=SD
        weights <- wlist[age_group_id == demo_dt$age_group_id[1] & sex_id == demo_dt$sex_id[1],
                         ][, c("age_group_id", "sex_id", "year_id", "location_id") := NULL][1, ]
        dens <- mclapply(0:(n_draws-1), function(d) get_edensity(weights, demo_dt[draw == d, ]$exp_mean[1],
                                 demo_dt[draw == d, ]$exp_sd[1]), mc.cores = 6)
        cap <- mclapply(1:n_draws, function(d)
            if (inv_exp) dens[[d]]$XMIN else dens[[d]]$XMAX, mc.cores = 6)
        pafs <- mclapply(1:nrow(demo_dt), calc_paf, mc.cores = 6)
        demo_dt <- cbind(demo_dt, paf = unlist(pafs))
        out[[demo]] <- demo_dt
        rm(demo_dt);rm(weights);rm(dens);rm(cap);rm(pafs)
    }
    dt <- rbindlist(out, fill=T, use.names=T)
    return(dt)

}

# categorical, PAF = (SUM(exp * rr) - SUM(tmrel * rr)) / SUM(exp * rr)
categ_paf <- function(dt) {
    # expand mortality and morbidity
    dt <- rbind(dt[!(mortality == 1 & morbidity == 1), ],
                dt[mortality == 1 & morbidity == 1,
                   .(mortality = c(0, 1), morbidity = c(1, 0)),
                   by = c("location_id", "year_id", "age_group_id", "sex_id", "cause_id",
                          "parameter", "draw", "exp_mean", "rr", "tmrel")])
    dt <- dt[, .(exp_mean=sum(exp_mean * rr), tmrel=sum(tmrel * rr)),
             by = c("location_id", "year_id", "age_group_id", "sex_id", "cause_id",
                    "mortality", "morbidity", "draw")]
    dt[, paf := (exp_mean - tmrel)/exp_mean]
    return(dt)
}
