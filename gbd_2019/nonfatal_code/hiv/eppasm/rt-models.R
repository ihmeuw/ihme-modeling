prepare_logrw <- function(fp, tsEpidemicStart=fp$ss$time_epi_start+0.5){

  fp$tsEpidemicStart <- fp$proj.steps[which.min(abs(fp$proj.steps - tsEpidemicStart))]
  rw_steps <- fp$proj.steps[fp$proj.steps >= fp$tsEpidemicStart]

  rt <- list()
  rt$nsteps_preepi <- length(fp$proj.steps[fp$proj.steps < tsEpidemicStart])

  if(!exists("n_rw", fp))
    rt$n_rw <- ceiling(diff(range(rw_steps)))  ##
  else
    rt$n_rw <- fp$n_rw

  fp$numKnots <- rt$n_rw
  
  ## Random walk design matrix
  rt$rw_knots <- seq(min(rw_steps), max(rw_steps), length.out=rt$n_rw+1)
  rt$rwX <- outer(rw_steps, rt$rw_knots[1:rt$n_rw], ">=")
  class(rt$rwX) <- "integer"

  fp$rt <- rt

  fp$rvec.spldes <- rbind(matrix(0, rt$nsteps_preepi, fp$numKnots), rt$rwX)
                                
  if(!exists("eppmod", fp))
    fp$eppmod <- "logrw"
  fp$iota <- NULL
  
  return(fp)
}


rlog_pr_mean <- c(log(0.35), log(0.09), log(0.2), 1993)
rlog_pr_sd <- c(0.5, 0.3, 0.5, 5)
                
rlogistic <- function(t, p){
  ## p[1] = log r(0)    : log r(t) at the start of the epidemic (exponential growth)
  ## p[2] = log r(Inf)  : endemic value for log r(t)
  ## p[3] = alpha       : rate of change in log r(t)
  ## p[4] = t_mid       : inflection point for logistic curve

  p[1] - (p[1] - p[2]) / (1 + exp(-p[3] * (t - p[4])))
}


#' @param fp model parameters object
#' @param tsEpidemicStart time step at which epidemic is seeded
#' @param rw_start time when random walk starts
#' @param rw_trans number of years to transition from logistic differences to RW differences. If NULL, defaults to 5 steps
prepare_rhybrid <- function(fp,
                            tsEpidemicStart = fp$ss$time_epi_start+0.5,
                            rw_start = fp$rw_start,
                            rw_trans = fp$rw_trans,
                            rw_dk = fp$rw_dk){

  if(is.null(rw_start))
    rw_start <- 2003

  if(is.null(rw_trans))
    rw_trans <- 5

  if(is.null(rw_dk))
    rw_dk <- 5

  fp$tsEpidemicStart <- fp$proj.steps[which.min(abs(fp$proj.steps - tsEpidemicStart))]

  rt <- list()

  rt$proj.steps <- fp$proj.steps
  
  rt$rw_start <- rw_start
  rt$rw_trans <- rw_trans

  switch_idx <- max(which(fp$proj.steps <= rw_start))
  rt$rlogistic_steps <- fp$proj.steps[1:switch_idx]
  rt$rw_steps <- fp$proj.steps[switch_idx:length(fp$proj.steps)]
  
  rt$n_rw <- ceiling((max(rt$proj.steps) - rw_start) / rw_dk)
  rt$rw_dk <- rw_dk
  rt$rw_knots <- seq(rw_start, rw_start + rt$rw_dk * rt$n_rw, by = rt$rw_dk)
  rt$rw_idx <- findInterval(rt$rw_steps[-1], rt$rw_knots)
  
  rt$n_param <- 4+rt$n_rw  # 4 parameters for rlogistic

  ## Linearly interpolate between 0 and 1 over the period (rw_start, rw_start + rw_trans)
  ## Add a small value to avoid R error in approx() if rw_trans = 0
  rt$rw_transition <- approx(c(rw_start, rw_start + rw_trans + 0.001), c(0, 1), rt$rw_steps[-1], rule = 2)$y
  
  rt$dt <- 1 / fp$ss$hiv_steps_per_year

  rt$eppmod <- "rhybrid"
  fp$rt <- rt

  if(!exists("eppmod", fp))
    fp$eppmod <- "rhybrid"
  fp$iota <- NULL
  
  return(fp)
}

create_rvec <- function(theta, rt){
  if(rt$eppmod == "rhybrid"){

    par <- theta[1:4]
    par[3] <- exp(par[3])
    rvec_rlog <- rlogistic(rt$rlogistic_steps, par)

    th_rw <- theta[4+1:rt$n_rw]

    diff_rlog <- diff(rlogistic(rt$rw_steps, par))
    diff_rw <- rt$dt * th_rw[rt$rw_idx] / sqrt(rt$rw_dk)
    diff_rvec <- (1 - rt$rw_transition) * diff_rlog + rt$rw_transition * diff_rw
    rvec_rw <- cumsum(c(rvec_rlog[length(rvec_rlog)], diff_rvec))

    rvec <- c(rvec_rlog, rvec_rw)

    return(exp(rvec))
  }
  else
    stop(paste(rt$eppmod, "is not impmented in create_rvec()"))
}


#' Sample from conditional posterior distribution for variance parameter
sample_invgamma_post <- function(x, prior_shape, prior_rate){
  ## x: n_samples, n_knots
  if(is.vector(x)) x <- matrix(x, 1)
  1/rgamma(nrow(x), shape=prior_shape + ncol(x)/2,
           rate=prior_rate + 0.5*rowSums(x^2))
}

extend_projection <- function(fit, proj_years){


  if(proj_years > fit$fp$ss$PROJ_YEARS)
    stop("Cannot extend projection beyond duration of projection file")
  
  fp <- fit$fp
  fpnew <- fp

  fpnew$SIM_YEARS <- as.integer(proj_years)
  fpnew$proj.steps <- with(fpnew$ss, seq(proj_start+0.5, proj_start-1+fpnew$SIM_YEARS+0.5, by=1/hiv_steps_per_year))

  if(fp$eppmod == "rhybrid"){
    idx1 <- 5  # start of random walk parameters
    idx2 <- 4+fp$rt$n_rw
    fpnew <- prepare_rhybrid(fpnew)
  } else if(fp$eppmod == "logrw") {
    idx1 <- 1L
    idx2 <- fp$rt$n_rw
    fpnew <- prepare_logrw(fpnew)
  }

  theta <- fit$resample[,idx1:idx2, drop=FALSE]

  if(!is.null(fp$prior_args$rw_prior_sd))
     fit$rw_sigma <- fp$prior_args$rw_prior_sd
  else
    fit$rw_sigma <- rw_prior_sd
  
  nsteps <- fpnew$rt$n_rw - fp$rt$n_rw

  if(nsteps > 0){
    thetanew <- matrix(nrow=nrow(theta), ncol=fpnew$rt$n_rw)
    thetanew[,1:ncol(theta)] <- theta
    thetanew[,ncol(theta)+1:nsteps] <- rnorm(nrow(theta)*nsteps, sd=fit$rw_sigma)

    if(idx1 > 1)
      fit$resample <- cbind(fit$resample[,1:(idx1-1), drop=FALSE], thetanew, fit$resample[,(idx2+1):ncol(fit$resample), drop=FALSE])
    else
      fit$resample <- cbind(thetanew, fit$resample[,(idx2+1):ncol(fit$resample), drop=FALSE])
  } else {
    warning("already specified length, added rw_sigma only")
  }

  fit$fp <- fpnew

  return(fit)
}




calc_rtrend_rt <- function(t, fp, rveclast, prevlast, pop, i, ii){

  ## Attach state space variables
  invisible(list2env(fp$ss, environment())) # put ss variables in environment for convenience

  hivn.ii <- sum(pop[p.age15to49.idx,,hivn.idx,i])
  hivn.ii <- hivn.ii - sum(pop[p.age15to49.idx[1],,hivn.idx,i])*(1-DT*(ii-1))
  hivn.ii <- hivn.ii + sum(pop[tail(p.age15to49.idx,1)+1,,hivn.idx,i])*(1-DT*(ii-1))

  hivp.ii <- sum(pop[p.age15to49.idx,,hivp.idx,i])
  hivp.ii <- hivp.ii - sum(pop[p.age15to49.idx[1],,hivp.idx,i])*(1-DT*(ii-1))
  hivp.ii <- hivp.ii + sum(pop[tail(p.age15to49.idx,1)+1,,hivp.idx,i])*(1-DT*(ii-1))

  prevcurr <- hivp.ii / (hivn.ii + hivp.ii)

  
  if(t > fp$tsEpidemicStart){
    par <- fp$rtrend
    gamma.t <- if(t < par$tStabilize) 0 else (prevcurr-prevlast)*(t - par$tStabilize) / (fp$ss$DT*prevlast)
    logr.diff <- par$beta[2]*(par$beta[1] - rveclast) + par$beta[3]*prevlast + par$beta[4]*gamma.t
      return(exp(log(rveclast) + logr.diff))
    } else
      return(fp$rtrend$r0)
}




####  Model for iota  ####


logiota.unif.prior <- log(c(1e-13, 0.0025))
r0logiotaratio.unif.prior <- c(-25, -5)

logit <- function(p) log(p/(1-p))
invlogit <- function(x) 1/(1+exp(-x))
ldinvlogit <- function(x){v <- invlogit(x); log(v) + log(1-v)}

transf_iota <- function(par, fp){

  if(exists("prior_args", where = fp)){
    for(i in seq_along(fp$prior_args))
      assign(names(fp$prior_args)[i], fp$prior_args[[i]])
  }

  if(exists("logitiota", fp) && fp$logitiota)
    exp(invlogit(par)*diff(logiota.unif.prior) + logiota.unif.prior[1])
  else
    exp(par)  
}

lprior_iota <- function(par, fp){

  if(exists("prior_args", where = fp)){
    for(i in seq_along(fp$prior_args))
      assign(names(fp$prior_args)[i], fp$prior_args[[i]])
  }


  if(exists("logitiota", fp) && fp$logitiota)
    ldinvlogit(par)  # Note: parameter is defined on range logiota.unif.prior, so no need to check bound
  else
    dunif(par, logiota.unif.prior[1], logiota.unif.prior[2], log=TRUE)
}

sample_iota <- function(n, fp){
  if(exists("prior_args", where = fp)){
    for(i in seq_along(fp$prior_args))
      assign(names(fp$prior_args)[i], fp$prior_args[[i]])
  }
  if(exists("logitiota", fp) && fp$logitiota)
    return(logit(runif(n)))
  else
    runif(n, logiota.unif.prior[1], logiota.unif.prior[2])
}

ldsamp_iota <- lprior_iota
