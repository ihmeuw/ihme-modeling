



#' Calculate predicted number of new diagnoses
calc_diagnoses <- function(mod, fp){
  
  colSums(attr(mod, "diagnoses"),,3)
}

cumgamma_diagn_rate <- function(gamma_max, delta_rate, fp){
  
  delta_t <- rep(0, fp$ss$PROJ_YEARS)
  ii <- fp$t_diagn_start:fp$ss$PROJ_YEARS
  
  delta_t[ii] <- gamma_max * pgamma(ii - (ii[1] - 1), shape=1, rate = delta_rate)
  
  ## Diagnosis rate assumed proportional to expected mortality
  fp$diagn_rate <- array(fp$cd4_mort,
                         c(fp$ss$hDS, fp$ss$hAG, fp$ss$NG, fp$ss$PROJ_YEARS))
  
  ii <- seq_len(fp$ss$PROJ_YEARS)
  delta_t <- fp$gamma_max * pgamma(ii, shape=1, rate = fp$delta_rate)
  
  fp$diagn_rate <- sweep(fp$diagn_rate, 4, delta_t, "*")
  
  fp
}



#' Log-liklihood for new diagnoses and AIDS deaths
ll_csavr <- function(theta, fp, likdat){
  
  fp <- create_param_csavr(theta, fp)
  
  mod <- simmod(fp)
  
  mod_aidsdeaths <- colSums(attr(mod, "hivdeaths"),,2)
  ll_aidsdeaths <- with(likdat$aidsdeaths, sum(dpois(aidsdeaths, mod_aidsdeaths[idx] * (1 - prop_undercount), log=TRUE)))
  
  mod_diagnoses <- calc_diagnoses(mod, fp)
  ll_diagnoses <- with(likdat$diagnoses, sum(dpois(diagnoses, mod_diagnoses[idx] * (1 - prop_undercount), log=TRUE)))
  
  ll_aidsdeaths + ll_diagnoses
}





sample_prior_diagn <- function(n, fp){
  
  nparam <- length(diagn_theta_mean)
  val <- rnorm(n * nparam, diagn_theta_mean, diagn_theta_sd)
  mat <- t(matrix(val, nparam, n))
  
  mat
}

lprior_csavr <- function(theta, fp){
  
  nparam_eppmod <- get_nparam_eppmod(fp)
  
  lprior_eppmod(theta[1:nparam_eppmod], fp)
}



lprior_diagn <- function(theta_diagn, fp){
  sum(dnorm(theta_diagn, diagn_theta_mean, diagn_theta_sd, log=TRUE))
}



prior_csavr <- function(theta, fp, log=FALSE){
  if(is.vector(theta))
    lval <- lprior_csavr(theta, fp)
  else
    lval <- unlist(lapply(seq_len(nrow(theta)), function(i) (lprior_csavr(theta[i,], fp))))
  if(log)
    return(lval)
  else
    return(exp(lval))
}

likelihood_csavr <- function(theta, fp, likdat, log=FALSE){
  if(is.vector(theta))
    lval <- ll_csavr(theta, fp, likdat)
  else
    lval <- unlist(lapply(seq_len(nrow(theta)), function(i) ll_csavr(theta[i,], fp, likdat)))
  if(log)
    return(lval)
  else
    return(exp(lval))
}