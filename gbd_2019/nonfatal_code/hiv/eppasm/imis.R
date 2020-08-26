## Modified from IMIS package by Le Bao (http://cran.r-project.org/web/packages/IMIS/)

#' Incremental Mixture Importance Sampling (IMIS)
#'
#' Implements IMIS algorithm with optional optimization step (Raftery and Bao 2010).
#'
#' @param B0 number of initial samples to draw
#' @param B number of samples at each IMIS iteration
#' @param B_re number of resamples
#' @param number_k maximum number of iterations
#' @param opt_k vector of iterations at which to use optimization step to identify new mixture component
#' @param fp fixed model parameters
#' @param likdat likeihood data
#' @param prior function to calculate prior density for matrix of parameter inputs
#' @param likelihood function to calculate likelihood for matrix of parameter inputs
#' @param sample_prior function to draw an initial sample of parameter inputs
#' @param dsamp function to calculate density for initial sampling distribution (may be equal to prior)
#' @param save_all logical whether to save all sampled parameters
#'
#' @return list with items resample, stat, and center
imis <- function(B0, B, B_re, number_k, opt_k=NULL, fp, likdat,
                 prior=eppasm::prior,
                 likelihood=eppasm::likelihood,
                 sample_prior=eppasm::sample.prior,
                 sample_prior_group2=eppasm::sample.prior.group2,
                 dsamp = eppasm::dsamp, save_all=FALSE){

  ## Draw initial samples from prior distribution
  if((exists('group', where = fp) & fp$group == '2')){
    X_k <- sample_prior_group2(B0, fp) # Draw initial samples of mort param adjustment from the prior distribution
    }else{
     X_k <- sample_prior(B0, fp)  # Draw initial samples from the prior distribution
    }
  cov_prior = cov(X_k)        # estimate of the prior covariance

  ## Locations and covariance of mixture components
  center_all <- list()
  sigma_all <- list()

  ll_all <- numeric()
  lprior_all <- numeric()
  mix_all <- numeric()
  n_k <- integer()

  X_all <- matrix(0, B0+B*number_k, ncol(X_k))
  n_all <- 0
  stat <- matrix(NA, number_k, 7)

  idx_exclude <- integer()  ## inputs to exclude from initial points for optimization step


  iter_start_time <- proc.time()
  for(k in 1:number_k){
    ## Calculate log-likelihood for new inputs
    ll_k <- likelihood(X_k, fp, likdat, log=TRUE)

    ##  Keep only inputs with non-zero likelihood, calculate importance weights
    which_k <- which(ll_k > -Inf)
    
    n_k[k] <- length(which_k)
    idx_k <- n_all + seq_len(n_k[k])
    n_all <- n_all+n_k[k]

    X_k <- X_k[which_k,]
    ll_k <- ll_k[which_k]
    prior_k <- prior(X_k, fp)
    prior_k[prior_k == 0] <- .Machine$double.xmin
    lprior_k <- log(prior_k)

    ## calculate mixture weights for new inputs
    mix_k <- dsamp(X_k, fp) * B0 / B
    mix_k[mix_k == 0] <- .Machine$double.xmin
    if(k > 1)
      mix_k <- mix_k + rowSums(mapply(mvtnorm::dmvnorm, mean=center_all, sigma=sigma_all, MoreArgs=list(x=X_k)))

    X_all[idx_k,] <- X_k
    ll_all[idx_k] <- ll_k
    lprior_all[idx_k] <- lprior_k
    mix_all[idx_k] <- mix_k

    offset <- max(ll_all) - 400
    weights <- exp(ll_all + lprior_all - log(mix_all) - offset)
    lmarg_post <- log(sum(weights)) - log(B0+B*(k-1)) + offset + log(B0/B+(k-1))  # log marginal posterior
    weights <- weights / sum(weights)

    ## Store convergence monitoring criteria
    iter_stop_time <- proc.time()
    stat[k,] <- c(lmarg_post,                         # log marginal posterior
                  sum(1-(1-weights)^B_re),            # the expected number of unique points
                  max(weights),                       # the maximum weight
                  1/sum(weights^2),                   # the effictive sample size
                  -sum(weights*log(weights), na.rm = TRUE) / log(length(weights)),    # the entropy relative to uniform !!! NEEDS UDPATING FOR OMITTED SAMPLES !!!
                  var(weights/mean(weights)),                                         # the variance of scaled weights  !!! NEEDS UDPATING FOR OMITTED SAMPLES !!!
                  as.numeric(iter_stop_time - iter_start_time)[3])
    iter_start_time <- iter_stop_time

    if (k==1)
      print("Stage   MargLike   UniquePoint   MaxWeight      ESS   IterTime")
    print(sprintf("%5d  %9.3f  %12.2f %11.2f %8.2f %10.2f", k, stat[k,1], stat[k,2], stat[k,3], stat[k,4], stat[k,7]))


    ## Check for convergence
    if(stat[k,2] > (1 - exp(-1))*B_re || k == number_k)
      break;

    ## ## Identify new mixture component

    if(k %in% opt_k){

      idx_remain <- setdiff(seq_len(n_all), idx_exclude)
      idx_init <- idx_remain[which.max(weights[idx_remain])]
      idx_exclude <- c(idx_exclude, idx_init)
      theta_init <- X_all[idx_init,]
      
      nlposterior <- function(theta){-prior(theta, fp, log=TRUE)-likelihood(theta, fp, likdat, log=TRUE)}

      ## opt <- optimization_step(theta_init, nlposterior, cov_prior)  # Version by Bao uses prior covariance to parscale optimizer
      opt <- optimization_step(theta_init, nlposterior, cov(X_all[1:n_all, ]))
      center_all[[k]] <- opt$mu
      sigma_all[[k]] <- opt$sigma
      
      ## exclude the neighborhood of the local optima
      distance_remain <- mahalanobis(X_all[seq_len(n_all),], center_all[[k]], diag(diag(sigma_all[[k]])))
      idx_exclude <- union(idx_exclude, order(distance_remain)[seq_len(n_all/length(opt_k))])
      
    } else {

      ## choose mixture component centered at input with current maximum weight
      center_all[[k]] <- X_all[which.max(weights),]
      distance_all <- mahalanobis(X_all[1:n_all,], center_all[[k]], diag(diag(cov_prior)))   # Raftery & Bao version
      ## distance_all <- mahalanobis(X_all[1:n_all,], center_all[[k]], cov(X_all[1:n_all, ]))           # Suggested by Fasiolo et al.
      which_close <- order(distance_all)[seq_len(min(n_all, B))]  # Choose B nearest inputs (use n_all if n_all < B)
      sigma_all[[k]] <- cov.wt(X_all[which_close, , drop=FALSE], wt = weights[which_close]+1/n_all, center = center_all[[k]])$cov   # Raftery & Bao version
      if(any(is.na(sigma_all[[k]])))
        sigma_all[[k]] <- cov_prior
      ## sigma_all[[k]] <- cov.wt(X_all[which_close,], center = center_all[[k]])$cov                                   # Suggested by Fasiolo et al.
    }

    ## Update mixture weights according with new mixture component
    mix_all <- mix_all + mvtnorm::dmvnorm(X_all[1:n_all,], center_all[[k]], sigma_all[[k]])

    ## Sample B inputs from new mixture component
    X_k <- mvtnorm::rmvnorm(B, center_all[[k]], sigma_all[[k]])

  } # end of iteration k

  resample <- X_all[sample(seq_len(n_all), B_re, replace = TRUE, prob = weights),]

  if(save_all){
    mix_all_k <- cbind(dsamp(X_all[1:n_all,], fp) * B0 / B, mapply(mvtnorm::dmvnorm, mean=center_all, sigma=sigma_all, MoreArgs=list(x=X_all[1:n_all,])))
    return(list(stat=stat[1:k,], resample=resample, center=center_all, sigma_all=sigma_all,
                ll_all=ll_all, lprior_all=lprior_all, mix_all=mix_all, mix_all_k = mix_all_k, n_k=n_k))
  }
  return(list(stat=stat[1:k,], resample=resample, center=center_all))
}


optimization_step <- function(theta, fn, cov){
  
  ## The rough optimizer uses the Nelder-Mead algorithm.
  ptm.opt = proc.time()
  optNM <- optim(theta, fn, method="Nelder-Mead",
                 control=list(maxit=5000, parscale=sqrt(diag(cov))))

  ## The more efficient optimizer uses the BFGS algorithm
  optBFGS <- try(stop(""), TRUE)
  step_expon <- 0.2
  while(inherits(optBFGS, "try-error") && step_expon <= 0.8){
    optBFGS <- try(optim(optNM$par, fn, method="BFGS", hessian=TRUE,
                         control=list(parscale=sqrt(diag(cov)), ndeps=rep(.Machine$double.eps^step_expon, length(optNM$par)), maxit=1000)),
                   silent=TRUE)
    step_expon <- step_expon + 0.05
  }
  ptm.use = (proc.time() - ptm.opt)[3]
  if(inherits(optBFGS, "try-error")){
    print(paste0("BFGS optimization failed."))
    print(paste("maximum log posterior=", round(-optNM$value,2),
                ## ", likelihood=", round(likelihood(optNM$par, fp, likdat, log=TRUE),2),
                ## ", prior=", round(log(prior(optNM$par, fp)),2),
                ", time used=", round(ptm.use/60,2),
                "minutes, convergence=", optNM$convergence))
    mu <- optNM$par
    sigma <- cov
  } else {
    print(paste("maximum log posterior=", round(-optBFGS$value,2),
                ## ", likelihood=", round(likelihood(optBFGS$par, fp, likdat, log=TRUE),2),
                ## ", prior=", round(log(prior(optBFGS$par, fp)),2),
                ", time used=", round(ptm.use/60,2),
                "minutes, convergence=", optBFGS$convergence))
    mu <- optBFGS$par
    eig <- eigen(optBFGS$hessian)
    if (all(eig$values>0)){
      sigma <- chol2inv(chol(optBFGS$hessian))  # the covariance of new samples
    } else { # If the hessian matrix is not positive definite, we define the covariance as following

      eigval <- replace(eig$values, eig$values < 0, 0)
      sigma <- chol2inv(chol(eig$vectors %*% diag(eigval) %*% t(eig$vectors) + diag(1/diag(cov))))
    }
  }
  return(list(mu=mu, sigma=sigma))
}
