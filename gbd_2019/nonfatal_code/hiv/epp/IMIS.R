## Modified from IMIS package by Le Bao (http://cran.r-project.org/web/packages/IMIS/)

IMIS <- function(B0, B, B.re, number_k=500, D=0, opt_iter=0, fp, likdat){
  
  X_k <- sample.prior(B0, fp)  # Draw initial samples from the prior distribution
  X_all <- matrix(0, B0 + B*((D-1)*opt_iter+number_k), dim(X_k)[2])
  n_all <- 0
  X_all[1:B0,] <- X_k
  
  Sig2_global = cov(X_all[1:B0,])        # the prior covariance
  stat_all = matrix(NA, 6, number_k)                            # 6 diagnostic statistics at each iteration
  center_all = NULL                                             # centers of Gaussian components
  prior_all = like_all = gaussian_sum = vector("numeric", B0 + B*((D-1)*opt_iter+number_k))
  sigma_all = list()                                            # covariance matrices of Gaussian components
  
  iter.start.time = proc.time()
  for (k in 1:number_k ){
    ##Rprof(paste("IMIS-k", k, ".out", sep=""))
    ptm.like = proc.time()

    prior_all[n_all + 1:dim(X_k)[1]] <-  prior(X_k, fp)
    like_all[n_all + 1:dim(X_k)[1]] <-  likelihood(X_k, fp, likdat)
    ptm.use = (proc.time() - ptm.like)[3]
    if (k==1)   print(paste(B0, "likelihoods are evaluated in", round(ptm.use/60,2), "minutes"))
    which_pos <- which(like_all[1:(n_all + dim(X_k)[1])] > 0)

    
    if(k>=2){
      if(k <= (opt_iter+1)){
        if(k > 2 && length(which_pos) > n_pos)
          gaussian_sum[(n_pos+1):length(which_pos)] <- rowSums(matrix(sapply(1:(D*(k-2)), function(j)dmvnorm(X_all[which_pos[(n_pos+1):length(which_pos)],,drop=FALSE], center_all[j,], sigma_all[[j]])), nrow=length(which_pos)-n_pos))
        n_pos <- length(which_pos)
        gaussian_sum[1:n_pos] <- gaussian_sum[1:n_pos] + rowSums(matrix(sapply(D*(k-2)+1:D, function(j) dmvnorm(X_all[which_pos,,drop=FALSE], center_all[j,], sigma_all[[j]])),nrow=n_pos))
      } else {
        if(k > 2 && length(which_pos) > n_pos)
          gaussian_sum[(n_pos+1):length(which_pos)] <- rowSums(matrix(sapply(1:((D-1)*opt_iter + k-2), function(j)dmvnorm(X_all[which_pos[(n_pos+1):length(which_pos)],,drop=FALSE], center_all[j,], sigma_all[[j]])), nrow=length(which_pos)-n_pos))
        n_pos <- length(which_pos)
        gaussian_sum[1:n_pos] <- gaussian_sum[1:n_pos] + dmvnorm(X_all[which_pos,], center_all[(D-1)*opt_iter + k-1,], sigma_all[[(D-1)*opt_iter + k-1]])
      }
    }
        

    
    if (k==1)   envelop_pos = prior_all[which_pos]        # envelop stores the sampling densities

    if (k>1)    envelop_pos = (prior_all[which_pos]*B0/B + gaussian_sum[1:n_pos]) / (B0/B+(k-1))

    Weights = prior_all[which_pos]*like_all[which_pos]/ envelop_pos  # importance weight is determined by the posterior density divided by the sampling density

    stat_all[1,k] = log(mean(Weights)*length(which_pos)/(n_all+dim(X_k)[1]))                  # the raw marginal likelihood
    Weights = Weights / sum(Weights)
    stat_all[2,k] = sum(1-(1-Weights)^B.re)             # the expected number of unique points
    stat_all[3,k] = max(Weights)                                # the maximum weight
    stat_all[4,k] = 1/sum(Weights^2)                    # the effictive sample size
    stat_all[5,k] = -sum(Weights*log(Weights), na.rm = TRUE) / log(length(Weights))     # the entropy relative to uniform
    stat_all[6,k] = var(Weights/mean(Weights))  # the variance of scaled weights
    if (k==1)   print("Stage   MargLike   UniquePoint   MaxWeight   ESS   IterTime")
    iter.stop.time = proc.time()
    print(c(k, round(stat_all[1:4,k], 3), as.numeric(iter.stop.time - iter.start.time)[3]))
    iter.start.time = iter.stop.time

    if(k <= opt_iter){
      ## Sig2_global = cov(X_all[which(like_all>min(like_all)),])  ## !! not sure why recalculating this -- but may be based on few points
      n_all <- n_all + dim(X_k)[1]
      X_k <- NULL
      which_exclude = NULL					# exclude the neighborhood of the local optima
      label_weight = sort(Weights, decreasing = TRUE, index = TRUE)
      which_remain = which(Weights > 0)  # the candidate inputs for the starting points
      size_remain = length(which_remain)
      for(i in 1:D){
        important = NULL
        if (length(which_remain)>0)
          important <- which_remain[which(Weights[which_remain]==max(Weights[which_remain]))]
        if (length(important)>1) important = sample(important,1)
        X_imp <- X_all[which_pos[important],]
        ## Remove the selected input from candidates
        which_exclude = union( which_exclude, important )
        which_remain = setdiff(which_remain, which_exclude)

        nlposterior <- function(theta){-log(prior(theta, fp))-log(likelihood(theta, fp, likdat))}
        ## The rough optimizer uses the Nelder-Mead algorithm.
        if (length(important)==0) X_imp = center_all[1,]
        ptm.opt = proc.time()
        optNM <- optim(X_imp, nlposterior, method="Nelder-Mead", 
                       control=list(maxit=5000, parscale=sqrt(diag(Sig2_global))))
        ## The more efficient optimizer uses the BFGS algorithm 
        optBFGS <- try(optim(optNM$par, nlposterior, method="BFGS", hessian=TRUE,
                             control=list(parscale=sqrt(diag(Sig2_global)), maxit=1000)), silent=TRUE)
        ptm.use = (proc.time() - ptm.opt)[3]
        if(inherits(optBFGS, "try-error")){
          print(paste0("D = ", i, "; BFGS optimization failed."))
          print(paste("maximum log posterior=", round(-optNM$value,2),
                      ", likelihood=", round(log(likelihood(optNM$par, fp, likdat)),2), 
                      ", prior=", round(log(prior(optNM$par, fp)),2),
                      ", time used=", round(ptm.use/60,2),
                      "minutes, convergence=", optNM$convergence))
          center_all <- rbind(center_all, optNM$par)
          sigma_all[[D*(k-1) + i]] <- Sig2_global
        } else {
          print(paste("maximum log posterior=", round(-optBFGS$value,2),
                      ", likelihood=", round(log(likelihood(optBFGS$par, fp, likdat)),2), 
                      ", prior=", round(log(prior(optBFGS$par, fp)),2),
                      ", time used=", round(ptm.use/60,2),
                      "minutes, convergence=", optBFGS$convergence))
          center_all <- rbind(center_all, optBFGS$par)  # the center of new samples
          eig <- eigen(optBFGS$hessian)
          if (all(eig$values>0)){
            sigma_all[[D*(k-1) + i]] = solve(optBFGS$hessian)  # the covariance of new samples
          } else { # If the hessian matrix is not positive definite, we define the covariance as following
            eigval <- replace(eig$values, eig$values < 0, 0)
            sigma_all[[D*(k-1) + i]] <- solve(eig$vectors %*% diag(eigval) %*% t(eig$vectors) + diag(1/diag(Sig2_global)) )
          }
        }

        distance_remain <- mahalanobis(X_all[which_pos[which_remain],], center_all[i,], diag(diag(Sig2_global)) )
        
        ## exclude the neighborhood of the local optima 
        label_dist <- sort(distance_remain, decreasing = FALSE, index=TRUE)
        which_exclude <- union( which_exclude, which_remain[label_dist$ix[1:floor(size_remain/D)]])
        which_remain <- setdiff(which_remain, which_exclude)

        X_k <- rbind(X_k, rmvnorm(B, center_all[D*(k-1) + i,], sigma_all[[D*(k-1) + i]]))
      }
      X_all[n_all + 1:(B*D),] <- X_k
    } else {
  
      ## choose the important point
      important = which(Weights == max(Weights))
      if (length(important)>1)  important = important[1]
      X_imp <- X_all[which_pos[important],]            # X_imp is the maximum weight input
      center_all <- rbind(center_all, X_imp)
      distance_all <- mahalanobis(X_all[1:(n_all+dim(X_k)[1]),], X_imp, diag(diag(Sig2_global)) )
      label_nr = sort(distance_all, decreasing = FALSE, index=TRUE, method="quick")             # Sort the distances
      which_var = label_nr$ix[1:B]                                                              # Pick B inputs for covariance calculation

      ## #########
      weight_close <- Weights[match(which_var, which_pos)]
      weight_close[!which_var %in% which_pos] <- 0

      n_all <- n_all + dim(X_k)[1]
      Sig2 <- cov.wt(X_all[which_var,], wt = weight_close+1/n_all, cor = FALSE, center = X_imp, method = "unbias")$cov
      sigma_all[[(D-1)*opt_iter+k]] <- Sig2
      X_k <- rmvnorm(B, X_imp, Sig2)                           # Draw new samples
      X_all[n_all + 1:B,] <- X_k

      if (stat_all[2,k] > (1-exp(-1))*B.re)       break
    }
  } # end of k

    ##nonzero = which(Weights>0)
  ##which_X = sample(nonzero, B.re, replace = TRUE, prob = Weights[nonzero])
  which_X = sample(which_pos, B.re, replace = TRUE, prob = Weights)
  if (is.matrix(X_all)) resample_X = X_all[which_X,]

  return(list(stat=t(stat_all[,1:k]), resample=resample_X, center=center_all))
} # end of IMIS
