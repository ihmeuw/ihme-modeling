

library(fitdistrplus)

ModelMarginalDistr <- function(data = NULL, mean = NULL, sd = NULL, XMIN = NULL, XMAX = NULL, univ_fam, weights = NULL){
  #' @description Models the parametric form of a marginal distribution from univariate data. 
  #' Can model ensemble or non-ensemble marginals.
  #' @param data numeric vector of univariate data
  #' @param univ_fam character string. Describes the family of the distribution. Accepts one of: ensemble, exp, gamma, invgamma, llogis, 
  #' gumbel, weibull, lnorm, norm, betasr, mgamma, mgumbel, unif
  #' @param weights data.table (NOT data.frame). Only specify if univ_fam = "ensemble". The weights should be in the standard format produced by the GBD ensemble weight-generation code.
  #' @return A list of density objects. Each density object itself is a list of 8 objects:
  #' family: chr the distribution family
  #' weight: num the weight assigned to the distribution family. If the marginal distribution is not part of an ensemble distribution model, then the weight = 1.0
  #' temp_weight: num the temporary weight assigned to the distribution family, since one of the distributions (betasr) is broken. Because of this, any weight assigned to betasr is redistributed equally to all other marginal families. 
  #' If the marginal distribution is not part of an ensemble distribution model, then the weight = 1.0
  #' params: num named numeric vector of the parameter values of the marginal distribution (e.g. a normal distribution could have the params vector <1,2>, named "mean", "sd")
  #' XMIN: num the minimum value of the distribution. The density at X less than this point is 0.
  #' XMAX: the maximum value of the distribution. The density at X greater than this point is 0.
  #' x: num 1000 x values of the empirical density distribution - used for plotting
  #' fx: num 1000 y values of the empirical density distribution - used for plotting 
  
  if(is.null(data) & is.null(mean) & is.null(sd) & is.null(XMIN) & is.null(XMAX)){
    stop("Data OR Mean, SD, XMIN, and XMAX need to be provided")
    break
  } else if(!is.null(data) & (!is.null(mean) | !is.null(sd))){
    stop("Data OR Mean, SD, XMIN, and XMAX need to be provided")
  } else if (!is.null(data)){
    
    XMIN = min(data)
    XMAX = max(data)
    
    mean <- mean(data)
    sd <- sd(data)
    
  } 
  
  # Make global variables
  XMIN <<- XMIN
  XMAX <<- XMAX
  
  x = seq(XMIN, XMAX, length = 1000)
  fx = 0 * x
  
  buildDENlist <- function(z) {
    
    distr = names(weights)[[z]]
    weight = weights[[z]]
    
    
    LENGTH <- length(formals(unlist(dlist[[distr]]$mv2par)))
    if (LENGTH==4) {
      est <- try((unlist(dlist[[distr]]$mv2par(mean,sd^2,XMIN=XMIN,XMAX=XMAX))),silent=T)
    } else {
      est <- try((unlist(dlist[[distr]]$mv2par(mean,sd^2))),silent=T)
    }
    
    # Calculate the "fx's" - evaluate the density function at each x cut-point
    fxj <- try(dlist[[distr]]$dF(x,est),silent=T)
    
    # If there is something wrong with the density calculation, it will return all 0s for fxj 
    if(class(est)=="try-error") {
      fxj <- rep(0,length(fx))
    }
    
    fxj[!is.finite(fxj)] <- 0
    
    return(list(family = distr, weight = weight, params = est, XMIN = XMIN, XMAX = XMAX, x = x, fx = fxj))
    
  }
  
  
  
  
  
  
  
  
  if(univ_fam == "unif"){
    
    x = seq(XMIN, XMAX, length = 1000)
    distr <- fitdist(x, distr = univ_fam, method = "mle")
    density.list <- list(list(family = univ_fam, weight = 1, params = distr$estimate, XMIN = XMIN, XMAX = XMAX, x = x, fx = rep(1/1000, 1000)))
    
  } else if(univ_fam %in% c("exp", "gamma", "gumbel", "invgamma", "llogis", "lnorm", "mgamma", "mgumbel", "norm", "weibull")) {
    
    weights = data.table(weight = 1)
    setnames(weights, "weight", univ_fam)
    
    density.list <- list(buildDENlist(1))
    
  } else if(univ_fam == "ensemble"){
    
    # remove 0s
    #weights_copy <- copy(weights)
    #weights_copy <- weights_copy[1, weights_copy != 0, with=FALSE]
    
    density.list <- lapply(1:length(weights), buildDENlist)
    
    
  } else {
    
    message("Distribution not implemented. Choose ensemble, exp, gamma, invgamma, llogis, gumbel, weibull, lnorm, norm, mgamma, mgumbel")
    
  }
  
  return(density.list)
  
}

fit_copula <- function(data, family = NA, param_filepath){
  
  fit.data <- copy(data)
  fit.data <- fit.data[, lapply(.SD, pobs)]
  
  RVM <- RVineStructureSelect(fit.data, familyset = family)
  
  
  
  if(!is.na(param_filepath)) { 
    
    pairs <- combn(1:ncol(fit.data), m = 2)
    
    cop_params <- lapply(1:ncol(pairs), function(j){
      
      x <- pairs[,j][1]
      y <- pairs[,j][2]
      
      y_x <- data.table(vars = paste0("var", y, "_var", x), 
                        family = RVM$family[y,x], 
                        par1 = RVM$par[y,x], 
                        par2 = RVM$par2[y,x], 
                        tau = RVM$tau[y,x], 
                        #AIC = RVM$pair.AIC[y,x], 
                        total_AIC = RVM$AIC, 
                        #BIC = RVM$pair.BIC[y,x], 
                        total_BIC = RVM$BIC)
      
      return(y_x)
      
    }) %>% rbindlist
    
    cop_params[, N := nrow(fit.data)]
    
    saveRDS(cop_params, param_filepath) 
    
  } 
  
  return(RVM)
  
}

ApplyQuantileFunction <- function(dens.list, data){
  
  
  weighted.densities <- lapply(dens.list, function(dens){
    
    if(dens$family == "exp"){
      weighted_fx <- dens$weight * qexp(p = data, rate = dens$params["rate"])
    }
    
    if(dens$family == "gamma"){
      weighted_fx <- dens$weight * qgamma(p = data, shape = dens$params["shape"], rate = dens$params["rate"])
    }
    
    if(dens$family == "invgamma"){
      weighted_fx <- dens$weight * qinvgamma(p = data, shape = dens$params["shape"], scale = dens$params["scale"])
    }
    
    if(dens$family == "llogis"){
      weighted_fx <- dens$weight * qllogis(p = data, shape = dens$params["shape"], scale = dens$params["scale"])
    }
    
    if(dens$family == "gumbel"){
      weighted_fx <- dens$weight * qgumbel(p = data, alpha = dens$params["alpha"], scale = dens$params["scale"])
    }
    
    if(dens$family == "weibull"){
      weighted_fx <- dens$weight * qweibull(p = data, shape = dens$params["shape"], scale = dens$params["scale"])
    }
    
    if(dens$family == "lnorm"){
      weighted_fx <- dens$weight * qlnorm(p = data, meanlog = dens$params["meanlog"], sdlog = dens$params["sdlog"])
    }
    
    if(dens$family == "norm"){ #
      weighted_fx <- dens$weight * qnorm(p = data, mean = dens$params["mean"], sd = dens$params["sd"])
    }
    
    
    if(dens$family == "mgamma"){ #
      
      XMAX <<- dens$XMAX
      XMIN <<- dens$XMIN
      
      qmgamma_mod <- function(p, shape, rate) {
        XMAX - qgamma(p, shape = shape, rate = rate)
      }
      
      weighted_fx <- dens$weight * qmgamma_mod(p = 1-data, shape = dens$params["shape"], rate = dens$params["rate"]) 
      
    }
    
    if(dens$family == "mgumbel"){ #
      
      XMAX <<- dens$XMAX
      XMIN <<- dens$XMIN
      
      qmgumbel_mod <- function(p, alpha, scale) {
        XMAX - (qgumbel(p, alpha, scale))
      }
      
      weighted_fx <- dens$weight * qmgumbel_mod(p = 1-data, alpha = dens$params["alpha"], scale = dens$params["scale"])
      
    }
    
    if(dens$family == "unif"){
      weighted_fx <- dens$weight * qunif(p = data, min = dens$params["min"], max = dens$param["max"])
    }
    
    
    return(weighted_fx)
    
  })
  
  
  if(length(weighted.densities)>1){
    length_to_reduce_over <- 1:length(weighted.densities)
  } else{
    length_to_reduce_over <- 1
  }
  
  ensemble_fx <- Reduce(x = weighted.densities[length_to_reduce_over], f = "+")
  
  weighted.densities[[length(weighted.densities)+1]] <- ensemble_fx
  
  names_of_weights <- sapply(dens.list, function(den){
    
    return(den$family)
    
  })
  
  names(weighted.densities) <- c(names_of_weights, "modeled")
  
  return(weighted.densities)
  
}



stretch_optim_37 <- function(par, sim, ga_u_37){
  
  ga_par <- par
  
  ga_threshold <- 40
  min_ga <- min(sim[, "ga"])
  ga_vector_u_37 <- sim[ga<ga_threshold, ga]
  
  ga_vector_u_37 <- unique(sim[ga<ga_threshold, ga])
  
  ga_vector_u_37 <- sort(ga_vector_u_37, decreasing = FALSE)
  
  ga_scalars <- ((ga_vector_u_37 - min_ga)/(ga_threshold - min_ga))^(ga_par)
  
  ga_dt <- data.table(ga = sort(ga_vector_u_37, decreasing = FALSE),
                      ga_scalars = sort(ga_scalars, decreasing = FALSE))
  
  sim <- merge(sim, ga_dt, all.x = T, by = c("ga"))
  
  sim[, old_ga := ga]
  sim[ga < 40, ga := ga * ga_scalars]
  
  e_ga_u_37 <- (nrow(sim[ga < 37, ]) / nrow(sim))
  
  sum(log(e_ga_u_37/ga_u_37))^2
  
}

stretch_return_37 <- function(par, sim){
  
  ga_par <- par
  
  ga_threshold <- 40
  min_ga <- min(sim[, "ga"])
  ga_vector_u_37 <- sim[ga<ga_threshold, ga]
  
  ga_vector_u_37 <- unique(sim[ga<ga_threshold, ga])
  
  ga_vector_u_37 <- sort(ga_vector_u_37, decreasing = FALSE)
  
  ga_scalars <- ((ga_vector_u_37 - min_ga)/(ga_threshold - min_ga))^(ga_par)
  
  ga_dt <- data.table(ga = sort(ga_vector_u_37, decreasing = FALSE),
                      ga_scalars = sort(ga_scalars, decreasing = FALSE))
  
  sim <- merge(sim, ga_dt, all.x = T, by = c("ga"))
  
  sim[, old_ga := ga]
  sim[ga < ga_threshold, ga := ga * ga_scalars]
  
  sim <- sim[, -c("ga_scalars")]
  
  return(sim)
  
}

stretch_optim_28 <- function(par, sim, ga_u_28, ga_u_37){
  
  ga_par <- par
  
  ga_threshold <- 37
  min_ga <- min(sim[, "ga"])
  ga_vector_u_37 <- sim[ga<ga_threshold, ga]
  
  ga_vector_u_37 <- unique(sim[ga<ga_threshold, ga])
  
  ga_vector_u_37 <- sort(ga_vector_u_37, decreasing = FALSE)
  
  ga_scalars <- ((ga_vector_u_37 - min_ga)/(ga_threshold - min_ga))^(ga_par)
  
  ga_dt <- data.table(ga = sort(ga_vector_u_37, decreasing = FALSE),
                      ga_scalars = sort(ga_scalars, decreasing = FALSE))
  
  sim <- merge(sim, ga_dt, all.x = T, by = c("ga"))
  
  sim[, old_ga := ga]
  sim[ga < ga_threshold, ga := ga * ga_scalars]
  
  e_ga_u_28 <- (nrow(sim[ga < 28, ]) / nrow(sim))
  
  sum(log(e_ga_u_28/ga_u_28))^2
  
}

stretch_return_28 <- function(par, sim){
  
  ga_par <- par
  
  ga_threshold <- 37
  min_ga <- min(sim[, "ga"])
  
  ga_vector_u_37 <- unique(sim[ga<ga_threshold, ga])
  
  ga_vector_u_37 <- sort(ga_vector_u_37, decreasing = FALSE)
  
  ga_scalars <- ((ga_vector_u_37 - min_ga)/(ga_threshold - min_ga))^(ga_par)
  
  ga_dt <- data.table(ga = sort(ga_vector_u_37, decreasing = FALSE),
                      ga_scalars = sort(ga_scalars, decreasing = FALSE))
  
  sim <- merge(sim, ga_dt, all.x = T, by = c("ga"))
  
  sim[, old_ga := ga]
  sim[ga < 37, ga := ga * ga_scalars]
  
  sim <- sim[, -c("ga_scalars")]
  
  return(sim)
  
}

stretch_optim_2500 <- function(par, sim, bw_u_2500){
  
  bw_par <- par
  
  bw_threshold <- 3000
  min_bw <- min(sim[, "bw"])
  bw_vector_u_2500 <- sim[bw<bw_threshold, bw]
  
  bw_vector_u_2500 <- unique(sim[bw<bw_threshold, bw])
  
  bw_vector_u_2500 <- sort(bw_vector_u_2500, decreasing = FALSE)
  
  bw_scalars <- ((bw_vector_u_2500 - min_bw)/(bw_threshold - min_bw))^(bw_par)
  
  bw_dt <- data.table(bw = sort(bw_vector_u_2500, decreasing = FALSE),
                      bw_scalars = sort(bw_scalars, decreasing = FALSE))
  
  sim <- merge(sim, bw_dt, all.x = T, by = c("bw"))
  
  sim[, old_bw := bw]
  sim[bw < 3000, bw := bw * bw_scalars]
  
  e_bw_u_2500 <- (nrow(sim[bw < 2500, ]) / nrow(sim))
  
  sum(log(e_bw_u_2500/bw_u_2500))^2
  
}

stretch_return_2500 <- function(par, sim){
  
  bw_par <- par
  
  bw_threshold <- 3000
  min_bw <- min(sim[, "bw"])
  bw_vector_u_2500 <- sim[bw<bw_threshold, bw]
  
  bw_vector_u_2500 <- unique(sim[bw<bw_threshold, bw])
  
  bw_vector_u_2500 <- sort(bw_vector_u_2500, decreasing = FALSE)
  
  bw_scalars <- ((bw_vector_u_2500 - min_bw)/(bw_threshold - min_bw))^(bw_par)
  
  bw_dt <- data.table(bw = sort(bw_vector_u_2500, decreasing = FALSE),
                      bw_scalars = sort(bw_scalars, decreasing = FALSE))
  
  sim <- merge(sim, bw_dt, all.x = T, by = c("bw"))
  
  sim[, old_bw := bw]
  sim[bw < 3000, bw := bw * bw_scalars]
  
  sim <- sim[, -c("bw_scalars")]
  
  return(sim)
  
}

stretch_optim_500 <- function(par, sim, bw_u_2500){
  
  bw_par <- par
  
  bw_threshold <- 2500
  bw_stretch_pt <- 500
  
  min_bw <- min(sim[, "bw"])
  
  bw_vector_u_2500 <- sim[bw<bw_threshold, bw]
  
  bw_vector_u_2500 <- unique(sim[bw<bw_threshold, bw])
  
  bw_vector_u_2500 <- sort(bw_vector_u_2500, decreasing = FALSE)
  
  bw_scalars <- ((bw_vector_u_2500 - min_bw)/(bw_threshold - min_bw))^(bw_par)
  
  bw_dt <- data.table(bw = sort(bw_vector_u_2500, decreasing = FALSE),
                      bw_scalars = sort(bw_scalars, decreasing = FALSE))
  
  sim <- merge(sim, bw_dt, all.x = T, by = c("bw"))
  
  sim[, old_bw := bw]
  sim[bw < bw_threshold, bw := bw * bw_scalars]
  
  #ratio_500_2500 <- nrow(bw_microdata[bw < bw_stretch_pt, "bw"]) / nrow(bw_microdata[bw <2500, "bw"])
  ratio_500_2500 <- 0.01634069 ## Using ratio of 500 / 2500
  
  
  e_ratio_500_2500 <- nrow(sim[bw < bw_stretch_pt, ]) / nrow(sim[bw < 2500, ])
  
  
  
  sum(log(e_ratio_500_2500/ratio_500_2500))^2
  
  
}

stretch_return_500 <- function(par, sim){
  
  bw_par <- par
  
  bw_threshold <- 2500
  bw_stretch_pt <- 500
  
  min_bw <- min(sim[, "bw"])
  
  bw_vector_u_2500 <- sim[bw<bw_threshold, bw]
  
  bw_vector_u_2500 <- unique(sim[bw<bw_threshold, bw])
  
  bw_vector_u_2500 <- sort(bw_vector_u_2500, decreasing = FALSE)
  
  bw_scalars <- ((bw_vector_u_2500 - min_bw)/(bw_threshold - min_bw))^(bw_par)
  
  bw_dt <- data.table(bw = sort(bw_vector_u_2500, decreasing = FALSE),
                      bw_scalars = sort(bw_scalars, decreasing = FALSE))
  
  sim <- merge(sim, bw_dt, all.x = T, by = c("bw"))
  
  sim[, old_bw := bw]
  sim[bw < bw_threshold, bw := bw * bw_scalars]
  
  sim <- sim[, -c("bw_scalars")]
  
  return(sim)
  
}