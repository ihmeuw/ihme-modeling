ModelMarginalDistr <- function(data = NULL, mean = NULL, sd = NULL, minimum = NULL, maximum = NULL, univ_fam, weights = NULL){
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
  
  if(is.null(data) & is.null(mean) & is.null(sd) & is.null(minimum) & is.null(maximum)){
    stop("Data OR Mean and SD (optionally min & max) need to be provided")
    break
  } else if (!is.null(data)){
    
    .min = min(data)
    .max = max(data)
    
    M_ <- mean(data)
    S_ <- sd(data)
    
  } else if (!(is.null(mean) & is.null(sd))){
    
    if(!is.null(minimum) & !is.null(maximum)){
      
      print(minimum)
      print(maximum)
      
      .min = minimum
      .max = maximum
      
    }
    
    M_ = mean
    S_ = sd
    
  }
  
  
  mu <- log(M_/sqrt(1 + (S_^2/(M_^2))))
  sdlog <- sqrt(log(1 + (S_^2/M_^2)))
  
  XMIN <<- qlnorm(0.001, mu, sdlog)
  XMAX <<- qlnorm(0.999, mu, sdlog)
  
  if (is.null(minimum) | is.null(maximum)) {
    xx = seq(XMIN, XMAX, length = 1000)
  } else {   
    xx = seq(.min, .max, length = 1000)
  }
  
  fx = 0 * xx
  
  
  buildDENlist <- function(jjj) {
    
    distn = names(W_)[[jjj]]
    EST <- NULL
    LENGTH <- length(formals(unlist(dlist[paste0(distn)][[1]]$mv2par)))
    if (LENGTH == 4) {
      EST <- try(unlist(dlist[paste0(distn)][[1]]$mv2par(M_, (S_^2),
                                                         XMIN = XMIN,
                                                         XMAX = XMAX)),
                 silent = T)
    } else {
      EST <- try(unlist(dlist[paste0(distn)][[1]]$mv2par(M_, (S_^2))),
                 silent = T)
    }
    
    
    d.dist <- NULL
    d.dist <- try(dlist[paste0(distn)][[1]]$dF(xx, EST), silent = T)
    if (class(EST) == "numeric" & class(d.dist) == "numeric") {
      dEST <- EST
      dDEN <- d.dist
      weight <- W_[[jjj]]
    } else {
      dEST <- 0
      dDEN <- 0
      weight <- 0
    }
    
    dDEN[!is.finite(dDEN)] <- 0
    return(list(family = distn, weight = weight, temp_weight = weight/temp_weight_denom, params = EST, XMIN = XMIN, XMAX = XMAX, x = xx, fx = dDEN))
  }
  
  
  if(univ_fam == "unif"){
    
    distr <- fitdist(data, distr = univ_fam, method = "mle")
    x = seq(.min, .max, length = 1000)
    
    density.list <- list(list(family = univ_fam, weight = 1, temp_weight = 1, params = distr$estimate, XMIN = .min, XMAX = .max, x = x, fx = rep(0.001, 1000)))
    
  } else if(univ_fam %in% c("exp", "gamma", "invgamma", "llogis", "gumbel", "weibull", "lnorm", "norm", "mgamma", "mgumbel", "betasr")) {
    
    temp_weight_denom = 1
    
    W_ = data.table(weight = 1)
    setnames(W_, "weight", univ_fam)
    
    density.list <- list(buildDENlist(1))
    
  } else if(univ_fam == "ensemble"){
    
    # remove 0s
    W_ <- copy(weights)
    W_ <- W_[1, W_ != 0, with=FALSE]
    

    temp_weight_denom <- sum(weights[, !"betasr", with = F])
    
    density.list <- lapply(1:length(W_), buildDENlist)
    
    betasr_index <- lapply(1:length(W_), function(i){  if(density.list[[i]]$family == "betasr"){ return(i) }  }) %>% unlist
    
    if(!is.null(betasr_index)){
      
      density.list[[betasr_index]]$temp_weight <- 0
      
    }
    
    
  } else {
    
    message("Distribution not implemented. Choose ensemble, exp, gamma, invgamma, llogis, gumbel, weibull, lnorm, norm, mgamma, mgumbel, betasr")
    
  }
  
  return(density.list)
  
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
    
    if(dens$family == "betasr"){
      
      XMAX <<- dens$XMAX
      XMIN <<- dens$XMIN
      
      qbetasr_mod <- function(p, shape1, shape2) {
        (XMAX-XMIN)*qbeta(p, shape1 = dens$params["shape1"], shape2 = dens$params["shape2"]) + XMIN
      }
      
      weighted_fx <- dens$weight * qbetasr_mod(p = data, 
                                               shape1 = dens$params["shape1"], shape2 = dens$params["shape2"])  
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



rake_optim_37 <- function(par, sim, ga_u_37){
  
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

rake_return_37 <- function(par, sim){
  
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

rake_optim_28 <- function(par, sim, ga_u_28, ga_u_37){
  
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

rake_return_28 <- function(par, sim){
  
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

rake_optim_2500 <- function(par, sim, bw_u_2500){
  
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

rake_return_2500 <- function(par, sim){
  
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

rake_optim_500 <- function(par, sim, bw_u_2500){
  
  bw_par <- par
  
  bw_threshold <- 2500
  bw_rake_pt <- 500
  
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
  
  ratio_500_2500 <- 0.01634069 ## Using ratio of 500 / 2500
  
  e_ratio_500_2500 <- nrow(sim[bw < bw_rake_pt, ]) / nrow(sim[bw < 2500, ])
    
  sum(log(e_ratio_500_2500/ratio_500_2500))^2
  
  
}

rake_return_500 <- function(par, sim){
  
  bw_par <- par
  
  bw_threshold <- 2500
  bw_rake_pt <- 500
  
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