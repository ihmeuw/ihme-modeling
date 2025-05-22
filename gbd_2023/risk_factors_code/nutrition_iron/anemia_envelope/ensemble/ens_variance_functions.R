
library(data.table)
library(magrittr)
library(ggplot2)

source("FILEPATH/get_draws.R"  )
source("FILEPATH/pdf_families.R")

dlist <- c(classA, classM)

createMeanPrevList <- function(meids, location_ids, sex_ids, year_ids, age_group_ids, num_draws, decomp_step, gbd_round_id, date){
  
  # Get draws for all meids and reshape long
  draws <- lapply(meids, function(meid){
    
    path <- paste0("FILEPATH/",date,"/",meid)
    draws <- readRDS(paste0(path,"/",location_ids,".rds"))[sex_id==sex_ids & year_id==year_ids & age_group_id==age_group_ids]
    
    draws <- melt(draws, id.vars = c("location_id", "sex_id", "year_id", "age_group_id"), 
                  measure.vars =  paste0("draw_", 0:999), variable.name = "draw", value.name = paste0("meid_", meid) )
    
    return(draws)
    
  })
  
  # Merge into one data.table
  draws <- Reduce(merge,draws)
  
  # Rename draws
  draws <- draws[draw %in% paste0("draw_", 0:(num_draws-1))]
  
  # Split into list
  draws <- split(draws, by = c("location_id", "sex_id", "year_id", "age_group_id", "draw"), drop = TRUE)
  
  return(draws)
  
}


getOptimalSDs <- function(meanprev_list, mean_meid, prev_meids, min_allowed_sd, max_allowed_sd, weights, thresholds, threshold_weights, XMIN, XMAX) {
  
  meanprev_list <- lapply(meanprev_list, function(meanprev){
    
    # start seed as random number between min_allowed_sd and max_allowed_sd
    optim_sd <- try(optim(par = runif(1, min = min_allowed_sd, max = max_allowed_sd),
                          fn = calcEstimDensityError,
                          meanprev = meanprev,
                          mean_meid = mean_meid, 
                          prev_meids = prev_meids,
                          weights = weights,
                          thresholds = thresholds,
                          threshold_weights = threshold_weights,
                          method = "Brent",
                          lower = min_allowed_sd,
                          upper = max_allowed_sd,
                          XMIN = XMIN,
                          XMAX = XMAX,
                          control = list(maxit = 1000))$par, "try-error")
    
    if(inherits(optim_sd, what = "try-error")) optim_sd = as.numeric(NA)
    
    meanprev$sd <- optim_sd
    
    mean = meanprev[, get(paste0("meid_", mean_meid))]
    
    obs_prev <- calcPrevFromMeanSD(mean = mean, sd = optim_sd, weights = weights, thresholds = thresholds, XMIN = XMIN, XMAX = XMAX)
    
    lapply(1:length(obs_prev), function(i){   meanprev[, (paste0("est_meid_", prev_meids[i])) := obs_prev[i]]    })
    
    print(meanprev)
    
    return(meanprev)
    
  })
  
  return(meanprev_list)
  
}

calcEstimDensityError <- function(par, meanprev, mean_meid, prev_meids, weights, thresholds, threshold_weights, XMIN, XMAX) {
  
  sd = par
  
  print(sd)
  
  mean = meanprev[, get(paste0("meid_", mean_meid))]
  obs_prevs = sapply(prev_meids, function(meid) meanprev[, get(paste0("meid_", meid))])
  
  estim_prevs <- calcPrevFromMeanSD(mean = mean, sd = sd, weights = weights, thresholds = thresholds, XMIN = XMIN, XMAX = XMAX)
  
  sqr_error <- sum(threshold_weights*((obs_prevs - estim_prevs)^2))
  
  return(sqr_error)
  
}

getOptimalSDsPctError <- function(meanprev_list, mean_meid, prev_meids, min_allowed_sd, max_allowed_sd, weights, thresholds, threshold_weights, XMIN, XMAX) {
  
  meanprev_list <- lapply(meanprev_list, function(meanprev){
    
    # start seed as random number between min_allowed_sd and max_allowed_sd
    optim_sd <- try(optim(par = runif(1, min = min_allowed_sd, max = max_allowed_sd),
                          fn = calcEstimDensityPctError,
                          meanprev = meanprev,
                          mean_meid = mean_meid, 
                          prev_meids = prev_meids,
                          weights = weights,
                          thresholds = thresholds,
                          threshold_weights = threshold_weights,
                          method = "Brent",
                          lower = min_allowed_sd,
                          upper = max_allowed_sd,
                          XMIN = XMIN,
                          XMAX = XMAX,
                          control = list(maxit = 1000))$par, "try-error")
    
    if(inherits(optim_sd, what = "try-error")) optim_sd = as.numeric(NA)
    
    meanprev$sd <- optim_sd
    
    mean = meanprev[, get(paste0("meid_", mean_meid))]
    
    obs_prev <- calcPrevFromMeanSD(mean = mean, sd = optim_sd, weights = weights, thresholds = thresholds, XMIN = XMIN, XMAX = XMAX)
    
    lapply(1:length(obs_prev), function(i){   meanprev[, (paste0("est_meid_", prev_meids[i])) := obs_prev[i]]    })
    
    print(meanprev)
    
    return(meanprev)
    
  })
  
  return(meanprev_list)
  
}

calcEstimDensityPctError <- function(par, meanprev, mean_meid, prev_meids, weights, thresholds, threshold_weights, XMIN, XMAX) {
  
  sd = par
  
  print(sd)
  
  mean = meanprev[, get(paste0("meid_", mean_meid))]
  obs_prevs = sapply(prev_meids, function(meid) meanprev[, get(paste0("meid_", meid))])
  
  estim_prevs <- calcPrevFromMeanSD(mean = mean, sd = sd, weights = weights, thresholds = thresholds, XMIN = XMIN, XMAX = XMAX)
  
  sqr_error <- sum(threshold_weights*(((obs_prevs - estim_prevs)/obs_prevs)^2))
  
  return(sqr_error)
  
}



calcPrevFromMeanSD <- function(mean, sd, weights, thresholds, XMIN, XMAX){
  
  # Set as global variables
  XMIN <<- XMIN
  XMAX <<- XMAX
  
  ensemble_dens_func <- get_ensemble_density(weights = weights, mean = mean, sd = sd, XMIN = XMIN, XMAX = XMAX)
  
  # Calculate prevalence (integrate density) at each threshold 
  fx <- ensemble_dens_func(seq(XMIN, XMAX, length = 1000))
  prevs <- sapply(thresholds, function(threshold){
    prev <- try(integrate(ensemble_dens_func, lower = 0, upper = threshold)$value)
    
    if(class(prev) == "try-error" ){ 
      message("Bad ensemble")
      prev <- 0.999999 
    }
    
    return(prev)
    
  })
  
  return(prevs)
  
}



get_ensemble_density <- function(weights, mean, sd, XMIN, XMAX){

  x <- seq(XMIN, XMAX, length = 1000) # Create x with 1000 cut-points from XMIN to XMAX
  fx <- 0*x # Create fx "template" - temporarily put in 0's for each cut-point along the x vector
  
  # Loop through weights and calculate what the density (fx) would be at each x point. 
  # Then weight the fx by the weights. 
  # Sum all weighted fx's at the end to get the ensemble distribution
  
  for(z in 1:length(weights)){
    
    distr = names(weights)[z]
    
    
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
    
    # Apply weight for the distribution, and then add it to fx. The obj fx at the end of this for-loop will hold the sum of the weighted individual distribution -> which is the ensemble distribution!
    fx <- (fx + fxj*weights[[distr]])
  }
  
  fx[!is.finite(fx)] <- 0
  fx[length(fx)] <- 0
  fx[1] <- 0
  
  # Create a functional (mathematical function) form of the x and fx vectors
  ensemble_dens_func <- approxfun(x, fx, yleft=0, yright=0)
  
  return(ensemble_dens_func)
}
