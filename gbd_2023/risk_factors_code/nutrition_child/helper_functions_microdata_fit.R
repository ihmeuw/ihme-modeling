

select_initial_weights <- function(distlist, initial_condition){
  # Select weights based on initial condition. Given n = the number of distributions in distlist, 
  # the first to nth sets of weights should have a different distribution given 90% of weight, 
  # and the n + 1 set of weights will be all distributions given equal weight
  
  if(initial_condition == (length(distlist)+1) ){
    weights = rep(1/length(distlist), length(distlist))
  } else if(initial_condition %in% 1:length(distlist)){
    weights = rep((1 - 0.9)/(length(distlist)-1), length(distlist))
    weights[[initial_condition]] <- 0.9
    
  } else {
    weights = runif(length(distlist),0,1)
    weights = weights/sum(weights)
  }
  
  return(weights)
}



create_CDF <- function(Data, nid_loc_yr_i) {
  cdfDATA <- ecdf(Data)
  CDF_Data_Params <- list()
  CDF_Data_Params$mn <- mean(Data, na.rm = T)
  CDF_Data_Params$variance <- var(Data,na.rm=T)
  CDF_Data_Params$XMIN <- min(Data, na.rm = T)
  CDF_Data_Params$XMAX <- max(Data, na.rm = T)
  CDF_Data_Params$x <- seq(CDF_Data_Params$XMIN, CDF_Data_Params$XMAX, length = 1000)
  CDF_Data_Params$fx <- 0*CDF_Data_Params$x
  CDF_Data_Params$cdfDATAYout <- cdfDATA(sort(CDF_Data_Params$x))
  CDF_Data_Params$nid <- nid_loc_yr_i
  print(paste(CDF_Data_Params$mn, CDF_Data_Params$variance, CDF_Data_Params$XMIN, CDF_Data_Params$XMAX))
  return(CDF_Data_Params)
}

find_bad_fits <- function(weights, distlist, CDF_Data_Params) {
  for(z in length(weights):1){
    LENGTH <- length(formals(unlist(distlist[[z]]$mv2par)))
    if (LENGTH==4) {
      est <- try((unlist(distlist[[z]]$mv2par(CDF_Data_Params$mn,CDF_Data_Params$variance,XMIN=CDF_Data_Params$XMIN,XMAX=CDF_Data_Params$XMAX))),silent=T)
    } else {
      est <- try((unlist(distlist[[z]]$mv2par(CDF_Data_Params$mn,CDF_Data_Params$variance))),silent=T)
    }
    if(class(est)=="try-error") {
      weights[z] <- 0
    }
  }
  return(weights)
}

rescale_to_one <- function(weights){
  
  weights = weights/sum(weights)
  
}

create_pdf <- function(distlist, weights, CDF_Data_Params) {
  for(z in 1:length(weights)){
    LENGTH <- length(formals(unlist(distlist[[z]]$mv2par)))
    if (LENGTH==4) {
      est <- try((unlist(distlist[[z]]$mv2par(CDF_Data_Params$mn,CDF_Data_Params$variance,XMIN=CDF_Data_Params$XMIN,XMAX=CDF_Data_Params$XMAX))),silent=T)
    } else {
      est <- try((unlist(distlist[[z]]$mv2par(CDF_Data_Params$mn,CDF_Data_Params$variance))),silent=T)
    }
    fxj <- try(distlist[[z]]$dF(CDF_Data_Params$x,est),silent=T)
    if(class(est)=="try-error") {
      fxj <- rep(0,length(CDF_Data_Params$fx))
    }
    fxj[!is.finite(fxj)] <- 0
    CDF_Data_Params$fx <- (CDF_Data_Params$fx + fxj*weights[[z]])
  }
  CDF_Data_Params$fx[!is.finite(CDF_Data_Params$fx)] <- 0
  CDF_Data_Params$fx[length(CDF_Data_Params$fx)] <- 0
  CDF_Data_Params$fx[1] <- 0
  den <- approxfun(CDF_Data_Params$x, CDF_Data_Params$fx, yleft=0, yright=0) #density function
  
  return(den)
}



integrate_pdf <- function(den, CDF_Data_Params) {
  CDF_Data_Params$cdfFITYout <- c()
  for(val in CDF_Data_Params$x) {
    v<-try(integrate(den, min(CDF_Data_Params$x), val)$value)
    if (class(v)=="try-error") {
      v <- NA
    }
    CDF_Data_Params$cdfFITYout <- append(CDF_Data_Params$cdfFITYout,v)
  }
  
  
  return(CDF_Data_Params)
} 



invert_cdfs <- function(CDF_Data_Params, binwidth = 0.001){
  #' @description approximates the inverse functions of the data_CDF and fit_CDF, 
  #' and calculates the x's at equally spaced y's to allow for tail method1 and method2 goodness-of-fit calculations.
  #' The equally spaced y-s and corresponding xs for the data_cdf and fit_cdf are stored in the object CDF_Data_Params    
  #' @param CDF_Data_Params 
  #' @param binwidth the size of each equally spaced bin between 0-1. Must be between 0-1. E.g. binwidth of 0.001 leads to 1000 equally spaced bins between 0-1 
  #' @return CDF_Data_Params with three additional list objects: 
  #' 1) CDF_Data_Params$cdfYout_tails - equally spaced Y's between 0-1 (e.g. 0.001, 0.002, 0.003...0.999)
  #' 2) CDF_Data_Params$cdfFITX_tails - corresponding X's of the fit_CDF at the values of cdfY_tails 
  #' 3) CDF_Data_Params$cdfDATAX_tails - corresponding X's of the data_cdf at the values of cdfY_tails
  
  # Must start the equally spaced Y-values after 0 and end at the maximum where the original cdFIT ends  
  min_y = binwidth
  max_y = max(CDF_Data_Params$cdfFITYout)
  CDF_Data_Params$cdfYout_tails <- seq(min_y,max_y,by=binwidth)
  
  # yleft needs to be very small and non-zero
  # yright needs to be almost but not one
  # rule specifies what to do with values less than the minimum/max of the data points informing the function 
  
  # ----- Fit CDF
  # Approximate cdf function 
  cdf_fit_func <- approxfun(x = CDF_Data_Params$x,
                            y = CDF_Data_Params$cdfFITYout, 
                            yleft = 0.00000001, yright = 0.99999999,
                            rule = 2)
  
  # Approximate the inverse of the cdf function
  inv_cdf_fit_func <- inverse(cdf_fit_func, lower = min(CDF_Data_Params$x)-1, upper = max(CDF_Data_Params$x)+1)
  
  # Find the corresponding X's for the cdfFIT, given the equally spaced Y's
  CDF_Data_Params$cdfFITX_tails <- unlist(lapply(CDF_Data_Params$cdfYout_tails, inv_cdf_fit_func))
  
  # ----- Data CDF
  # Approximate cdf functions
  cdf_data_func <- approxfun(x = CDF_Data_Params$x, 
                             y = CDF_Data_Params$cdfDATAYout, yleft = 0.00000001, yright = 0.99999999, rule = 2)
  
  # Approximate the inverse of the cdf function
  inv_cdf_data_func <- inverse(cdf_data_func, lower = min(CDF_Data_Params$x)-1, upper = max(CDF_Data_Params$x)+1)
  
  # Find the corresponding X's for the cdfDATA, given the equally spaced Y's
  CDF_Data_Params$cdfDATAX_tails <- unlist(lapply(CDF_Data_Params$cdfYout_tails, inv_cdf_data_func))
  
  return(CDF_Data_Params)
  
}

goodness_of_fit_calc <- function(Data, allData, CDF_Data_Params, weights, option, low_threshold, medium_threshold, high_threshold, low_tail, middle_tail, high_tail, low_weight, medium_weight, high_weight) {
  
  if (option == "thresholds_1"){ #old way, looking for the biggest difference at all anywhere in the distribution
    
    xloc <- which.max(abs(CDF_Data_Params$cdfDATAYout-CDF_Data_Params$cdfFITYout))
    ks <- abs(CDF_Data_Params$cdfDATAYout[xloc]-CDF_Data_Params$cdfFITYout[xloc])
    y0 <- CDF_Data_Params$cdfDATAYout[xloc]
    y1 <- CDF_Data_Params$cdfFITYout[xloc]
    xPOSITION <- CDF_Data_Params$x[which(CDF_Data_Params$cdfDATAYout == y0)[1]]
    weightfit <- list()
    weightfit$ks <- ks
    weightfit$weights <- weights
    weightfit$distnames <- names(distlist)
    return(weightfit)
    
  } else if (option == "thresholds_2") { # old way, looking for the biggest difference at all anywhere on the NEGATIVE Z score side of the distribution
    
    x_loc_at_zero <- which.min(abs(CDF_Data_Params$x - mean(Data)))
    
    data_under_zero <- CDF_Data_Params$cdfDATAYout[1:x_loc_at_zero]
    
    length_data <- length(data_under_zero)
    
    model_under_zero <- CDF_Data_Params$cdfFITYout[1:length_data]
    
    xloc <- which.max(abs(data_under_zero-model_under_zero))
    
    ts <- abs(data_under_zero[xloc]-model_under_zero[xloc])
    
    weightfit <- list()
    weightfit$ks <- ts
    weightfit$weights <- weights
    weightfit$distnames <- names(distlist)
    
    return(weightfit)
    
    
  } else if (option == "thresholds_3") { # looking for the biggest difference at one of the three thresholds, whichever has biggest diff
    
    
    x_minus_3 <- which.min(abs(CDF_Data_Params$x - (offset + low_threshold)))
    x_minus_2 <- which.min(abs(CDF_Data_Params$x - (offset + medium_threshold)))
    x_minus_1 <- which.min(abs(CDF_Data_Params$x - (offset + high_threshold)))
    
    
    if ((CDF_Data_Params$x[x_minus_3] > min(Data)) & (CDF_Data_Params$x[x_minus_3] < max(Data))) {
      
      diff_min_x_minus_3 <- abs(CDF_Data_Params$cdfDATAYout[x_minus_3]-CDF_Data_Params$cdfFITYout[x_minus_3])
      
    } else {
      diff_min_x_minus_3 <- 0
    }
    
    
    
    if ((CDF_Data_Params$x[x_minus_2] > min(Data)) & (CDF_Data_Params$x[x_minus_2] < max(Data))) {
      
      diff_min_x_minus_2 <- abs(CDF_Data_Params$cdfDATAYout[x_minus_2]-CDF_Data_Params$cdfFITYout[x_minus_2])
      
    }else {
      diff_min_x_minus_2 <- 0
    }
    
    
    
    if ((CDF_Data_Params$x[x_minus_1] > min(Data)) & (CDF_Data_Params$x[x_minus_1] < max(Data))) {
      
      diff_min_x_minus_1 <- abs(CDF_Data_Params$cdfDATAYout[x_minus_1]-CDF_Data_Params$cdfFITYout[x_minus_1])
      
    }else {
      diff_min_x_minus_1 <- 0
    }
    
    
    ts <- c(diff_min_x_minus_3, diff_min_x_minus_2, diff_min_x_minus_1)
    max_ts <- max(ts)
    
    
    if(max_ts > 0) {
      
    weightfit <- list()
    weightfit$ks <- max_ts
    weightfit$weights <- weights
    weightfit$distnames <- names(distlist)
    
    return(weightfit)
    
    } else if ( max_ts == 0) { #here if none of the data crosses the three thresholds, we're telling it to just do test stat 1
      
      xloc <- which.max(abs(CDF_Data_Params$cdfDATAYout-CDF_Data_Params$cdfFITYout))
      ks <- abs(CDF_Data_Params$cdfDATAYout[xloc]-CDF_Data_Params$cdfFITYout[xloc])
      y0 <- CDF_Data_Params$cdfDATAYout[xloc]
      y1 <- CDF_Data_Params$cdfFITYout[xloc]
      xPOSITION <- CDF_Data_Params$x[which(CDF_Data_Params$cdfDATAYout == y0)[1]]
      weightfit <- list()
      weightfit$ks <- ks
      weightfit$weights <- weights
      weightfit$distnames <- names(distlist)
      
      return(weightfit)
      
    }
    
    
    
    
  } else if (option == "thresholds_4") { # adding a weighted sum of the differences at the three thresholds
    
    x_minus_3 <- which.min(abs(CDF_Data_Params$x - (offset + low_threshold)))
    x_minus_2 <- which.min(abs(CDF_Data_Params$x - (offset + medium_threshold)))
    x_minus_1 <- which.min(abs(CDF_Data_Params$x - (offset + high_threshold)))
    
    if ((CDF_Data_Params$x[x_minus_3] > min(Data)) & (CDF_Data_Params$x[x_minus_3] < max(Data))) {
      
      diff_min_x_minus_3 <- abs(CDF_Data_Params$cdfDATAYout[x_minus_3]-CDF_Data_Params$cdfFITYout[x_minus_3])
      
    } else {
      diff_min_x_minus_3 <- 0
    }
    
    
    if ((CDF_Data_Params$x[x_minus_2] > min(Data)) & (CDF_Data_Params$x[x_minus_2] < max(Data))) {
      
      diff_min_x_minus_2 <- abs(CDF_Data_Params$cdfDATAYout[x_minus_2]-CDF_Data_Params$cdfFITYout[x_minus_2])
      
    } else {
      diff_min_x_minus_2 <- 0
    }
    
    
    if ((CDF_Data_Params$x[x_minus_1] > min(Data)) & (CDF_Data_Params$x[x_minus_1] < max(Data))) {
      
      diff_min_x_minus_1 <- abs(CDF_Data_Params$cdfDATAYout[x_minus_1]-CDF_Data_Params$cdfFITYout[x_minus_1])
      
    } else {
      diff_min_x_minus_1 <- 0
    }

    ts <- c(diff_min_x_minus_3, diff_min_x_minus_2, diff_min_x_minus_1)
    weights_for_ts_sum <- c(low_weight, medium_weight, high_weight)                          #these weights are for -3SD threshold, -2SD threshold, -1SD threshold in that order
    weighted_ts <- ts*weights_for_ts_sum
    weighted_ts_sum <- sum(weighted_ts)
    
    if (weighted_ts_sum > 0) {
    
    weightfit <- list()
    weightfit$ks <- weighted_ts_sum
    weightfit$weights <- weights
    weightfit$distnames <- names(distlist)
    
    return(weightfit)
    
    } else if (weighted_ts_sum == 0) {
      
      xloc <- which.max(abs(CDF_Data_Params$cdfDATAYout-CDF_Data_Params$cdfFITYout))
      ks <- abs(CDF_Data_Params$cdfDATAYout[xloc]-CDF_Data_Params$cdfFITYout[xloc])
      y0 <- CDF_Data_Params$cdfDATAYout[xloc]
      y1 <- CDF_Data_Params$cdfFITYout[xloc]
      xPOSITION <- CDF_Data_Params$x[which(CDF_Data_Params$cdfDATAYout == y0)[1]]
      weightfit <- list()
      weightfit$ks <- ks
      weightfit$weights <- weights
      weightfit$distnames <- names(distlist)
      
      return(weightfit)
      
    }
    
  } else if (option == "thresholds_5") { # adding a weighted sum of the SQUARED differences at the three thresholds
    
    x_minus_3 <- which.min(abs(CDF_Data_Params$x - (offset + low_threshold)))
    x_minus_2 <- which.min(abs(CDF_Data_Params$x - (offset + medium_threshold)))
    x_minus_1 <- which.min(abs(CDF_Data_Params$x - (offset + high_threshold)))
    
    if ((CDF_Data_Params$x[x_minus_3] > min(Data)) & (CDF_Data_Params$x[x_minus_3] < max(Data))) {
      
      diff_min_x_minus_3 <- abs(CDF_Data_Params$cdfDATAYout[x_minus_3]-CDF_Data_Params$cdfFITYout[x_minus_3])
      
    } else {
      diff_min_x_minus_3 <- 0
    }
    
    
    
    if ((CDF_Data_Params$x[x_minus_2] > min(Data)) & (CDF_Data_Params$x[x_minus_2] < max(Data))) {
      
      diff_min_x_minus_2 <- abs(CDF_Data_Params$cdfDATAYout[x_minus_2]-CDF_Data_Params$cdfFITYout[x_minus_2])
      
    } else {
      diff_min_x_minus_2 <- 0
    }
    
    
    
    if ((CDF_Data_Params$x[x_minus_1] > min(Data)) & (CDF_Data_Params$x[x_minus_1] < max(Data))) {
      
      diff_min_x_minus_1 <- abs(CDF_Data_Params$cdfDATAYout[x_minus_1]-CDF_Data_Params$cdfFITYout[x_minus_1])
      
    } else {
      diff_min_x_minus_1 <- 0
    }

    ts <- c(diff_min_x_minus_3, diff_min_x_minus_2, diff_min_x_minus_1)
    ts_squared <- ts^2
    weights_for_ts_sum <- c(low_weight, medium_weight , high_weight) #these weights are for -3SD threshold, -2SD threshold, -1SD threshold in that order
    weights_ts_squared_sum <- ts_squared * weights_for_ts_sum
    weighted_ts_sum <- sum(weights_ts_squared_sum)
    
    if (weighted_ts_sum > 0) {
    
    weightfit <- list()
    weightfit$ks <- weighted_ts_sum
    weightfit$weights <- weights
    weightfit$distnames <- names(distlist)
    
    return(weightfit)
    
    } else if (weighted_ts_sum == 0) {
      
      xloc <- which.max(abs(CDF_Data_Params$cdfDATAYout-CDF_Data_Params$cdfFITYout))
      ks <- abs(CDF_Data_Params$cdfDATAYout[xloc]-CDF_Data_Params$cdfFITYout[xloc])
      y0 <- CDF_Data_Params$cdfDATAYout[xloc]
      y1 <- CDF_Data_Params$cdfFITYout[xloc]
      xPOSITION <- CDF_Data_Params$x[which(CDF_Data_Params$cdfDATAYout == y0)[1]]
      weightfit <- list()
      weightfit$ks <- ks
      weightfit$weights <- weights
      weightfit$distnames <- names(distlist)
      
      return(weightfit)
      
    }
    
  } else if (option == "tails_1") {
    
    CDF_Data_Params <- invert_cdfs(CDF_Data_Params, binwidth = 0.001)
    
    max_index <- which.max(abs(CDF_Data_Params$cdfDATAX_tails - CDF_Data_Params$cdfFITX_tails))
    max_ts <- max(abs(CDF_Data_Params$cdfDATAX_tails - CDF_Data_Params$cdfFITX_tails))
    
    weightfit <- list()
    weightfit$ks <- max_ts
    weightfit$weights <- weights
    weightfit$distnames <- names(distlist)
    
    return(weightfit)
    
  } else if (option == "tails_2") {
    
    CDF_Data_Params <- invert_cdfs(CDF_Data_Params, binwidth = 0.001)
    
    cdfDATAX_tails_below_zero = CDF_Data_Params$cdfDATAX_tails[CDF_Data_Params$cdfFITX_tails < mean(Data)]
    cdfFITX_tails_below_zero = CDF_Data_Params$cdfFITX_tails[CDF_Data_Params$cdfFITX_tails < mean(Data)]
    
    max_index_below_zero <- which.max(abs(cdfDATAX_tails_below_zero - 
                                            cdfFITX_tails_below_zero))
    
    max_ts <- max(abs(cdfDATAX_tails_below_zero - 
                        cdfFITX_tails_below_zero))
    
    weightfit <- list()
    weightfit$ks <- max_ts
    weightfit$weights <- weights
    weightfit$distnames <- names(distlist)
    
    
    return(weightfit)
    
  } else if (option == "tails_3") { # looking for the biggest difference at one of the three tails, whichever has biggest diff
    
    
    y_low_tail_data <- which.min(abs(CDF_Data_Params$cdfDATAYout-low_tail))
    y_middle_tail_data <- which.min(abs(CDF_Data_Params$cdfDATAYout-middle_tail))
    y_high_tail_data <- which.min(abs(CDF_Data_Params$cdfDATAYout-high_tail))
    
    
    y_low_tail_model <- which.min(abs(CDF_Data_Params$cdfFITYout-low_tail))
    y_middle_tail_model <- which.min(abs(CDF_Data_Params$cdfFITYout - middle_tail))
    y_high_tail_model <- which.min(abs(CDF_Data_Params$cdfFITYout - high_tail))
    
    diff_min_y_low <- abs(CDF_Data_Params$x[y_low_tail_model]-CDF_Data_Params$x[y_low_tail_data])
    diff_min_y_middle <- abs(CDF_Data_Params$x[y_middle_tail_model]-CDF_Data_Params$x[y_middle_tail_data])
    diff_min_y_high <- abs(CDF_Data_Params$x[y_high_tail_model]-CDF_Data_Params$x[y_high_tail_data])
    
    ts <- c(diff_min_y_low, diff_min_y_middle, diff_min_y_high)
    max_ts <- max(ts)
    
    weightfit <- list()
    weightfit$ks <- max_ts
    weightfit$weights <- weights
    weightfit$distnames <- names(distlist)
    
    return(weightfit)
    
    
  } else if (option == "tails_4") { # adding a weighted sum of the differences at the three tails
    
    y_low_tail_data <- which.min(abs(CDF_Data_Params$cdfDATAYout-low_tail))
    y_middle_tail_data <- which.min(abs(CDF_Data_Params$cdfDATAYout-middle_tail))
    y_high_tail_data <- which.min(abs(CDF_Data_Params$cdfDATAYout-high_tail))
    
    
    y_low_tail_model <- which.min(abs(CDF_Data_Params$cdfFITYout-low_tail))
    y_middle_tail_model <- which.min(abs(CDF_Data_Params$cdfFITYout - middle_tail))
    y_high_tail_model <- which.min(abs(CDF_Data_Params$cdfFITYout - high_tail))
    
    diff_min_y_low <- abs(CDF_Data_Params$x[y_low_tail_model]-CDF_Data_Params$x[y_low_tail_data])
    diff_min_y_middle <- abs(CDF_Data_Params$x[y_middle_tail_model]-CDF_Data_Params$x[y_middle_tail_data])
    diff_min_y_high <- abs(CDF_Data_Params$x[y_high_tail_model]-CDF_Data_Params$x[y_high_tail_data])
    
    ts <- c(diff_min_y_low, diff_min_y_middle, diff_min_y_high)
    weights_for_ts_sum <- c(low_weight, medium_weight , high_weight) #these weights are for low tail, middle tail, high tail, in that order
    weighted_ts <- ts*weights_for_ts_sum
    weighted_ts_sum <- sum(weighted_ts)
    
    weightfit <- list()
    weightfit$ks <- weighted_ts_sum
    weightfit$weights <- weights
    weightfit$distnames <- names(distlist)
    
    return(weightfit)
    
  } else if (option == "tails_5") { # adding a weighted sum of the SQUARED differences at the three tails
    
    
    y_low_tail_data <- which.min(abs(CDF_Data_Params$cdfDATAYout-low_tail))
    y_middle_tail_data <- which.min(abs(CDF_Data_Params$cdfDATAYout-middle_tail))
    y_high_tail_data <- which.min(abs(CDF_Data_Params$cdfDATAYout-high_tail))
    
    
    y_low_tail_model <- which.min(abs(CDF_Data_Params$cdfFITYout-low_tail))
    y_middle_tail_model <- which.min(abs(CDF_Data_Params$cdfFITYout - middle_tail))
    y_high_tail_model <- which.min(abs(CDF_Data_Params$cdfFITYout - high_tail))
    
    diff_min_y_low <- abs(CDF_Data_Params$x[y_low_tail_model]-CDF_Data_Params$x[y_low_tail_data])
    diff_min_y_middle <- abs(CDF_Data_Params$x[y_middle_tail_model]-CDF_Data_Params$x[y_middle_tail_data])
    diff_min_y_high <- abs(CDF_Data_Params$x[y_high_tail_model]-CDF_Data_Params$x[y_high_tail_data])
    
    ts <- c(diff_min_y_low, diff_min_y_middle, diff_min_y_high)
    ts_squared <- ts^2
    weights_for_ts_sum <- c(low_weight, medium_weight , high_weight) #these weights are for low tail, middle tail, high tail, in that order
    weights_ts_times_sum <- ts_squared * weights_for_ts_sum
    weighted_ts_sum <- sum(weights_ts_times_sum)
    
    weightfit <- list()
    weightfit$ks <- weighted_ts_sum
    weightfit$weights <- weights
    weightfit$distnames <- names(distlist)
    
    return(weightfit)
    
    
  } else { 
    message("An invalid option: ", option, " was entered")
  }
}

iteration_weights_ks_tracking <- function(out_dir, optim_type, initial_condition, weightfit, nid_loc_yr_i) {
  
  files <- list.files(paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition))
  iteration = 1 + max(as.integer(unlist(lapply(files, function(f) { gsub(x = f, pattern = ".csv", replacement = "") } ))))
  write.csv(data.table(distname = weightfit$distnames, weights = weightfit$weights, ks = weightfit$ks, it.time = gsub("-", "_", Sys.time())), paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition, "/", iteration, ".csv"), row.names = F)
  
}  

global_ks_tracking <- function(allData, sum_KSs) {   
  for(nid_loc_yr_i in unique(allData$nid_loc_yr_index)) {
    files <- list.files(paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition))
    iteration = max(as.integer(unlist(lapply(files, function(f) { gsub(x = f, pattern = ".csv", replacement = "") } ))))
    it.data <- fread(paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition, "/", iteration, ".csv"))
    it.data[, globalks := sum_KSs]
    write.csv(it.data,paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition, "/", iteration, ".csv"), row.names = F)
  }
}

write_zero_csv <- function(out_dir, optim_type, initial_condition) {
  if(strategy == "m1"){
    dir.create(paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type), showWarnings = F)
    unlink(x = paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition), recursive = T)
    dir.create(paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition), recursive = T)
    write.csv(data.table(), paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition, "/0.csv"))
  }
  if(strategy == "m2"){
    for(nid_loc_yr_i in unique(allData$nid_loc_yr_index)) {
      dir.create(paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type), showWarnings = F)
      unlink(x = paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition), recursive = T)
      dir.create(paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition), recursive = T)
      write.csv(data.table(), paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition, "/0.csv"))
    }
  }
}



combine_fit <- function(KSs) {
  sum_KSs<- sum(KSs)
  return(sum_KSs)
}


aggregate_iterations <- function(out_dir, nid_loc_yr_i, optim_type, initial_condition, option, launch.date){
  
  files <- list.files(paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition))
  max_iteration = max(as.integer(unlist(lapply(files, function(f) { gsub(x = f, pattern = ".csv", replacement = "") } ))))
  
  dat <- lapply(1:max_iteration, function(iteration){
    
    weights.i <- fread(paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/", initial_condition, "/", iteration, ".csv")) 
    weights.i[, i := iteration]
    weights.i[, nid_loc_yr_i := nid_loc_yr_i]
    weights.i[, initial_condition := initial_condition]
    weights.i[, option := option]
    weights.i[, iteration_time := it.time]
    
    return(weights.i)
    
  })  %>% rbindlist()
  
  dat[, initial_condition := initial_condition]
  dir.create(paste0(out_dir,"/", nid_loc_yr_i, "/", optim_type, "/aggregates/"), showWarnings = F)
  dir.create(paste0(out_dir,"/", nid_loc_yr_i, "/", optim_type, "/aggregates/", launch.date, "/"), showWarnings = F)
  saveRDS(dat, paste0(out_dir, "/", nid_loc_yr_i, "/", optim_type, "/aggregates/",  launch.date, "/", initial_condition, ".rds"))
  
}



process_best_weights <- function(me_type, strategy, option, launch_date, launch_name){
  
  source("FILEPATH")
  
  distlist <- c(classA, classM) 
  
  if(me_type %in% c("bw", "ga")){
    distlist$invgamma <- NULL
    distlist$llogis <- NULL
  }
  
  all.ic.fp <- paste0("FILEPATH", launch_date, "/", me_type, "/", strategy, "/", option, "/1/sbplx/aggregates/", launch_name)
  all.ic.files <- list.files(all.ic.fp)
  
  all.ic.weights <- lapply(all.ic.files, function(file){
    single.ic <- readRDS(file.path(all.ic.fp, file))
    return(single.ic)
  }) %>% rbindlist(fill = T, use.names = T)
  
  all.ic.weights[ ,c("it.time", "nid_loc_yr_i") := NULL]
  
  best.ic.it <- all.ic.weights[globalks == min(globalks), ]
  best.ic.it <- best.ic.it[1:length(names(distlist))]
  
  best.weights = transpose(data.table(best.ic.it$weights))
  names(best.weights) <- names(distlist)
  
  dir.create(paste0("FILEPATH", me_type, "/", launch_date), recursive = TRUE)
  
  write.csv(best.weights, paste0("FILEPATH", me_type, "/", launch_date,  "/bestweights_", option, ".csv"), row.names = F)
  
}


select_best_weights <- function(me_type, launch_date, option, new.name, best_weight_output_fp){
  
  today.date = gsub("-", "_", Sys.Date())
  
  weight.set <- data.table::fread(paste0("FILEPATH", me_type, "/", launch_date,  "/bestweights_", option, ".csv"))
  
  
  existing.file <- file.exists()
  
  if(dir.exists(file.path(best_weight_output_fp, me_type)) == FALSE){
    
    dir.create(file.path(best_weight_output_fp, me_type), recursive = TRUE)
    
    
  }
  
  write.csv(weight.set, file.path(best_weight_output_fp, me_type, "final_weights.csv"), row.names = FALSE)
  
}

expand_weights <- function(me_type, release_id, weight_for_paf_calc_fp, estimation_years, location_set_id){
  
  ages <- ihme::get_age_metadata(release_id = release_id)
  
  age_group_ids <- unique(ages$age_group_id)
  
  sex_ids <- c(1,2)
  
  loc_meta <- ihme::get_location_metadata(location_set_id = location_set_id, release_id = release_id)
  
  loc_ids <- unique(loc_meta$location_id)
  
  expanded_age_sex <- expand.grid(age_group_ids, sex_ids, estimation_years, loc_ids)
  colnames(expanded_age_sex) <- c("age_group_id", "sex_id", "year_id", "location_id")
  
  best_weights <- data.table::fread(fs::path(
    "FILEPATH", me_type, "final_weights.csv"
  ))
  
  weights_for_paf_calc <- merge(expanded_age_sex, best_weights)
  
  if (me_type == "HAZ"){
    rei = "nutrition_stunting"
  } else if (me_type == "WAZ") {
    rei = "nutrition_underweight"
  } else if (me_type == "WHZ") {
    rei = "nutrition_wasting"
  } else (print("Invalid me_type"))
  
  write.csv(weights_for_paf_calc, paste0(weight_for_paf_calc_fp, rei, ".csv"), row.names = FALSE)
}








