
.fit_scam <- function(data,                       # Data table or data frame (assumes one location and age group)
                      response,                   # Identify response column by name
                      min_date=NULL,              # Start date of model prediction (if NULL, set to '2020-12-16')
                      max_date=NULL,              # Stop date of model prediction (if NULL set to max data date)
                      weights=NULL,               # A vector of weights on observations
                      use_weights=FALSE,          # Logical indicating whether or not to use total respondents as model weights
                      se_fit=FALSE,               # Logical indicating whether calculate standard errors
                      time_out=20,                # Amount of time in seconds before time out on scam
                      return_data_frame=FALSE     # Logical. If TRUE returns a model prediction as column added to other data. If FALSE, returns only vector of model predictions (or a list if se_fit = TRUE)   
                      ) {
  
  data <- as.data.frame(data)
  data$date <- as.Date(data$date)
  names(data)[names(data) == response] <- 'response'
  
  data[which(data$response <= 0), 'response'] <- 0.001
  data[which(data$response >= 1), 'response'] <- 0.999
  
  if (is.null(min_date)) min_date <- as.Date('2020-12-16')
  if (is.null(max_date)) max_date <- as.Date(max(data$date[!is.na(data$response)]))
  date_seq <- seq.Date(min_date, max_date, by=1)
  
  data <- merge(data.frame(date=as.Date(date_seq)), data, by='date', all.x=TRUE)
  data$location_id <- .get_column_val(data$location_id)
  data$age_group <- .get_column_val(data$age_group)
  
  # Set past dates to zero (necessary for locations/age-groups that start late and stay low)
  if (.get_column_val(data$location_id) %in% c(218)) {
    min_obs <- min(data$date[!is.na(data$response)])
    data$response[data$date < min_obs] <- 0
  }
  
  if (is.null(weights)) weights <- data$total_respondents_vaccination
  
  
  tryCatch({
    
    # Prep model object
    out <- data.frame(date =                       seq.Date(min_date, max_date, by=1),
                      location_id =               .get_column_val(data$location_id),
                      location_name =             .get_column_val(data$location_name),
                      parent_id =                 .get_column_val(data$parent_id),
                      region_id =                 .get_column_val(data$region_id),
                      region_name =               .get_column_val(data$region_name),
                      super_region_id =           .get_column_val(data$super_region_id),
                      super_region_name =         .get_column_val(data$super_region_name),
                      age_start =                 .get_column_val(data$age_start),
                      age_end =                   .get_column_val(data$age_end),
                      age_group =                 .get_column_val(data$age_group),
                      response =                   NA,
                      response_L95 =               NA,
                      response_H95 =               NA,
                      model_type =                 NA)
    

    f <- function(data, fam, use_weights) {
      if (use_weights) {
        scam::scam(response ~ s(as.integer(date), bs="mpi"), 
                   data=data, 
                   family=fam,
                   weights=weights,
                   control = scam::scam.control(maxit=1e9))
      } else {
      scam::scam(response ~ s(as.integer(date), bs="mpi"), 
                 data=data, 
                 family=fam,
                 control = scam::scam.control(maxit=1e9))
      }
    }
    
    mod <- tryCatch({
      
      tryCatch({
        
        # Monotonically increasing P-slpines with Quasibinomial likelihood
        model_type <- 'scam_quasibinomial'
        R.utils::withTimeout(f(data, fam='quasibinomial', use_weights=use_weights), 
                             timeout = time_out)
        
      }, TimeoutEdataception = function(t) {
        
        # Try Binomial model if Quasibinomial times out
        #model_type <- 'scam_binomial'
        #f(data, 'binomial', use_weights=use_weights)
        
        model_type <- 'gam_quasibinomial'
        mgcv::gam(response ~ s(as.integer(date), bs="tp"),
                  data=data,
                  family='quasibinomial')
      })
      
    }, warning = function(w) {
      
      model_type <- 'gam_quasibinomial'
      mgcv::gam(response ~ s(as.integer(date), bs="tp"),
                data=data,
                family='quasibinomial')
      
    }, error = function(e) {
      
      model_type <- 'gam_quasibinomial'
      mgcv::gam(response ~ s(as.integer(date), bs="tp"),
                data=data, 
                family='quasibinomial')
      
    })
    
    pred <- predict(mod, 
                    newdata=data.frame(date=as.integer(out$date)), 
                    type='response', 
                    se.fit=se_fit)
    
    # Janky but forcing this for now
    if ('gam' %in% class(mod)) {
      
      if (is.array(pred)) pred <- .make_cumulative(pred)
      if (is.list(pred)) pred <- lapply(pred, FUN=.make_cumulative)
      if (se_fit == FALSE) pred <- unlist(pred)
      
    }
    
    
    
    if (se_fit) {
      
    out$response <- pred$fit
    out$response_H95 <- pred$fit+pred$se.fit*1.96
    out$response_L95 <- pred$fit-pred$se.fit*1.96
    
    } else {
      
      out$response <- pred
    }
    
    out$response[out$response <= 0.001] <- 0
    
    out$model_type <- model_type
    
    
  }, error = function(e) {
    
    message(paste('ERROR:', unique(data$location_id), hierarchy[location_id == unique(data$location_id), 'location_name'], unique(data$age_group), conditionMessage(e)))
    
  })
  
  if (return_data_frame) {
    return(out)
  } else {
    return(pred)
  }
  
}



# Count version of the above function

.fit_scam_count <- function(data,                       # Data table or data frame (assumes one location and age group)
                            response,                   # Identify response column by name
                            se_fit=FALSE,               # Logical indicating whether calculate standard errors
                            return_data_frame=FALSE     # Logical. If TRUE returns a model prediction as column added to other data. If FALSE, returns only vector of model predictions (or a list if se_fit = TRUE)   
) {
  
  data <- as.data.frame(data)
  data$date <- as.Date(data$date)
  names(data)[names(data) == response] <- 'response'
  
  min_date <- as.Date('2020-12-16')
  max_date <- as.Date(max(data$date[!is.na(data$response)]))
  date_seq <- seq.Date(min_date, max_date, by=1)
  
  data <- merge(data.frame(date=as.Date(date_seq)), data, by='date', all.x=TRUE)
  data$location_id <- .get_column_val(data$location_id)
  data$age_group <- .get_column_val(data$age_group)
  
  # Set past dates to zero (necessary for locations/age-groups that start late and stay low)
  if (.get_column_val(data$location_id) %in% c(218)) {
    min_obs <- min(data$date[!is.na(data$response)])
    data$response[data$date < min_obs] <- 0
  }
  
  data$response <- as.integer(data$response)
  
  tryCatch({
    
    # Prep model object
    out <- data.frame(date =                       seq.Date(min_date, max_date, by=1),
                      location_id =               .get_column_val(data$location_id),
                      location_name =             .get_column_val(data$location_name),
                      parent_id =                 .get_column_val(data$parent_id),
                      region_id =                 .get_column_val(data$region_id),
                      region_name =               .get_column_val(data$region_name),
                      super_region_id =           .get_column_val(data$super_region_id),
                      super_region_name =         .get_column_val(data$super_region_name),
                      age_start =                 .get_column_val(data$age_start),
                      age_end =                   .get_column_val(data$age_end),
                      age_group =                 .get_column_val(data$age_group),
                      response =                   NA,
                      response_L95 =               NA,
                      response_H95 =               NA,
                      model_type =                 NA)
    
    
    mod <- tryCatch({
      
      # Monotonically increasing P-slpines with Quasibinomial likelihood
      model_type <- 'scam_poisson'
      scam::scam(response ~ s(as.integer(date), bs="mpi"), 
                 data=data, 
                 family='poisson',
                 control = scam::scam.control(maxit=1e6))
      
    }, warning = function(w) {
      
      model_type <- 'gam_poisson'
      mgcv::gam(response ~ s(as.integer(date), bs="gp"),
                data=data, 
                family='poisson',
                control = mgcv::gam.control(maxit=1e6))
      
    }, error = function(e) {
      
      model_type <- 'gam_poisson'
      mgcv::gam(response ~ s(as.integer(date), bs="gp"),
                data=data, 
                family='poisson',
                control = mgcv::gam.control(maxit=1e6))
      
    })
    
    pred <- predict(mod, 
                    newdata=data.frame(date=as.integer(out$date)), 
                    type='response', 
                    se.fit=se_fit)
    
    # Janky but forcing this for now
    if ('gam' %in% class(mod)) {
      
      if (is.array(pred)) pred <- .make_cumulative(pred)
      if (is.list(pred)) pred <- lapply(pred, FUN=.make_cumulative)
      if (se_fit == FALSE) pred <- unlist(pred)
      
    }
    
    if (se_fit) {
      
      out$response <- pred$fit
      out$response_H95 <- pred$fit+pred$se.fit*1.96
      out$response_L95 <- pred$fit-pred$se.fit*1.96
      
    } else {
      
      out$response <- pred
    }
    
    out$model_type <- model_type
    
    
  }, warning = function(w) {
    
    message(paste('WARNING:', i, hierarchy[location_id == i, 'location_name'], j, conditionMessage(w)))
    
  }, error = function(e) {
    
    message(paste('ERROR:', i, hierarchy[location_id == i, 'location_name'], j, conditionMessage(e)))
    
  })
  
  if (return_data_frame) {
    return(out)
  } else {
    return(pred)
  }
  
}


