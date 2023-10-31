
# Standarized methods for inferring vaccination quantities to zero berfore EUA date
# WHen EUA date not available, function will infer the regional average
# For adult age groups, first data >0 in observed all-age data is used


get_eua_dates <- function(vaccine_output_root, hierarchy) {
  
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  model_inputs_root <- model_parameters$model_inputs_path
  include_all_ages <- model_parameters$include_all_ages
  
  age_starts <- model_parameters$age_starts
  age_groups <- .get_age_groups(age_starts)
  age_ends <- as.numeric(stringr::str_split(age_groups, pattern='-', simplify=T)[,2])
  all_age_group <- paste0(age_starts[1], '-', 125)
  
  # Load data on date of Emergency Use Authorization for younger age groups
  message('Loading EUA data')
  eua_data <- .load_emergency_use_authorizations(model_inputs_root = model_inputs_root, use_data_intake = TRUE)

  message('Loading all-age vaccination data')
  observed_all_age <- vaccine_data$load_observed_vaccinations(vaccine_output_root)
  observed_all_age$date <- as.Date(observed_all_age$date)
  
  
  #-----------------------------------------------------------------------------
  # Date of first vaccinations in all age data (proxy for EUA in older age groups)
  #-----------------------------------------------------------------------------
  
  message('Calculating date of first vaccination for all-age data')
  f <- function(x) {
    data.frame(location_id=.get_column_val(x$location_id),
               date=as.Date(min(x$date[x$reported_vaccinations > 0])))
  }
  
  first_vaccination_dates <- .apply_func_location(object = observed_all_age, func = f)

  check <- function(a) {
    if (length(a) == 0 ) {
      return(TRUE)
    } else if (is.na(a)) {
      return(TRUE)
    } else {
      return(FALSE)
    }
  }

  message('Getting EUA dates for all locations and age groups...')
  out <- data.frame()
  
  for (i in hierarchy[level >= 3, location_id]) {
    for (j in age_groups) {
      
      age_start <- age_starts[age_groups == j]
      age_end <- age_ends[age_groups == j]

      # Check if EUA date is available
      tmp_eua_data <- eua_data[eua_data$location_id == i & eua_data$age_end >= age_start & eua_data$age_start <= age_end, ]
      
      if (nrow(tmp_eua_data) > 1) {
        
        if (all(is.na(tmp_eua_data$date))) {
          
          eua_date <- NA
          
        } else {
          
          eua_date <- min(tmp_eua_data$date, na.rm=T)
        }
        
      } else {
        
        eua_date <- tmp_eua_data$date
      }
      
      observed <- !check(eua_date)
      
      if (length(eua_date) == 0) eua_date <- NA
      if (check(eua_date) & age_end < 5) eua_date <- as.Date(Sys.Date()) # Assumes 0-4 still not approved when not provided
      
      if (check(eua_date) & age_start < 18) {
        
        eua_date <- mean(eua_data[eua_data$region_id == hierarchy[location_id == i, region_id], 'date'], na.rm=T)
        if (check(eua_date)) eua_date <- mean(eua_data[eua_data$super_region_id == hierarchy[location_id == i, super_region_id], 'date'], na.rm=T)
        if (check(eua_date)) eua_date <- mean(eua_data$date, na.rm=T)
        
      }
      
      if (check(eua_date) & age_start >= 18) eua_date <- first_vaccination_dates[first_vaccination_dates$location_id == i, 'date']
      if (check(eua_date) & age_start >= 18) eua_date <- as.Date('2020-12-16') + 5 # Should update to not be hardcoded here
      
      out <- rbind(out,
                   data.frame(
                     location_id = i,
                     location_name = hierarchy[location_id == i, location_name],
                     parent_id = hierarchy[location_id == i, parent_id],
                     region_id = hierarchy[location_id == i, region_id],
                     region_name = hierarchy[location_id == i, region_name],
                     age_start = age_start,
                     age_end = age_end,
                     age_group = j,
                     date = eua_date,
                     observed = as.integer(observed),
                     source = ifelse(length(tmp_eua_data$source[which.min(tmp_eua_data$date)]) == 0, 
                                     NA, 
                                     tmp_eua_data$source[which.min(tmp_eua_data$date)])
                   )
      )
      
    }
  }
  
  if (any(is.na(out$date))) warning("get_eua_dates() failed for some locations/age-groups")
  
  file_name <- file.path(vaccine_output_root, 'emergency_use_authorizations.csv')
  write.csv(out, file=file_name, row.names = F)
  message(paste0('EUA dates saved here: ', file_name))
  
}





# Function for setting vaccination quantities in particular age groups to zero prior to EUA date
# Expects a dataframe or datatable long by location-age

set_eua_dates <- function(object,
                          eua_dates=NULL,
                          vaccine_output_root=NULL,
                          vax_quantities
) {
  
  is_dt <- ifelse(is.data.table(object), T, F)
  if (is_dt) object <- as.data.frame(object)
  
  # Load data on date of Emergency Use Authorization for younger age groups
  if (is.null(eua_dates)) eua_dates <- fread(file.path(vaccine_output_root, 'emergency_use_authorizations.csv'))
  
  #x <- object[object$location_id == 4749 & object$age_group == '18-39',]
  
  out <- do.call(
    rbind,
    lapply(split(object, list(object$location_id, object$age_group)), function(x) {
      
      eua_date <- as.Date(eua_dates$date[eua_dates$location_id == .get_column_val(x$location_id) & eua_dates$age_group == .get_column_val(x$age_group)])
      sel <- x$date <= eua_date
      if (all(sel == FALSE)) {
        sel <- x$date <= (min(x$date, na.rm=T) + 3)
        #message(paste('Location ', .get_column_val(x$location_id), ' eua_date too early. Setting first 5 days to zero.'))
      }
      
      for (i in vax_quantities) x[[i]][sel] <- 0 
      return(x)
      
    }))
  
  row.names(out) <- NULL
  if (is_dt) out <- as.data.table(out)
  return(out)
}
