
smooth_pct_vaccination_age_survey <- function(vaccine_output_root,
                                              hierarchy,
                                              population,
                                              knock_out_locations=NULL
) {
  
  .print_header_message('Estimating vaccination coverage using age-stratified FB survey data')
  
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  model_inputs_path <- model_parameters$model_inputs_path
  
  age_starts <- model_parameters$age_starts
  include_all_ages <- model_parameters$include_all_ages
  n_cores <- model_parameters$n_cores
  
  all_age_start <- age_starts[1]
  all_age_end <- 125
  all_age_group <- paste0(all_age_start, '-', all_age_end)
  
  
  #-----------------------------------------------------------------------------
  # Load data
  #-----------------------------------------------------------------------------
  
  message('Loading data...')

  # Get Facebook vaccination coverage data. Produced by smooth_vaccine_hesitancy_age()
  fb <- fread(file.path(vaccine_output_root, 'observed_survey_data_age.csv'))
  fb$date <- as.Date(fb$date)
  
  # Get observed NON-age-stratified vaccination coverage data
  observed <- vaccine_data$load_observed_vaccinations(vaccine_output_root)
  observed$date <- as.Date(observed$date)
  
  # Load modeled vaccination coverage from all-age data
  mods_all_age <- fread(file.path(vaccine_output_root, "smooth_pct_vaccinated.csv"))
  mods_all_age$date <- as.Date(mods_all_age$date)
  
  # Get observed vaccination coverage by age from (CDC)
  observed_age <- fread(file.path(vaccine_output_root, "observed_vaccination_data_age.csv"))
  observed_age$date <- as.Date(observed_age$date)
  
  #date_range <- range(c(as.Date(observed_age$date), as.Date(fb$date)))
  
  #-----------------------------------------------------------------------------
  # Clean up observed vaccination by age data
  #-----------------------------------------------------------------------------
  
  # Merge age group populations
  population <- population[,!colnames(population) %in% c('age_start', 'age_end')]
  observed_age <- merge(observed_age, population, by=c('location_id', 'age_group'))
  observed_age$pct_vaccinated <- observed_age$initially_vaccinated/observed_age$population
  
  fb$pct_vaccinated <- fb$vaccinated/fb$total_respondents_vaccination
  fb$pct_vaccinated[fb$total_respondents_vaccination == 0] <- NA
  fb$pct_vaccinated[is.nan(fb$pct_vaccinated)] <- NA
  fb$pct_vaccinated <- .do_manual_bounds_proportion(fb$pct_vaccinated)
  
  #fb <- fb[fb$date >= as.Date("2020-12-16"),]
  
  
  #-----------------------------------------------------------------------------
  # Set EUA dates
  #-----------------------------------------------------------------------------
  
  message('Setting EUA dates for pct_vaccinated')
  
  # Load data on date of Emergency Use Authorization for younger age groups
  emergency_use_authorizations <- .load_emergency_use_authorizations(model_inputs_root = model_inputs_path, use_data_intake = TRUE)
  
  f <- function(x) {
    data.frame(location_id=.get_column_val(x$location_id),
               date=as.Date(min(x$date[x$reported_vaccinations > 0])))
  }
  
  first_vaccination_dates <- .apply_func_location(object = observed, func = f)
  
  
  #x <- fb[fb$location_id == 19 & fb$age_group == '18-39',]
  
  fb <- do.call(
    rbind,
    lapply(split(fb, by=c('location_id', 'age_group')), function(x) {
      
      x_loc <- .get_column_val(x$location_id)
      x_age <- .get_column_val(x$age_group)
      
      # Check if EUA date is available
      eua_date <- emergency_use_authorizations[emergency_use_authorizations$location_id == x_loc & emergency_use_authorizations$age_group == x_age, 'date']

      check <- function(a) {
        if (length(eua_date) == 0) {
          return(TRUE)
        } else if (is.na(eua_date)) {
          return(TRUE)
        } else {
          return(FALSE)
        }
      }
      
      # If 0-4 not available, assume not approved yet
      if (check(eua_date) & .get_column_val(x$age_group == '0-4')) eua_date <- as.Date(Sys.Date()) # Assumes 0-4 still not approved when not provided
      
      # If other child age groups not available, get regional mean
      if (check(eua_date) & .get_column_val(x$age_start) < 18) {
        eua_date <- mean(emergency_use_authorizations[emergency_use_authorizations$region_id == hierarchy[location_id == x_loc, region_id], 'date'], na.rm=T)
        if (check(eua_date)) eua_date <- mean(emergency_use_authorizations[emergency_use_authorizations$super_region_id == hierarchy[location_id == x_loc, super_region_id], 'date'], na.rm=T)
        if (check(eua_date)) eua_date <- mean(emergency_use_authorizations$date, na.rm=T)
      }
      
      # If Adult not available, use first observed or set first week to zero
      if (check(eua_date) & .get_column_val(x$age_start) >= 18) eua_date <- first_vaccination_dates[first_vaccination_dates$location_id == x_loc, 'date']
      if (check(eua_date) & .get_column_val(x$age_start) >= 18) eua_date <- as.Date(min(x$date)) + 7
      
      x[x$date < as.Date(eua_date), 'pct_vaccinated'] <- 0
      
      return(x)
      
    }))
  
  
  
  # Manual check
  if (FALSE) {
    sel <- c(33, 48, 218, 81, 570, 71)
    
    g <- ggplot() +
      geom_point(data = fb[fb$location_id %in% c(19, 33, 48, 218, 570, 71),], 
                 aes(x=date, y=pct_vaccinated)) +
      facet_grid(rows=vars(age_group), cols=vars(location_id))
    print(g)
  }
  
  
  
  
  #-----------------------------------------------------------------------------
  # Model vaccination coverage by age 'smooth_pct_vaccinated'
  #-----------------------------------------------------------------------------
  
  .fit_vaccination_age_models <- function(data,               # data.frame long by location_id and age_group with data columns pct_vaccinated and total_respondents_vaccination
                                          level,              # level of spatial aggregation (expects 'global', 'super_region', 'region', 'parent', or 'location')
                                          use_weights,        # Logical indicating whether or not to use 'total_respondents_vaccination' as the sample size weights
                                          hierarchy,     # covid spatial hierarchy 
                                          time_out=20,        # Number of seconds to wait before trying binomial likelihood
                                          n_cores
  ) {
    
    message(level)
    t_start <- Sys.time()
    if (!inherits(data$date, 'Date')) data$date <- as.Date(data$date)

    
    if (level == 'parent') {
      sel <- data$parent_id %in% data$location_id
      duplicate_parents <- unique(data$parent_id[sel])
      message('The following parent locations have location-level vaccination data. Parent-level aggregate models will not be fitted:')
      message(paste(duplicate_parents, collapse = ' | '))
    }
    
    data <- set_spatial_scale(data = data, level = level, hierarchy = hierarchy) # pool data based on the spatial level
    
    if (level == 'parent') data <- data[!(data$location_id %in% duplicate_parents),]
    
    #data[data$pct_vaccinated < 0 | data$pct_vaccinated > 1, 'pct_vaccinated'] <- NA # Crude ignore of out-of-bounds
    
    suppressMessages(cl <- parallel::makeCluster(n_cores))
    doParallel::registerDoParallel(cl)
    
    fitted_models <- 
      foreach(i = unique(data$location_id), .combine=rbind) %:%
      foreach(j = unique(data$age_group), .combine=rbind, 
              .packages = c('scam', 'mgcv', 'R.utils', 'data.table'), 
              .export = c('.get_column_val', '.make_cumulative')) %dopar% {
                
                x <- data[data$location_id == i & data$age_group == j,]
                
                x$pct_vaccinated[x$pct_vaccinated == 0] <- 0.001
                x$pct_vaccinated[x$pct_vaccinated == 1] <- 0.999
                #fit_mods <- function(x) {
                
                tryCatch({
                  
                  # Prep model object
                  out <- data.frame(date =                       seq.Date(as.Date("2020-12-16"), max(x$date), by=1),
                                    location_id =               .get_column_val(x$location_id),
                                    location_name =             .get_column_val(x$location_name),
                                    parent_id =                 .get_column_val(x$parent_id),
                                    region_id =                 .get_column_val(x$region_id),
                                    region_name =               .get_column_val(x$region_name),
                                    super_region_id =           .get_column_val(x$super_region_id),
                                    super_region_name =         .get_column_val(x$super_region_name),
                                    age_start =                 .get_column_val(x$age_start),
                                    age_end =                   .get_column_val(x$age_end),
                                    age_group =                 .get_column_val(x$age_group),
                                    smooth_pct_vaccinated =      NA,
                                    smooth_pct_vaccinated_L95 =  NA,
                                    smooth_pct_vaccinated_H95 =  NA,
                                    fit_level =                  level,
                                    model_type =                 NA)
                  
                  
                  
                  f <- function(x, fam, use_weights=FALSE) {
                    
                    if (use_weights) {
                      
                      scam::scam(pct_vaccinated ~ s(as.integer(date), bs="mpi"), 
                                 data=x, 
                                 family=fam,
                                 weights=x$total_respondents_vaccination)
                      
                    } else {
                      
                      scam::scam(pct_vaccinated ~ s(as.integer(date), bs="mpi"), 
                                 data=x, 
                                 family=fam)
                    }
                  }
                  
                  
                  mod <- tryCatch({
                    
                    # Monotonically increasing P-spines with Quasibinomial likelihood
                    model_type <- 'scam_quasibinomial'
                    R.utils::withTimeout(f(x, fam='quasibinomial', use_weights=use_weights), 
                                         timeout = time_out)
                    
                  }, TimeoutException = function(t) {
                    
                    # Try Binomial model if Quasibinomial times out
                    model_type <- 'scam_binomial'
                    f(x, 'binomial', use_weights=use_weights)
                    
                  }, warning = function(w) {
                    
                    model_type <- 'gam_quasibinomial'
                    mgcv::gam(pct_vaccinated ~ s(as.integer(date), bs="gp"),
                              data=x, 
                              method="REML", 
                              family='quasibinomial')
                    
                  }, error = function(e) {
                    
                    model_type <- 'gam_quasibinomial'
                    mgcv::gam(pct_vaccinated ~ s(as.integer(date), bs="gp"),
                              data=x, 
                              method="REML", 
                              family='quasibinomial')
                    
                  })
                  
                  pred <- predict(mod, 
                                  newdata=data.frame(date=as.integer(out$date)), 
                                  type='response', 
                                  se.fit=T)
                  
                  pred$fit[pred$fit < 0.01] <- 0
                  if ('gam' %in% class(mod)) pred <- lapply(pred, FUN=.make_cumulative) # Forcing this as recourse for failed mods for now
                  
                  out$smooth_pct_vaccinated <- pred$fit
                  out$smooth_pct_vaccinated_H95 <- pred$fit+pred$se.fit*1.96
                  out$smooth_pct_vaccinated_L95 <- pred$fit-pred$se.fit*1.96
                  out$model_type <- model_type
                  
                }, warning = function(w) {
                  
                  message(paste('WARNING:', i, hierarchy[location_id == i, 'location_name'], j, conditionMessage(w)))
                  
                }, error = function(e) {
                  
                  message(paste('ERROR:', i, hierarchy[location_id == i, 'location_name'], j, conditionMessage(e)))
                  
                })
                
                #plot(x$date, x$pct_vaccinated)
                #lines(out$date, out$smooth_pct_vaccinated, col='red')
                
                out
                
                #return(out)
              }
    
    stopCluster(cl)
    
    #fitted_models <- .apply_func_location_age(object = data, func = fit_mods, n_cores = n_cores)
    
    row.names(fitted_models) <- NULL
    message('Done.')
    print(Sys.time() - t_start)
    return(fitted_models)
  }
  
  # Fit vaccination coverage by age at each spatial scale
  mods_age <- rbind(
    .fit_vaccination_age_models(data=fb, level='global', use_weights=F, hierarchy=hierarchy, time_out=5*60, n_cores=length(age_starts)+1),
    .fit_vaccination_age_models(data=fb, level='super_region', use_weights=F, hierarchy=hierarchy, time_out=30, n_cores=n_cores),
    .fit_vaccination_age_models(data=fb, level='region', use_weights=F, hierarchy=hierarchy, n_cores=n_cores),
    .fit_vaccination_age_models(data=fb, level='parent', use_weights=F, hierarchy=hierarchy, n_cores=n_cores),
    .fit_vaccination_age_models(data=fb, level='location', use_weights=F, hierarchy=hierarchy, n_cores=n_cores)
  )
  
  row.names(mods_age) <- NULL
  head(mods_age)
  
  #tmp <- mods_age[mods_age$location_id == 64 & mods_age$age_group == '5-11',]
  #plot(tmp$date, tmp$smooth_pct_vaccinated)
  
  #-----------------------------------------------------------------------------
  # Infer models in locations that are missing
  # Must be sequential to take advantage of the spatial hierarchy
  #-----------------------------------------------------------------------------
 
    message('Inferring missing models using parent locations...')
    
    ################################################################################
    # Do manual knock-outs: these locations will inherit a parent/region level model
    ################################################################################
    
    if (!is.null(knock_out_locations)) {
      message('Knocking out the following locations (these will inherit a parent-level model):')
      message(paste(knock_out_locations, collapse=' | '))
      sel <- mods_age$location_id %in% knock_out_locations
      mods_age <- mods_age[!sel,]
    }
    
    ################################################################################
    ################################################################################
    
    variable <- 'smooth_pct_vaccinated'
    variable_col <- which(colnames(mods_age) == variable)
    if (length(variable_col) == 0) stop('variable not found in mods')
    if (is.null(hierarchy)) hierarchy <- gbd_data$get_covid_modeling_hierarchy()
    
    check <- function(x) nrow(x) == 0 | all(is.na(x[,variable_col]))
    
    cl <- parallel::makeCluster(n_cores)
    doParallel::registerDoParallel(cl)
    
    out <- 
      foreach(i = hierarchy[, location_id], .combine=rbind) %:% 
      foreach(j = unique(mods_age$age_group), .combine=rbind, .packages=c('data.table', 'glue')) %do% {

        x <- mods_age[mods_age$location_id == i & mods_age$age_group == j,]
        
        # If the location is missing from models, find a parent model as substitute
        #if (check(x)) x <- mods_age[mods_age$location_id == hierarchy[location_id == i, parent_id] & mods_age$age_group == j,]
        if (check(x)) x <- mods_age[mods_age$location_id == hierarchy[location_id == i, region_id] & mods_age$age_group == j,]
        if (check(x)) x <- mods_age[mods_age$location_id == hierarchy[location_id == i, super_region_id] & mods_age$age_group == j,]
        if (check(x)) x <- mods_age[mods_age$location_id == 1 & mods_age$age_group == j,]
        if (check(x)) warning(glue('{i} | {j} | cannot find model'))
        
        x$location_id <- i
        x
        
      }
    
    stopCluster(cl)
    
    out <- .add_spatial_metadata(data=out, hierarchy=hierarchy)
    row.names(out) <- NULL
    
    # Check duplicates
    #if (sum(duplicated(out)) > 0) out <- out[!duplicated(out),]
    
    
    # Infer missing locations
    
    #out <- do_model_spatial_extrapolation(mods=mods_age, 
    #                                      variable='smooth_pct_vaccinated', 
    #                                      knock_out_locations=knock_out_locations,
    #                                      hierarchy=hierarchy)
    
    # Check for duplicates (there can be overlap between location and parent aggregates)
    dup_locs <- unique(out[duplicated(out[,c('location_id', 'date', 'age_group')]), 'location_id'])
    message(glue('Found these duplicated locations in mods_age:'))
    message(paste(dup_locs, collapse=' | '))
    message('Filtering for location level fits...')
    
    for (i in dup_locs) {
      for (j in unique(out$age_group)) {
        
        message(paste(i, j, sep=' | '))
        sel <- out$location_id == i & out$age_group == j
        tmp <- out[sel,]
        
        if ('location' %in% unique(tmp$fit_level)) {
          
          out <- rbind(out[!sel,], tmp[tmp$fit_level == 'location',])
        } else {
          warning(glue('Location {i} | age group {j} is duplicated but there is no model fit at location level'))
        }
        
      }
    }
    
    dup_locs <- unique(out[duplicated(out[,c('location_id', 'date', 'age_group')]), 'location_id'])
    if (length(dup_locs) > 0) warning(glue("Could not resolve location-age duplicates for: {paste(dup_locs, collapse=' ')}"))
    
    
    # Manual check
    if (FALSE) {
      sel <- c(1, 31, 32, 45, 570, 218, 71, 11, 92)
      g <- ggplot() +
        geom_point(data = fb[fb$location_id %in% sel,], 
                   aes(x=date, y=pct_vaccinated)) +
        geom_line(data = mods_age[mods_age$location_id %in% sel,], 
                  aes(x=date, y=smooth_pct_vaccinated), col='red') +
        geom_line(data = out[out$location_id %in% sel,], 
                  aes(x=date, y=smooth_pct_vaccinated), col='blue') +
        facet_grid(rows=vars(age_group), cols=vars(location_name))
      print(g)
    }
    
    
    # Check missing locations
    message('Could not infer models for the following locations (okay to return global here):')
    covid_locs <- hierarchy$location_id
    sel <- which(!(covid_locs %in% out$location_id))
    print(hierarchy[sel, .(location_id, location_name)])
    
    # Save
    tmp <- file.path(vaccine_output_root, 'smooth_pct_vaccinated_age.csv')
    write.csv(out, file=tmp, row.names = F)
    message(paste0('Vaccination coverage by age (using FB survey data) saved here: ', tmp))
    
  }
  
  