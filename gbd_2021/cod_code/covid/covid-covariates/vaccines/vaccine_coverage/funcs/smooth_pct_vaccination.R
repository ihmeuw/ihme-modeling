
smooth_pct_vaccination <- function(vaccine_output_root, 
                                   hierarchy,
                                   population
) {
  
  # Get global args
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

  # Get observed NON-age-stratified vaccination coverage data
  observed_all_age <- vaccine_data$load_observed_vaccinations(vaccine_output_root)

  #date_range <- c(min(as.Date(observed_all_age$date)), Sys.Date())
  
  #-----------------------------------------------------------------------------
  # Clean up observed vax data
  #-----------------------------------------------------------------------------

  message('Inferring missing children locations in data')
  
  missing_children <- infer_missing_children(object = observed_all_age,
                                             objective_cols = c('people_vaccinated'),
                                             population = population,
                                             hierarchy = hierarchy,
                                             verbose = TRUE)
  
  observed_all_age <- rbind(observed_all_age, missing_children)
  
  
  observed_all_age <- merge(observed_all_age[,.(location_id, date, people_vaccinated)], 
                            population[population$age_group == all_age_group,], 
                            by=c('location_id'))
  
  observed_all_age$pct_vaccinated <- observed_all_age$people_vaccinated / observed_all_age$population
  
  
  
  #-----------------------------------------------------------------------------
  # Model vaccination coverage by age 'smooth_pct_vaccinated'
  #-----------------------------------------------------------------------------
  
  #data <- data[location_id == 1 & age_group == "18-64",]
  #x <- observed_all_age[location_id == 523,]
  
  .fit_vaccination_models <- function(data,             # data.frame long by location_id and age_group with data columns pct_vaccinated and total_respondents_vaccination
                                      level,            # level of spatial aggregation (expects 'global', 'super_region', 'region', 'parent', or 'location')
                                      #date_range,      # start and stops dates for model predictions 
                                      hierarchy,        # covid spatial hierarchy 
                                      n_cores=1
  ) {
    
    t_start <- Sys.time()
    
    message(glue('Estimating vaccination coverage at the {level} level'))
    if (!inherits(data$date, 'Date')) data$date <- as.Date(data$date)

    data <- set_spatial_scale(data = data, level = level, hierarchy = hierarchy)
    data[data$pct_vaccinated < 0 | data$pct_vaccinated > 1, 'pct_vaccinated'] <- NA
    
    min_date <- as.Date('2020-12-16')
    max_date <- as.Date(max(data$date[!is.na(data$pct_vaccinated)]))
    
    message(paste(unique(data$location_id), collapse=" | "))
    message('Fitting models...')
    
    f <- function(x) {

      tryCatch({
        
        # Prep model object
        out <- data.frame(date =                       seq.Date(min_date, max_date, by=1),
                          location_id =               .get_column_val(x$location_id),
                          location_name =             .get_column_val(x$location_name),
                          parent_id =                 .get_column_val(x$parent_id),
                          region_id =                 .get_column_val(x$region_id),
                          region_name =               .get_column_val(x$region_name),
                          super_region_id =           .get_column_val(x$super_region_id),
                          super_region_name =         .get_column_val(x$super_region_name),
                          fit_level =                  level,
                          age_start =                 .get_column_val(x$age_start),
                          age_end =                   .get_column_val(x$age_end),
                          age_group =                 .get_column_val(x$age_group),
                          smooth_pct_vaccinated =      NA,
                          smooth_pct_vaccinated_L95 =  NA,
                          smooth_pct_vaccinated_H95 =  NA)
        
        x <- x[,.(date, pct_vaccinated)]
        x <- x[complete.cases(x),]
        
        mod <- tryCatch({
          
          scam::scam(pct_vaccinated ~ s(as.integer(date), bs="mpi"), 
                     data=x, 
                     family='quasibinomial')
          
        }, error = function(e) {
          
          message(paste('Warning: location', i, 'scam failed. Using gam and forcing cumulative values.'))

          mgcv::gam(pct_vaccinated ~ s(as.integer(date), bs="gp"),
                    data=x, 
                    method="REML", 
                    family='quasibinomial')
        })
        
        pred <- predict(mod, 
                        newdata=data.frame(date=as.integer(out$date)), 
                        type='response', 
                        se.fit=T)
        
        if ('gam' %in% class(mod)) pred <- lapply(pred, FUN=.make_cumulative)
        
        out$smooth_pct_vaccinated <- pred$fit
        out$smooth_pct_vaccinated_H95 <- pred$fit+pred$se.fit*1.96
        out$smooth_pct_vaccinated_L95 <- pred$fit-pred$se.fit*1.96

        
        # Check
        #plot(x$date, x$pct_vaccinated, ylim=c(0,1))
        #lines(out$date, out$smooth_pct_vaccinated, col='red')
        
      }, warning = function(w) {
        
        message(paste('WARNING:', unique(out$location_id), unique(out$age_group), conditionMessage(w)))
        
      }, error = function(e) {
        
        message(paste('ERROR:', unique(out$location_id), unique(out$age_group), conditionMessage(e)))
        
      })
      
      return(out)

    }
    
    fitted_models <- .apply_func_location_age(object=data, func=f, n_cores=n_cores)
    
    
    row.names(fitted_models) <- NULL
    message('Done.')
    print(Sys.time() - t_start)
    return(fitted_models)
  }
  

  mods_all_age <- .fit_vaccination_models(data=observed_all_age, 
                                          level='location', 
                                          #date_range=date_range, 
                                          hierarchy=hierarchy, 
                                          n_cores=n_cores)
  
  
  row.names(mods_all_age) <- NULL
  head(mods_all_age)
  
  # Check
  sel <- c(570, 48, 59)
  ggplot() +
    geom_point(data = observed_all_age[observed_all_age$location_id %in% sel,], 
               aes(x=date, y=pct_vaccinated)) +
    geom_line(data = mods_all_age[mods_all_age$location_id %in% sel,], 
              aes(x=date, y=smooth_pct_vaccinated), color='red') +
    facet_grid(rows=vars(age_group), cols=vars(location_id))

  tmp <- file.path(vaccine_output_root, 'smooth_pct_vaccinated.csv')
  write.csv(mods_all_age, file=tmp, row.names = F)
  message(paste0('All-age vaccination coverage saved here: ', tmp))
  
}

