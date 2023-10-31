

smooth_vaccine_hesitancy <- function(vaccine_output_root, 
                                     plot_timeseries=FALSE,
                                     plot_maps=FALSE
                                     
) {
  
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  model_inputs_path <- model_parameters$model_inputs_path
  previous_best_path <- model_parameters$previous_best_path
  
  start_date <- as.Date(model_parameters$data_start_date)
  yes_responses <- model_parameters$survey_yes_responses
  use_limited_use <- model_parameters$use_limited_use
  
  
  ##-------------------------------------------------------------------
  ## Import data ##
  message('Importing data')
  hierarchy <- gbd_data$get_covid_covariate_prep_hierarchy()
  
  #population <- model_inputs_data$load_total_population(model_inputs_path)
  population <- model_inputs_data$load_adult_population(model_inputs_path, model_parameters)
  setnames(population, 'adult_population', 'population')
  
  #-----------------------------------------------------------------------------
  # Use 5+ population for the following locations
  
  o5_locs <- c(6, 101, 156, 102) # China, Canada, UAE, US
  over5_population <- model_inputs_data$load_o5_population(model_inputs_path)
  setnames(over5_population, 'over5_population', 'population')
  sel <- which(population$location_id %in% o5_locs)
  population <- rbind(population[-sel,], over5_population[sel,])
  population <- population[order(location_id),]
  
  #-----------------------------------------------------------------------------
  
  .symptom_surveys <- model_inputs_data$load_symptom_survey_data(model_inputs_path, use_limited_use)
  us_states <- .symptom_surveys$us_states
  global <- .symptom_surveys$global
  us_states[, date := as.Date(date)]
  global[, date := as.Date(date)]
  
  
  ## Pull in the projected adults vaccinated
  projection <- vaccine_data$load_scenario_forecast(previous_best_path, 'slow')
  projection <- projection[, c("location_id", "date", "cumulative_all_vaccinated", "adult_population", "supply_and_delivery_doses")]
  
  #projection <- projection[location_id %in% c(global$location_id, us$location_id),]
  #projection[, pct_vaccinated := cumulative_all_vaccinated / adult_population]
  #projection[, prob_vaccine_access := supply_and_delivery_doses / adult_population]
  
  ## Pull in observed (vaccinated_pct should not be less than acceptance)
  observed <- vaccine_data$load_observed_vaccinations(previous_best_path)
  observed <- observed[date == max_date, c("location_id","people_vaccinated")]
  observed <- observed[!is.na(people_vaccinated)]
  observed <- merge(observed, projection[date == "2021-04-01", c("location_id","adult_population")], by = "location_id")
  
  
  
  ###########################################################################################
  # Global needs to add national, subnational data
  
  message('Processing survey responses')
  
  us <- us_states[variable == "V3"]
  us_yes <- us[value == 1]
  us_yes_probably <- us[value == 2]
  us_no_probably <- us[value == 3]
  us_no <- us[value == 4]
  
  setnames(us_yes, "prop_nm","getvaccine_yes")
  setnames(us_yes_probably, "prop_nm", "getvaccine_yesprobably")
  setnames(us_no_probably, "prop_nm", "getvaccine_noprobably")
  setnames(us_no, "prop_nm", "getvaccine_no")
  
  us <- merge(us_yes, us_yes_probably[,c("location_id", "date", "getvaccine_yesprobably")], by = c("location_id", "date"))
  us <- merge(us, us_no_probably[,c("location_id", "date", "getvaccine_noprobably")], by = c("location_id", "date"))
  us <- merge(us, us_no[,c("location_id", "date", "getvaccine_no")], by = c("location_id", "date"))
  
  us <- us[!is.na(location_id)]
  
  
  
  if (F) {
    loc <- 69
    grid.arrange(
      ggplot(global[variable == "V3" & location_id == loc]) +
        geom_point(aes(x=date, y=num, color=value)) +
        ggtitle(glue('{loc} responses to V3')),
      
      ggplot(global[variable == "V3a" & location_id == loc]) +
        geom_point(aes(x=date, y=num, color=value)) +
        ggtitle(glue('{loc} responses to V3a')),
      nrow=2
    )
  }
  
  
  #g_vaccinated <- global[variable == "V1" & value == 1]
  #global <- global[variable == "V3"]
  global <- global[variable %in% c("V3", "V3a")]
  dups <- duplicated(global[,.(location_id, date, value)])
  global <- global[!dups,]
  
  
  if (F) {
  ggplot(global[location_id == loc]) +
    geom_point(aes(x=date, y=num, color=value)) +
    ggtitle(glue('{loc} composite V3/V3a'))
  }
  
  
  global_yes <- global[value == 1]
  global_yes_probably <- global[value == 2]
  global_no_probably <- global[value == 3]
  global_no <- global[value == 4]
  
  #setnames(g_vaccinated, "prop_nm", "prop_vaccinated")
  setnames(global_yes, "prop_nm", "getvaccine_yes")
  setnames(global_yes_probably, "prop_nm", "getvaccine_yesprobably")
  setnames(global_no_probably, "prop_nm", "getvaccine_noprobably")
  setnames(global_no, "prop_nm", "getvaccine_no")
  
  global <- merge(global_yes, global_yes_probably[,c("location_id","date","getvaccine_yesprobably")],
                  by = c("location_id","date"))
  
  global <- merge(global, global_no_probably[,c("location_id","date","getvaccine_noprobably")],
                  by = c("location_id","date"))
  
  global <- merge(global, global_no[,c("location_id","date","getvaccine_no")],
                  by = c("location_id","date"))
  
  #global <- merge(global, g_vaccinated[,c("location_id","date","prop_vaccinated")],
  #                by = c("location_id","date"))
  
  global <- global[!is.na(location_id)]
  #global[, getvaccine_no := 1 - (getvaccine_yes + getvaccine_yesprobably + getvaccine_noprobably)]
  
  
  dt <- rbind(global, us, fill = T)
  
  
  # Bring in pct_vaccinated
  #sel <- which(projection$location_id %in% dt$location_id & projection$date <= as.Date(max(dt$date)))
  
  sel <- which(projection$location_id %in% hierarchy$location_id[hierarchy$most_detailed == 1 | hierarchy$level >= 3] & projection$date <= as.Date(max(dt$date)))
  projection <- projection[sel,]
  projection[, pct_vaccinated := cumulative_all_vaccinated / adult_population]
  dt <- merge(dt, projection, by = c("location_id", "date"), all=TRUE)
  
  sel <- which(projection$pct_vaccinated > 1)
  if (length(sel) > 0) {
    message('NOTE: The following lcoations have pct_vaccinated is > 1. These will be capped at 1.')
    tmp <- unique(projection[sel, location_id])
    print(unique(hierarchy[location_id %in% tmp, .(location_id, location_name)]))
  }
  
  sel <- which(projection$pct_vaccinated > 0.9)
  if (length(sel) > 0) {
    message('NOTE: The following locations have pct_vaccinated is > 0.9')
    tmp <- unique(projection[sel, location_id])
    print(unique(hierarchy[location_id %in% tmp, .(location_id, location_name)]))
  }
  
  
  
  
  #x <- dt[location_id == 81,]
  #plot(x$date, x$pct_vaccinated)
  
  #===============================================================================
  # From here fix survey_yes -> combined_yes swamp
  # Recalculate cumulative vaccinated and willing
  #===============================================================================
  
  message('Defining survey_yes')
  
  if (yes_responses == "yes_probably"){
    
    dt[, survey_yes := getvaccine_yes + getvaccine_yesprobably]
    
  } else if (yes_responses == "yes_definitely"){
    
    dt[, survey_yes := getvaccine_yes]
    
  } else if (yes_responses == "no_probably"){
    
    dt[, survey_yes := getvaccine_yes + getvaccine_yesprobably + getvaccine_noprobably]
    
  } else if (yes_responses == "no_definitely"){
    
    dt[, survey_yes := getvaccine_yes + getvaccine_yesprobably + getvaccine_noprobably + getvaccine_no]
    
  } else {
    
    stop('Unrecognized yes_responses argument')
  }
  
  
  ####################################################################################
  # Use regional mean in locations with poor data
  
  message('NOTE: Substituting regional mean for survey_yes in locations with poor data')
  
  tmp <- do.call(
    rbind,
    pbapply::pblapply(split(dt, by='location_id'), function(x) {
      
      tryCatch( {
        
        return(
          data.frame(location_id=unique(((x$location_id))),
                     n_obs=sum(!is.na(x$survey_yes)))
        )
        
      }, error = function(e){
        
        cat("Warning :", unique(x$location_id), ":", conditionMessage(e), "\n")
        
      })
      
      
    })
  )
  
  locs_poor_data <- tmp$location_id[tmp$n_obs < 10]
  
  locs_manual <- c(35, 39, 40, 49, 58, 59, 60, 77, 83, 35508, 35495, 87, 88, 367, 60896, 108, 198,
                   hierarchy[parent_id == 44538, location_id])
  
  locs <- unique(c(locs_poor_data, locs_manual))
  print(locs)
  
  
  dt <- merge(dt, hierarchy[,.(location_id, region_id)], by='location_id', all.x=TRUE)
  
  
  
  #x <- dt[location_id == 60896]
  
  tmp <- do.call(
    rbind,
    pbmcapply::pbmclapply(split(dt, by='location_id'), 
                          mc.cores = 20,
                          FUN=function(x) {
                            
                            tryCatch( { 
                              
                              loc <- unique(x$location_id)
                              
                              if (loc %in% locs) {
                                
                                reg <- dt[region_id == hierarchy[location_id == loc, region_id]]
                                reg <- aggregate(reg$survey_yes, by=list(reg$date), FUN=function(x) mean(x, na.rm=TRUE))
                                names(reg) <- c('date', 'survey_yes')
                                
                                x[,survey_yes := NULL]
                                x <- merge(x, reg, by='date', all.y=TRUE)
                                x$location_id <- loc
                                x$location_name <- hierarchy[location_id == loc, location_name]
                                
                                return(x)
                                
                              }
                              
                            }, error = function(e){
                              
                              cat("Warning :", unique(x$location_id), ":", conditionMessage(e), "\n")
                              
                            })
                          })
  )
  
  
  
  
  sel <- dt$location_id %in% tmp$location_id
  dt <- dt[!sel,]
  dt <- rbind(dt, tmp)
  dt[,region_id := NULL]
  dt[which(is.nan(survey_yes)), 'survey_yes'] <- NA
  
  dt <- dt[order(date)]
  dt <- dt[date > start_date]
  dt <- dt[, max_loc_date := max(date), by = "location_id"]
  dt <- dt[date < max_loc_date]
  
  # Try to smooth some locations more
  #dt <- dt[(getvaccine_yes > 0 & getvaccine_yes < 1)]
  dt[, location_rows := .N, by = "location_id"]
  dt <- dt[location_rows > 5]
  
  
  
  #-------------------------------------------------------------------------------
  # Adjust survey responses for the shift from wave 10 (pre-May20) to wave 11 (post-May20)
  # Take mean of "survey_yes" for last few days prior to May-20 and get difference from
  # few days mean post May-20. Use this bias to scale "survey_yes" post May-20
  #-------------------------------------------------------------------------------
  
  #x <- dt[location_name == 'Poland']
  
  
  message('Fix for wave 10 -> wave 11')
  dt <- do.call(
    rbind,
    pbapply::pblapply(split(dt, by='location_id'), function(x) {
      
      #message(unique(x$location_id))
      
      tryCatch( { # if data break model - return NAs and fill in after
        
        t <- as.Date("2021-05-20")
        delta_t <- 7 # match mean of week prior to mean of week post
        mean_prior <- mean(x[date >= t-delta_t & date <= t, survey_yes], na.rm=TRUE)
        mean_post <- mean(x[date > t & date <= t+delta_t, survey_yes], na.rm=TRUE)
        bias <- mean_prior - mean_post
        x[date > t, survey_yes := survey_yes + bias]
        
      }, error = function(e){
        
        cat("Warning :", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(e), "\n")
        
      })
      
      return(x)
    })
  )
  
  ##############################################################################################################
  # Location specific hard codes
  
  china_surrogate_id <- 69 # Singapore - DO NOT handle surrogates by name, use location_id
  china_surrogate <- hierarchy[location_id==china_surrogate_id, location_name] 
  
  china_and_subnats <- children_of_parents(6, hierarchy, "loc_ids", include_parent = TRUE) 
  
  message(glue("Using hesitancy from {china_surrogate} in China and subnationals:"))

  for (i in china_and_subnats) {
    
    message(i)

    tmp <- dt[location_id == i,]
    
    tmp <- merge(tmp[, survey_yes := NULL], 
          dt[location_id == china_surrogate_id, c('date', 'survey_yes')], 
          by='date', all.x=T)
    
    dt <- rbind(dt[!(dt$location_id == i),], tmp)
    
  }
  
  
  #   filter(location_id %in% children_of_parents(6, hierarchy, "loc_ids", include_parent = TRUE)) %>% 
  #   ggplot(aes(x=date, y=survey_yes)) +
  #   geom_line() +
  #   theme_minimal_hgrid() +
  # facet_wrap(~location_id) # all accounted for? 
  
  
  # India Other Union territories
  message('India Other Union territories inherit India national average pct_vaccinated')
  
  for (i in hierarchy[parent_id == 44538, location_id]) {
    
    message(i)
    dt[location_id == i, 'pct_vaccinated'] <- dt[location_id == 163, 'pct_vaccinated']
    
  }
  
  
    # filter(location_id %in% children_of_parents(163, hierarchy, "loc_ids", include_parent = TRUE)) %>% 
    # ggplot(aes(x=date, y=pct_vaccinated)) +
    # geom_line() +
    # theme_minimal_hgrid() +
    # facet_wrap(~location_id) # all accounted for? 
  
  
  ##############################################################################################################
  
  
  dt[which(dt$pct_vaccinated > 1), 'pct_vaccinated'] <- 1
  dt[which(dt$survey_yes > 1), 'survey_yes'] <- 1
  dt[which(dt$survey_yes < 0), 'survey_yes'] <- 0
  
  #x <- dt[location_id == 39,]
  
  message('Fitting GAM to survey_yes and calculating smooth_combined_yes')
  dt <- do.call(
    rbind,
    pbapply::pblapply(split(dt, by='location_id'), function(x) {
      
      tryCatch( { 
        
        if (all(is.na(x$pct_vaccinated))) { # If pct_vaccinated is missing grab from parent
          x <- merge(x[,pct_vaccinated := NULL], dt[location_id == hierarchy[location_id == x$location_id[1], parent_id], c('date', 'pct_vaccinated')], by='date')
        }
        
        if (all(is.na(x$survey_yes))) { # If pct_vaccinated is missing grab from parent
          x <- merge(x[,survey_yes := NULL], dt[location_id == hierarchy[location_id == x$location_id[1], parent_id], c('date', 'survey_yes')], by='date')
        }
        
        mod <- mgcv::gam(survey_yes ~ s(as.integer(date), bs="tp"),
                         data=x, 
                         family='quasibinomial')
        
        x$smooth_survey_yes <- predict(mod, 
                                       newdata=data.frame(date=as.integer(x$date)), 
                                       type='response', 
                                       se.fit=F)
        
        rm(mod)
        x$smooth_combined_yes <- x$pct_vaccinated + (1 - x$pct_vaccinated) * x$smooth_survey_yes
        as.data.table(x)
        
      }, error = function(e){
        
        cat("Warning :", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(e), "\n")
        
      })
    })
  )
  

  if (F) {

    x <- dt[location_id == 60896,]
    
    par(mfrow=c(1,3))
    plot(x$date, x$pct_vaccinated, ylim=c(0,1), main=x$location_name[1])
    lines(x$date, x$pct_vaccinated, col='red')
    abline(h=x$smooth_combined_yes[which.max(x$date)], col='purple')
    abline(h=x$pct_vaccinated[which.max(x$date)], col='red')
    plot(x$date, x$survey_yes, ylim=c(0,1))
    lines(x$date, x$smooth_survey_yes, col='blue')
    plot(x$date, x$smooth_combined_yes, ylim=c(0,1), col='purple')
    abline(h=x$smooth_combined_yes[which.max(x$date)], col='purple')
    par(mfrow=c(1,1))
    
  }
  
  
  sel <- complete.cases(dt[,c('pct_vaccinated', 'smooth_combined_yes')])
  if (any(dt$pct_vaccinated[sel] > dt$smooth_combined_yes[sel])) warning('pct_vaccinated > pct_vaccinated + willing')
  
  
  
  
  
  
  message('Regions and parents')
  
  ## Make national, regional aggregates
  dt <- merge(dt, hierarchy[, c("location_id","parent_id","region_id")], by = "location_id", all.x = T)
  dt[location_id == 891, parent_id := 102] # District of Columbia
  
  dt <- merge(dt, population, by = "location_id", all.x = T)
  dt[, smooth_combined_yes_count := smooth_combined_yes * population]
  
  # Regions, parents
  
  national_parents <- hierarchy[most_detailed == 0 & level == 3, location_id]
  national_parents <- national_parents[!(national_parents %in% unique(dt$location_id))] # empty
  
  nats <- dt[parent_id %in% national_parents, lapply(.SD, function(x) sum(x, na.rm = T)),
             by = c("parent_id","date"),
             .SDcols = c("population","smooth_combined_yes_count")]
  nats[, smooth_combined_yes := smooth_combined_yes_count / population]
  nats[, location_id := parent_id]
  nats[, parent_id := NULL]
  nats <- merge(nats, hierarchy[,c("location_id","parent_id","region_id","location_name")], by = "location_id")
  
  dt <- rbind(dt, nats, fill = T)
  
  
  regions <- dt[, lapply(.SD, function(x) sum(x, na.rm = T)),
                by = c("region_id","date"),
                .SDcols = c("smooth_combined_yes_count","population")]
  regions[, smooth_combined_yes := smooth_combined_yes_count / population]
  regions[, location_id := region_id]
  regions[, region_id := NULL]
  regions <- merge(regions, hierarchy[,c("location_id","parent_id","region_id","location_name")], by = "location_id")
  
  
  
  
  
  message('Filling missing locations with parent')
  
  
  ## Fill missing locs with parent
  missing_locs <- setdiff(hierarchy[most_detailed == 1, location_id], unique(dt$location_id)) # empty
  
  #na_locs <- do.call(c, lapply(split(dt, by='location_id'), function(x) if (all(is.na(x$smooth_combined_yes))) return(x$location_id[1])))
  
  
  missing_locs <- data.table(location_id = missing_locs)
  missing_locs <- merge(missing_locs, hierarchy[,c("location_id","parent_id")], by = "location_id")
  
  missing_locs <- merge(missing_locs, dt[,c("date","location_id","pct_vaccinated", "survey_yes", "smooth_survey_yes", "smooth_combined_yes")],
                        by.x = "parent_id", by.y = "location_id", all.x = T)
  missing_locs <- merge(missing_locs, hierarchy[,c("location_id","location_name")], by = "location_id")
  missing_locs[, parent_id := NULL]
  
  dt <- rbind(dt[, c("location_id", "date", "location_name", "pct_vaccinated", "survey_yes", "smooth_survey_yes", "smooth_combined_yes" )], missing_locs)
  
  
  if (F) {
    tmp <- dt[location_name %in% c('Singapore', 'Republic of Korea', 'China')]
    
    grid.arrange(
      
      ggplot(tmp) +
        geom_line(aes(x=date, y=survey_yes, color=location_name), size=1.5) +
        ggtitle('survey_yes'),
      
      ggplot(tmp) +
        geom_line(aes(x=date, y=smooth_combined_yes, color=location_name), size=1.5) +
        ggtitle('smooth_combined_yes')
      
    )
  }
  
  
  message('Applying hot-fixes')
  #-------------------------------------------------------------------------------
  # Set upper bound of 1 for hesitancy values
  
  dt$smooth_combined_yes[dt$smooth_combined_yes > 1] <- 1
  
  
  #-------------------------------------------------------------------------------
  # Fix skipped dates
  
  date_start <- as.Date(min(dt$date, na.rm=T))
  date_stop <- as.Date(max(dt$date, na.rm=T))
  
  f <- function(x) {
    
    tmp <- x[!is.na(x)]
    head_val <- tmp[1]
    tail_val <- tmp[length(tmp)]
    
    nas_index <- which(is.na(x))
    head_index <- min(which(x == head_val))
    tail_index <- max(which(x == tail_val))
    
    nas_lead <- nas_index[nas_index < head_index]
    nas_trail <- nas_index[nas_index > tail_index]
    
    x[nas_lead] <- head_val
    x[nas_trail] <- tail_val
    
    return(x)
  }
  
  
  
  dt <- do.call(
    rbind,
    pbapply::pblapply(split(dt, by='location_id'), function(x) {
      
      tryCatch( { # if data break model - return NAs and fill in after
        
        # Add any skipped dates
        x <- merge(x, data.table(date=seq.Date(date_start, date_stop, by=1)), all.y=TRUE)
        x$location_id <- .get_column_val(x$location_id)
        x$location_name <- .get_column_val(x$location_name)
        x$max_date <- date_stop
        
        x$smooth_combined_yes <- zoo::na.approx(x$smooth_combined_yes, na.rm=FALSE) # Interpolate NAs
        x$smooth_combined_yes <- f(x$smooth_combined_yes) # Extend end values out through head and tail
        
      }, error=function(e){
        
        cat("Warning :", unique(x$location_id), ":", conditionMessage(e), "\n")
        
      })
      
      return(x)
    })
  )
  
  if (sum(is.na(dt$location_id)) > 0) dt <- dt[!is.na(dt$location_id),]
  
  
    # filter(location_id %in% children_of_parents(6, hierarchy, "loc_ids", include_parent = TRUE)) %>% 
    # ggplot(aes(x=date, y=smooth_combined_yes)) +
    # geom_line() +
    # theme_minimal_hgrid() +
    # facet_wrap(~location_id) # all China and subnats accounted for? 
  
  #-------------------------------------------------------------------------------
  # Make a single time point prediction (mean of last week)
  
  dt[, max_date := max(date), by = "location_id"]
  
  dt_out <- dt[date > max_date - 5 & date <= max_date]
  
  dt_out <- dt_out[, lapply(.SD, function(x) mean(x)),
                   by = c("location_id","location_name"),
                   .SDcols = c("smooth_combined_yes")]
  
  ## Merge with observed ##
  dt_out <- merge(dt_out, observed, by = "location_id", all.x = T)
  
  
  
  ##-------------------------------------------------------------------
  # Save, plot, map
  message('Writing hesitancy models to file')
  vaccine_data$write_time_point_vaccine_hesitancy(dt_out, vaccine_output_root, 'default')
  vaccine_data$write_time_series_vaccine_hesitancy(dt, vaccine_output_root, 'default')
  
  
  sel <- which(dt$smooth_combined_yes < dt$pct_vaccinated)
  if (length(sel) > 0) {
    message('NOTE: The following locations have pct_vaccinated < pct_vaccinated + willing')
    tmp <- unique(projection[sel, location_id])
    print(unique(hierarchy[location_id %in% tmp, .(location_id, location_name)]))
  }
  
  #as.data.frame(dt[sel,])
  tmp <- dt[date == max_date,]
  
  #par(mfrow=c(1,1))
  #plot(tmp$smooth_combined_yes, tmp$pct_vaccinated,
  #     xlab='pct_vaccinated + willing',
  #     ylab='pct_vaccinated',
  #     main=glue('pct_vaccinated vs pct_vaccinated + willing on {tmp$max_date[1]}'))
  #abline(a=0, b=1, col='red')
  
  sel <- which(dt$pct_vaccinated > 1)
  if (length(sel) > 0) {
    warning('There are STILL observations where pct_vaccinated > 1')
    print(dt[sel,])
  }
  
  
  
  
  #-------------------------------------------------------------------------------
  
  # Load previous best model
  best_estimate <- vaccine_data$load_time_series_vaccine_hesitancy(previous_best_path, "default")
  
  # Data for scatter
  best_point <- vaccine_data$load_time_point_vaccine_hesitancy(previous_best_path, "default")
  scat_dt <- merge(best_point, dt_out, by = "location_id")
  
  
  pdf(file.path(vaccine_output_root, glue("vaccine_hesitancy_scatter_{yes_responses}.pdf")))
  
  # Comparison to previous model
  s <- ggplot(scat_dt, aes(x = smooth_combined_yes.x, y = smooth_combined_yes.y)) + geom_point() +
    geom_abline(intercept = 0, slope = 1, col = "red", lty = 2) + xlab("Previous Best") + ylab("New model") +
    theme_minimal() +
    geom_text_repel(data = scat_dt[abs(smooth_combined_yes.x - smooth_combined_yes.y) > 0.1], aes(label = location_name.x))
  
  print(s)
  
  dev.off()
  
  
  
  if (plot_timeseries) {
    
    message('Plotting hesitancy time series...')
    
    pal <- RColorBrewer::brewer.pal(9, 'Set1')
    
    pdf(file.path(vaccine_output_root, glue("vaccine_hesitancy_timeseries_{yes_responses}.pdf")), height=4, width=10, onefile=TRUE)
    
    
    for (loc in hierarchy[level >= 3, location_id]) {
      
      tmp <- dt[location_id == loc]
      tmp_best <- best_estimate[location_id == loc,]
      
      if (nrow(tmp) == 0 | nrow(tmp_best) == 0) {
        
        p <- plot_grid(grid::textGrob(paste(tmp$location_id[1], tmp$location_name[1], 'plot failed', sep=' | ')))
        
      } else {
        
        g <- ggplot(data=tmp, aes(x=date)) + ylim(0,1) + theme_minimal() 
        
        g <- plot_grid(
          
          g + 
            geom_hline(aes(yintercept=max(pct_vaccinated)), color=pal[1], lty=2) +
            geom_hline(aes(yintercept=tmp$smooth_combined_yes[which.max(tmp$date)]), color=pal[4], lty=2) +
            geom_hline(aes(yintercept=tmp_best$smooth_combined_yes[which.max(tmp_best$date)]), color='pink', lty=2) +
            geom_point(aes(y=pct_vaccinated), size=3, shape=1) +
            geom_line(aes(y=pct_vaccinated), color=pal[1], size=2) +
            ylab('Proportion vaccinated'),
          
          g + 
            geom_point(aes(y=survey_yes), size=3, shape=1) +
            #geom_line(data=best_estimate[location_id == loc,], aes(y=smooth_survey_yes), color='lightblue', alpha=0.5, size=2) +
            geom_line(aes(y=smooth_survey_yes), color='dodgerblue', size=2) +
            theme(legend.position = 'none') +
            ylab('Proportion unvaccinated willing'),
          
          g + 
            geom_hline(aes(yintercept=tmp$smooth_combined_yes[which.max(tmp$date)]), color=pal[4], lty=2) +
            geom_hline(aes(yintercept=tmp_best$smooth_combined_yes[which.max(tmp_best$date)]), color='pink', lty=2) +
            geom_line(data=tmp_best, aes(y=smooth_combined_yes), color='pink', alpha=0.5, size=2) +
            geom_line(aes(y=smooth_combined_yes), color=pal[4], size=2) +
            ylab('Proportion vaccinated OR willing'),
          
          ncol=3
        )
        
        p <- plot_grid(grid::textGrob(paste(tmp$location_id[1], tmp$location_name[1], sep=' | ')),
                       g,
                       ncol=1, 
                       rel_heights=c(0.05, 1))
        
      }
      
      print(p)
      
    }
    
    dev.off()
    
  }
  
  
  
  
  if (plot_maps) {
    
    if(yes_responses == "default"){
      
      ### Make a map ###
      lsvid <- 771
      location_id <- 1
      loc_id <- 1
      
      source(CODE_PATHS$MAPPING_FUNCTION_PATH)
      
      briefs_cols <- colorRampPalette(rev(c("#F74C00",  "#FEA02F",  "#EBD9C8","#007A7A",  "#002043")))
      colors_accept <- c("gray", briefs_cols(6))
      colors_reject <- c("gray", rev(briefs_cols(6)))
      
      # Remove parents for mapping
      # china needs a single value
      #dt_out <- rbind(dt_out,
      #                data.table(location_id = 6,
      #                           location_name = "China",
      #                           smooth_combined_yes = dt_out[location_id == 491]$smooth_combined_yes), fill = T)
      
      dt_out <- dt_out[!(location_id %in% hierarchy[parent_id %in% c(11, 196, 165, 135, 130, 6, 101, 86, 81, 92, 71), location_id])]
      dt_out <- dt_out[!(location_id %in% c(95, 102, 163))]
      
      dt_out[, bin := cut(smooth_combined_yes, breaks = c(0,50,60,65,70,75,100)/100,
                          labels = c("<50","50-59","60-64","65-69","70-74","75+"))]
      dt_out[, date := "2020-12-01"]
      
      pdf(file.path(vaccine_output_root, "vaccine_acceptance_smooth_map.pdf"), height = 5.5, width = 11)
      generic_map_function(dt_out, title = "Percent of adults who have been vaccinated\nor would or probably would accept vaccine", colors = colors_accept)
      dev.off()
      
      pdf(file.path(vaccine_output_root, "/vaccine_acceptance_smooth_map_us.pdf"), height = 5.5, width = 8)
      
      generic_map_function(dt_out[location_id %in% hierarchy[parent_id == 102, location_id]],
                           title = "Percent of adults who have been vaccinated\nor would or probably would accept vaccine", colors = colors_accept, loc_id = 102)
      dev.off()
      
      dt_out[, bin := cut((1-smooth_combined_yes), breaks = c(0,10,20,30,40,50,100)/100,
                          labels = c("<10","10-19","20-29","30-39","40-49","50+"))]
      dt_out[, date := "2020-12-01"]
      
      pdf(file.path(vaccine_output_root, "vaccine_rejection_smooth_map.pdf"), height = 5.5, width = 11)
      generic_map_function(dt_out, title = "Percent of adults who would reject or probably reject vaccine", colors = colors_reject)
      dev.off()
      
      ## Map data availability
      dt_avb <- dt[!is.na(smooth_combined_yes)]
      dt_avb <- data.table(location_id = unique(dt_avb$location_id),
                           bin = "Has survey data")
      
      dt_avb <- merge(hierarchy[level >= 3, c("location_id","location_name")],
                      dt_avb, by = "location_id", all.x = T)
      dt_avb <- dt_avb[!(location_id %in% hierarchy[parent_id %in% c(11, 196, 165, 135, 130, 6, 101, 86, 81, 92, 71), location_id])]
      dt_avb <- dt_avb[!(location_id %in% c(95, 102, 163))]
      dt_avb$date <- Sys.Date()
      
      generic_map_function(dt_avb, title = "Vaccine confidence availability", colors = c("gray", "purple"))
    }
  }
  
}
