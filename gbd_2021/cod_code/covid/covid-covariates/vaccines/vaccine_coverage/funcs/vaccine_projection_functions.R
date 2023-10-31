

make_scenario_priority <- function(dt,
                                   observed,
                                   model_inputs_path,
                                   last_reported_cumulative,
                                   one_dose_ve,
                                   hierarchy, 
                                   priority = "mixed",            # Options for priority group are "essential", "highrisk", or "mixed"
                                   empirical_lag_dt=lag_dt,                 # Data table of empirical lags, Days from first vaccine to second dose
                                   lag_days = 28,                 # Days from first vaccine to second dose when lag_dt is NA
                                   loss_followup = 0.1,           # Proportion of people who get one dose but not second
                                   essential_name = "essential",  # This can be other names if essential workers are health care, etc.
                                   scale_up_duration = 60,        # How long does the scale up period take from first dose level to maximum?
                                   max_doses_per_day = 1500000,   # What is the maximum number of doses per day?
                                   distribute_type = "none",      # Should "residual" doses not accounted for be distributed?
                                   partial_1_pct = 0.6,           # Proportion of people protected after 1 dose
                                   lag_days_partial = 7,          # Days from first dose to partial_1_pct
                                   start_level = 1,               # Proportion of the doses available on day 1 that can be delivered
                                   shift_date = T,                # Mainly for testing, should available dose date be shifted?
                                   shift_level = T,               # Mainly for testing, should delivery capacity be shifted?
                                   cascade_spline = T,             # Scale up based on cascade spline?
                                   max_pct_threshold = 0.99 
){
  
  ## Moved from launch script
  dt[secured_daily_doses < 0, secured_daily_doses := 0]
  dt[secured_effective_daily_doses < 0, secured_effective_daily_doses := 0]
  dt[, projected_vaccine_doses_available := secured_daily_doses]
  dt[, projected_effective_doses_available := secured_effective_daily_doses]
  dt[, secured_daily_doses := ifelse(is.na(daily_reported_vaccinations), secured_daily_doses,
                                     ifelse(secured_daily_doses < daily_first_vaccinated, daily_first_vaccinated, secured_daily_doses))]
  
  # Define essential workers
  if(essential_name == "essential"){
    dt[, essential_workers := under65_population * essential]
  } else if(essential_name == "healthcare"){
    dt[, essential_workers := count_gbd_health_workers]
  } else if(essential_name == "jobtype"){
    dt[, essential_workers := under65_population * essential_jobtype]
  } else {
    message("essential_name must be {essential/healthcare/jobtype}!")
    stop()
  }
  
  # Redefine loss to follow up (people who don't get 1 dose still get partial protection)
  loss_followup <- loss_followup * partial_1_pct
  
  dt[, ratio_elderly_essential := over65_population / essential_workers]
  # I need this as proportion, so 
  dt[, ratio_elderly_essential := ratio_elderly_essential / (ratio_elderly_essential + 1)]
  dt[, non_essential := under65_population - essential_workers]
  
  ## Account for reported % vaccinated
  #dt$any_vaccinated_pct <- dt$people_vaccinated / dt$adult_population
  #dt[, any_vaccinated_pct := max(any_vaccinated_pct, na.rm = T), by = "location_id"]
  
  dt <- do.call(
    rbind,
    lapply(split(dt, by='location_id'), function(x) {
      
      tryCatch( { 
        
        x$any_vaccinated_pct <- max(x$people_vaccinated / x$adult_population, na.rm=T)
        return(x)
        
      }, warning=function(w){
        
        cat("Warning :", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(w), "\n")
        x$any_vaccinated_pct <- 0
        return(x)
        
      }, error=function(e){
        
        cat("Error :", unique(x$location_id), ":", unique(x$location_name), "\n")
        
      })
    })
  )
  
  
  
  dt[, would_be_vaccinated := ifelse(is.na(any_vaccinated_pct), smooth_combined_yes,
                                     ifelse(any_vaccinated_pct > smooth_combined_yes, any_vaccinated_pct, smooth_combined_yes))]
  
  tmp_locs <- unique(dt[dt$would_be_vaccinated > max_pct_threshold, location_name])
  message(glue('The following {length(tmp_locs)} locations have willing population > adult population. These will be capped at {max_pct_threshold} of the adult population:'))
  message(paste(tmp_locs, collapse=' | '))

  par(mfrow=c(2,1))
  tmp_max <- max(dt$would_be_vaccinated, na.rm=T)
  hist(dt$would_be_vaccinated, xlim=c(0,tmp_max))
  abline(v=max_pct_threshold, lty=2, col='red')
  
  dt$would_be_vaccinated[dt$would_be_vaccinated > max_pct_threshold] <- max_pct_threshold # Cap willing population at max pct threshold
  
  hist(dt$would_be_vaccinated, col='lightblue', xlim=c(0,tmp_max), main=paste('Capped at', max_pct_threshold))
  abline(v=max_pct_threshold, lty=2, col='red')
  par(mfrow=c(1,1))
  
  # Used to be := smooth_combined_yes
  dt[, accept_o60 := would_be_vaccinated]
  dt[, accept_all := would_be_vaccinated]
  
  dt[, count_accept_o65 := over65_population * accept_o60]
  dt[, count_accept_all := (under65_population - essential_workers) * accept_all]
  dt[, count_accept_essential := essential_workers * accept_all]
  
  # How should residual doses (unclaimed in purchasing agreements) be distributed?
  if(distribute_type == "all") {
    dt[, vaccine_doses_available := secured_daily_doses + global_daily_residual_location]
    dt[, effective_doses_available := secured_effective_daily_doses + global_effective_daily_residual_location]
  } else if(distribute_type == "purchased") {
    dt[, vaccine_doses_available := secured_daily_doses + residual_daily_doses]
    dt[, effective_doses_available := secured_effective_daily_doses + effective_daily_residual]
  } else if(distribute_type == "none") {
    dt[, vaccine_doses_available := secured_daily_doses]
    dt[, effective_doses_available := secured_effective_daily_doses]
  } else {
    message("The distribute_type must be 'all', 'purchased', or 'none'!")
    stop()
  }
  
  # Find "efficacy"
  dt[, daily_effectiveness_ratio := projected_effective_doses_available / projected_vaccine_doses_available]
  dt[!is.na(az_doses), daily_effectiveness_ratio_hr := 0.91] # Efficacy for Pfizer, shouldn't be hardcoded...
  dt[is.na(az_doses), daily_effectiveness_ratio_hr := daily_effectiveness_ratio]
  
  # Fill missing values
  dt[, daily_effectiveness_ratio := nafill(daily_effectiveness_ratio, "nocb"), by = "location_id"]
  
  # Calculate cumulative 
  dt[, cumulative_doses_available := cumsum(vaccine_doses_available), by = "location_id"]
  dt[is.na(effective_doses_available), effective_doses_available := 0]
  dt[, cumulative_effective_doses_available := cumsum(effective_doses_available), by = "location_id"]
  
  # an argument for making scenarios
  delivery_capacity <- delivery_doses_per_day(us_doses_per_day = max_doses_per_day, # Argument from make_scenario_priority func
                                              model_inputs_path = model_inputs_path, 
                                              hierarchy = hierarchy)
  dt <- merge(dt, delivery_capacity[,c("location_id","delivery_rate","delivery_count")], by = "location_id")
  
  # Create non-linear increase to maximum delivery capacity
  dt[, date := as.Date(date)]
  
  #dt[, first_dose_date := as.Date(min(date[vaccine_doses_available > 0])), by = "location_id"]
  dt <- do.call(
    rbind,
    lapply(split(dt, by='location_id'), function(x) {
      
      tryCatch( { 
        
        x$first_dose_date <- as.Date(min(x$date[x$vaccine_doses_available > 0], na.rm=TRUE))
        sel <- !is.na(x$reported_vaccinations)
        return(x)
        
      }, warning=function(w){
        
        cat("Warning :", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(w), "\n")
        
        # If location breaks get first dose date from region
        tmp <- hierarchy[location_id == .get_column_val(x$location_id), region_id]
        neighbors <- hierarchy[region_id == tmp, location_id]
        x_region <- dt[location_id %in% neighbors,]
        x$first_dose_date <- as.Date(min(x_region$date[x_region$vaccine_doses_available > 0], na.rm=TRUE))
        return(x)
        
      }, error=function(e){
        
        cat("Error :", unique(x$location_id), ":", unique(x$location_name), "\n")
        
      })
      
      
      
    })
  )
  
  
  
  # Change first dose date to reported, where available.
  if(shift_date == T){
    dt[, first_report_date := as.character(min(date[!is.na(reported_vaccinations)])), by = "location_id"]
    dt[, first_dose_date := fifelse(is.na(first_report_date), as.Date(first_dose_date), as.Date(first_report_date))]
    tmp <- dt$first_report_date
    dt[, c("first_report_date") := NULL]
  }
  
  #dt[, first_dose_avb := vaccine_doses_available[date == first_dose_date], by = "location_id"]
  dt <- do.call(
    rbind,
    lapply(split(dt, by='location_id'), function(x) {
      
      tryCatch( { 
        
        sel <- which(x$date == as.Date(unique(x$first_dose_date)))
        x$first_dose_avb <- x$vaccine_doses_available[sel]
        return(x)
        
      }, warning=function(w){
        
        cat("Warning :", unique(x$location_id), ":", unique(x$location_name), ":", conditionMessage(w), "\n")
        x$first_dose_avb <- 0
        return(x)
        
      }, error=function(e){
        
        cat("Error :", unique(x$location_id), ":", unique(x$location_name), "\n")
        x$first_dose_avb <- 0
        return(x)
        
      })
    })
  )
  
  dt[first_dose_avb == 0, first_dose_avb := 1]
  
  # How many of the doses on day 1 can actually be delivered?
  dt$first_dose_avb <- dt$first_dose_avb * start_level
  
  # Set the number of doses that can be delivered on the first date among locations that start in 2021
  # as the median of doses / population among locations that start in 2021
  median2021 <- quantile(dt[first_dose_date < "2021-01-01"]$first_dose_avb / dt[first_dose_date < "2021-01-01"]$adult_population, 0.25)
  
  dt[, first_dose_avb := ifelse(first_dose_date < "2021-01-01", first_dose_avb, median2021 * adult_population)]
  
  # Adjust for v. fast scale up
  q32021 <- quantile(dt[first_dose_date < "2021-01-01"]$first_dose_avb / dt[first_dose_date < "2021-01-01"]$adult_population, 0.75)
  dt[location_id %in% c(85, 156, 140), first_dose_avb := q32021 * adult_population]
  
  dt[, k := log(delivery_count / first_dose_avb) / scale_up_duration]
  #unique(dt$first_dose_avb) * exp(unique(dt$k) * 60)
  dt[, growth_period := ifelse(date >= first_dose_date & date < first_dose_date + scale_up_duration, 1, 0)]
  dt[, growth_period := ifelse(date >= first_dose_date & date < first_dose_date + scale_up_duration, cumsum(growth_period), 0)]
  
  dt[, growth_period := ifelse(date >= first_dose_date, as.numeric(date - first_dose_date), 0)]
  dt[, growth_period := ifelse(growth_period > scale_up_duration, scale_up_duration, growth_period)]
  
  dt[, scale_up_delivery := first_dose_avb * exp(k * growth_period)]
  dt[, scale_up_delivery := ifelse(date < first_dose_date, 0, scale_up_delivery)]
  dt[, scale_up_delivery_unadj := scale_up_delivery]
  dt[, cumulative_delivery_capacity := cumsum(scale_up_delivery), by = "location_id"]
  dt[, cumulative_delivery_capacity_unadj := cumulative_delivery_capacity]
  
  ## Attempted new approach to match reported data exactly ##  
  # Should delivery capacity be shifted to match reported?
  if(shift_level == T){
    dt <- merge(dt, last_reported_cumulative[,c("location_id", "max_daily_reported","max_date")],
                by = "location_id", all.x = T)
    
    locs_no_reported <- unique(dt[is.na(reported_vaccinations), location_id])
    locs_with_reported <- unique(dt[!is.na(reported_vaccinations), location_id])
    
    dt[is.na(max_daily_reported), max_daily_reported := 0]
    dt[, closest_val := cumulative_delivery_capacity[date == max_date], by = "location_id"]
    dt[, scalar_level := ifelse(!is.na(closest_val), reported_vaccinations / closest_val, 1)]
    dt[, scalar_level := scalar_level[date == max_date], by = "location_id"]
    dt[, tmp_max_delivery := delivery_count * scalar_level]
    dt[, difference := delivery_count - tmp_max_delivery]
    
    dt[, delivery_count := ifelse(delivery_count > max_daily_reported, delivery_count, max_daily_reported)]
    dt[location_id %in% locs_with_reported, cumulative_delivery_capacity := cumulative_delivery_capacity * scalar_level]
    dt[, scale_up_delivery := cumulative_delivery_capacity - shift(cumulative_delivery_capacity, fill = 0), by = "location_id"]
    dt[, scale_up_delivery := ifelse(scale_up_delivery > delivery_count, delivery_count, scale_up_delivery)] 
    
    dt[, difference := abs(scale_up_delivery_unadj - tmp_max_delivery)]
    dt[date > first_dose_date, nearest_date := date[which.min(difference)], by = "location_id"]
    dt[, shift_days := as.numeric((first_dose_date + scale_up_duration) - nearest_date)]
    dt[is.na(shift_days), shift_days := 0]
    
    for(loc_id in locs_with_reported){
      shift_val <- max(dt[location_id == loc_id, shift_days])
      dt[location_id == loc_id, shifted_scale_up_delivery_unadj := shift(scale_up_delivery_unadj, shift_val), by = "location_id"]
    }
    dt[date > first_dose_date + scale_up_duration & location_id %in% locs_with_reported, scale_up_delivery :=
         ifelse(scale_up_delivery > shifted_scale_up_delivery_unadj, scale_up_delivery, shifted_scale_up_delivery_unadj)]
    dt[!is.na(reported_vaccinations), scale_up_delivery := (daily_reported_vaccinations - daily_fully_vaccinated)]
    dt[!is.na(daily_people_vaccinated), scale_up_delivery := daily_people_vaccinated]
    
    dt[location_id %in% locs_with_reported, cumulative_delivery_capacity := cumsum(scale_up_delivery), by = "location_id"]
  }
  
  # Goal is to allow "warehoused" doses to be used after date available
  dt[, supply_and_delivery_doses_og := ifelse(scale_up_delivery > vaccine_doses_available, vaccine_doses_available, scale_up_delivery)]
  dt[, cumulative_supply_and_delivery := ifelse(cumulative_delivery_capacity > cumulative_doses_available, 
                                                cumulative_doses_available, cumulative_delivery_capacity)]
  
  dt[, supply_and_delivery_doses := cumulative_supply_and_delivery - shift(cumulative_supply_and_delivery), by = "location_id"]
  
  ## Merge with Spline Cascade Projections #
  if(cascade_spline == TRUE){
    
    # spline_cascade[, spline_pct_daily := spline_coverage - shift(spline_coverage), by = "location_id"]
    ## Update, spline cascade model for delivery
    
    
    # Historical spline model that we have been using for months
    spline_cascade <- fread(file.path(DATA_ROOTS$VACCINE_OUTPUT_ROOT, "static-data", "scale_up_spline_cascade_all_probably.csv"))
    
    # Spline fit model using current data
    #spline_cascade <- fread(file.path(.output_path, "scale_up_spline_cascade_all_probably.csv"))
    
    spline_cascade[, end_scale_up := max(running_days), by = "location_id"]
    spline_cascade[, spline_coverage := ifelse(spline_coverage >= 0.9999998, 1, spline_coverage)]
    dt[, running_days := ifelse(date >= first_dose_date, as.numeric(date - first_dose_date), NA)]
    dt[, implied_coverage := people_vaccinated / (smooth_combined_yes * adult_population)]
    dt[, max_implied_coverage := max(implied_coverage, na.rm = T), by = "location_id"]
    
    dt <- merge(dt, spline_cascade[,c("location_id","running_days","spline_coverage","end_scale_up")],
                by = c("location_id","running_days"), all.x = T)
    
    dt[, spline_shift_date := min(date[!is.na(spline_coverage) & spline_coverage > max_implied_coverage]), by = "location_id"]
    dt[, spline_shift_days := as.numeric(max_date - spline_shift_date)]
    dt[, end_scale_up := unique(end_scale_up[!is.na(end_scale_up)]), by = "location_id"]
    
    ###########
    ###########
    # Don't think this is correct
    dt[running_days >= end_scale_up, spline_coverage := 1] 
    dt[, spline_pct_daily := spline_coverage - shift(spline_coverage), by = "location_id"]
    
    dt[!is.na(spline_pct_daily) & is.na(reported_vaccinations), 
       scale_up_delivery := smooth_combined_yes * adult_population * spline_pct_daily] # Demand gets defined here when spline fit is used? Shouldnt this be (1 - spline_pct_daily)??
    dt[scale_up_delivery < 0, scale_up_delivery := 0]
    ###########
    ###########
    
  }
  
  # If scale_up_delivery is too high then tons os supply gets delivered...
  dt[, supply_and_delivery_doses := ifelse(supply_and_delivery_doses > scale_up_delivery, scale_up_delivery,
                                           supply_and_delivery_doses)]
  
  # I think this needs to be set to "first dose"
  dt[!is.na(daily_first_vaccinated), supply_and_delivery_doses := daily_first_vaccinated]
  
  dt[is.na(supply_and_delivery_doses), supply_and_delivery_doses := 0]
  
  # Find ratio total / effective
  dt[, effective_supply_and_delivery_doses := supply_and_delivery_doses * daily_effectiveness_ratio]
  
  # Find lagged variant efficacy
  dt[, variant_ratio_lag_partial := shift(weighted_variant_efficacy_location / weighted_efficacy_location, n = lag_days_partial, type = "lag"), by="location_id"]
  
  # Do in a loop for unique lag values by location_id
  for(loc_id in unique(dt$location_id)){
    shift_val <- unique(dt[location_id == loc_id, observed_lag])
    dt[location_id == loc_id, variant_ratio_lag_full := shift(weighted_variant_efficacy_location / weighted_efficacy_location, 
                                                              n = shift_val, type = "lag"), by = "location_id"]
  }
  
  if (priority == "highrisk") {
    group1 <- "elderly"
    group1_ratio <- 1
    group2_ratio <- 0
    group1_pop <- dt$count_accept_o65
    group2_pop <- dt$count_accept_essential
  } else if (priority == "essential") {
    group1 <- "essential"
    group1_ratio <- 1
    group2_ratio <- 0
    group1_pop <- dt$count_accept_essential
    group2_pop <- dt$count_accept_o65
  } else {
    # (priority == "mixed")
    group1 <- "elderly"
    group1_ratio <- dt$ratio_elderly_essential
    group2_ratio <- 1 - dt$ratio_elderly_essential
    group1_pop <- dt$count_accept_o65
    group2_pop <- dt$count_accept_essential
  }
  
  dt[is.na(pct_2_dose), pct_2_dose := 1]
  
  for(loc_id in unique(dt$location_id)){
    shift_val <- unique(dt[location_id == loc_id, observed_lag])
    dt[location_id == loc_id, reserve_second_dose := shift(supply_and_delivery_doses * (1 - loss_followup) * pct_2_dose * 0.8, 
                                                           n = shift_val, fill = 0, type = "lag"), by = "location_id"]
  }
  
  dt[, available_first_dose := supply_and_delivery_doses - reserve_second_dose]
  
  ##--------------------------------------------------------------------------------------------    
  ## Calculate number of people fully vaccinated, protected & non-infectious, non-infectious only
  dt[, group1_vaccinated := supply_and_delivery_doses * group1_ratio] 

  dt[, group1_effective := effective_supply_and_delivery_doses * pct_2_dose * group1_ratio]
  # Once the maximum number of group1 who would be vaccinated is reached, stop assigning vaccine here
  dt[, cumulative_group1_vaccinated := cumsum(group1_vaccinated), by="location_id"]
  
  # # Test new approach
  dt[, cumulative_group1_vaccinated := ifelse(cumulative_group1_vaccinated >= group1_pop, group1_pop, cumulative_group1_vaccinated)]
  dt[, group1_vaccinated := cumulative_group1_vaccinated - shift(cumulative_group1_vaccinated), by = "location_id"]
  dt[, group1_effective := group1_vaccinated * pct_2_dose * daily_effectiveness_ratio_hr]
  
  # vaccine_available is "effective" doses, so to get vaccinated but unprotected... 
  dt[, group1_unprotected := group1_vaccinated - group1_effective]
  
  # Some people protected partially after 1 dose
  dt[, group1_effective_1d := group1_effective * partial_1_pct]
  dt[, group1_effective_1d := shift(group1_effective_1d, n = lag_days_partial, type = "lag"), by="location_id"]
  dt[, group1_effective_1d := ifelse(is.na(group1_effective_1d), 0, group1_effective_1d)]
  
  # Others will have loss to followup
  dt[, group1_effective := group1_effective * (1 - loss_followup) * (1- partial_1_pct)] 
  # Shift the number effectively vaccinated, unprotected by unique value by location
  for(loc_id in unique(dt$location_id)){
    shift_val <- unique(dt[location_id == loc_id, observed_lag])
    dt[location_id == loc_id, group1_effective := shift(group1_effective, n = shift_val), by = "location_id"]
    dt[location_id == loc_id, group1_unprotected := shift(group1_unprotected, n = shift_val), by = "location_id"]
  }
  dt[, group1_effective := ifelse(is.na(group1_effective), 0, group1_effective)]
  dt[, group1_unprotected := ifelse(is.na(group1_unprotected), 0, group1_unprotected)]
  
  # Add the single dose
  
  dt[, group1_single_dose := group1_vaccinated * (1 - pct_2_dose) * one_dose_ve] # I think this is the VE for Johnson and Johnson?
  dt[, group1_effective := group1_single_dose + group1_effective + group1_effective_1d]
  dt[, group1_unprotected := group1_vaccinated * (1 - pct_2_dose) * (1 - one_dose_ve) + group1_unprotected]
  
  ##--------------------------------------------------------------------------------------------  
  # When the maximum number of group1 have been vaccinated, assign all doses to group 2
  dt[, group2_vaccinated := supply_and_delivery_doses *
       ifelse(cumulative_group1_vaccinated < group1_pop, group2_ratio, 1)]
  dt[, group2_effective := effective_supply_and_delivery_doses * pct_2_dose * 
       ifelse(cumulative_group1_vaccinated < group1_pop, group2_ratio, 1)]
  # Once the maximum number of group2 workers who would be vaccinated is reached, stop assigning vaccine here
  dt[, cumulative_group2_vaccinated := cumsum(group2_vaccinated), by="location_id"]
  
  # # Test new approach
  dt[, cumulative_group2_vaccinated := ifelse(cumulative_group2_vaccinated >= group2_pop, group2_pop, cumulative_group2_vaccinated)]
  dt[, group2_vaccinated := cumulative_group2_vaccinated - shift(cumulative_group2_vaccinated), by = "location_id"]
  dt[, group2_effective := group2_vaccinated * pct_2_dose * daily_effectiveness_ratio]
  
  # vaccine_available is "effective" doses, so to get vaccinated but unprotected... 
  dt[, group2_unprotected := group2_vaccinated - group2_effective]
  
  # Some people protected partially after 1 dose
  dt[, group2_effective_1d := group2_effective * partial_1_pct]
  dt[, group2_effective_1d := shift(group2_effective_1d, n = lag_days_partial, type = "lag"), by="location_id"]
  dt[, group2_effective_1d := ifelse(is.na(group2_effective_1d), 0, group2_effective_1d)]
  
  # Others will have loss to followup
  dt[, group2_effective := group2_effective * (1 - loss_followup) * (1- partial_1_pct)] 
  # Shift the number effectively vaccinated, unprotected by unique lag by location
  for(loc_id in unique(dt$location_id)){
    shift_val <- unique(dt[location_id == loc_id, observed_lag])
    dt[location_id == loc_id, group2_effective := shift(group2_effective, n = shift_val), by = "location_id"]
    dt[location_id == loc_id, group2_unprotected := shift(group2_unprotected, n = shift_val), by = "location_id"]
  }
  dt[, group2_effective := ifelse(is.na(group2_effective), 0, group2_effective)]
  dt[, group2_unprotected := ifelse(is.na(group2_unprotected), 0, group2_unprotected)]
  
  # Add the single dose
  dt[, group2_single_dose := group2_vaccinated * (1 - pct_2_dose) * one_dose_ve]
  dt[, group2_effective := group2_single_dose + group2_effective + group2_effective_1d]
  dt[, group2_unprotected := group2_vaccinated * (1 - pct_2_dose) * (1 - one_dose_ve) + group2_unprotected]
  
  ##--------------------------------------------------------------------------------------------  
  # When the maximum number of group2 workers have been vaccinated, assign all doses to remaining adults
  # adults  
  dt[, adults_vaccinated := supply_and_delivery_doses *
       ifelse(cumulative_group2_vaccinated < group2_pop, 0, 1)]
  dt[, adults_effective := effective_supply_and_delivery_doses * pct_2_dose * 
       ifelse(cumulative_group2_vaccinated < group2_pop, 0, 1)]
  # Once the maximum number of essential workers who would be vaccinated is reached, stop assigning vaccine here
  dt[, cumulative_adults_vaccinated := cumsum(adults_vaccinated), by="location_id"]
  
  # # Test new approach
  dt[, cumulative_adults_vaccinated := ifelse(cumulative_adults_vaccinated >= count_accept_all, count_accept_all, cumulative_adults_vaccinated)]
  dt[, adults_vaccinated := cumulative_adults_vaccinated - shift(cumulative_adults_vaccinated), by = "location_id"]
  dt[, adults_effective := adults_vaccinated * pct_2_dose * daily_effectiveness_ratio]
  
  # vaccine_available is "effective" doses, so to get vaccinated but unprotected... 
  dt[, adults_unprotected := adults_vaccinated - adults_effective]
  
  # Some people protected partially after 1 dose
  dt[, adults_effective_1d := adults_effective * partial_1_pct]
  dt[, adults_effective_1d := shift(adults_effective_1d, n = lag_days_partial, type = "lag"), by="location_id"]
  dt[, adults_effective_1d := ifelse(is.na(adults_effective_1d), 0, adults_effective_1d)]
  
  # Others will have loss to followup
  dt[, adults_effective := adults_effective * (1 - loss_followup) * (1- partial_1_pct)] 
  # Shift the number effectively vaccinated, unprotected by unique value by location
  for(loc_id in unique(dt$location_id)){
    shift_val <- unique(dt[location_id == loc_id, observed_lag])
    dt[location_id == loc_id, adults_effective := shift(adults_effective, n = shift_val), by = "location_id"]
    dt[location_id == loc_id, adults_unprotected := shift(adults_unprotected, n = shift_val), by = "location_id"]
  }
  dt[, adults_effective := ifelse(is.na(adults_effective), 0, adults_effective)]
  dt[, adults_unprotected := ifelse(is.na(adults_unprotected), 0, adults_unprotected)]
  
  # Add the single dose
  dt[, adults_single_dose := adults_vaccinated * (1 - pct_2_dose) * one_dose_ve]
  dt[, adults_effective := adults_single_dose + adults_effective + adults_effective_1d]
  dt[, adults_unprotected := adults_vaccinated * (1 - pct_2_dose) * (1 - one_dose_ve) + adults_unprotected]
  
  dt[is.na(group1_vaccinated), group1_vaccinated := 0]
  dt[is.na(group2_vaccinated), group2_vaccinated := 0]
  dt[is.na(adults_vaccinated), adults_vaccinated := 0]
  
  dt[, cumulative_group1_vaccinated := cumsum(group1_vaccinated), by="location_id"]
  dt[, cumulative_group2_vaccinated := cumsum(group2_vaccinated), by="location_id"]
  dt[, cumulative_adults_vaccinated := cumsum(adults_vaccinated), by="location_id"]
  dt[, cumulative_all_vaccinated := cumulative_group1_vaccinated + cumulative_group2_vaccinated + cumulative_adults_vaccinated]
  
  if(priority == "essential"){
    names(dt) <- gsub("group1", "essential", names(dt))
    names(dt) <- gsub("group2", "elderly", names(dt))
  } else {
    names(dt) <- gsub("group1", "elderly", names(dt))
    names(dt) <- gsub("group2", "essential", names(dt))
  }
  
  ## Don't lag?
  dt[, variant_ratio_lag_partial := weighted_variant_efficacy_location / weighted_efficacy_location]
  dt[, variant_ratio_lag_full := weighted_variant_efficacy_location / weighted_efficacy_location]
  
  dt[, c("elderly_effective_variant","essential_effective_variant","adults_effective_variant") :=
       lapply(.SD, function(x) x * variant_ratio_lag_partial * (1-pct_2_dose) + x * variant_ratio_lag_full * pct_2_dose),
     .SDcols = c("elderly_effective","essential_effective","adults_effective")]
  
  dt[, elderly_effective_wildtype := elderly_effective - elderly_effective_variant]
  dt[, essential_effective_wildtype := essential_effective - essential_effective_variant]
  dt[, adults_effective_wildtype := adults_effective - adults_effective_variant]
  
  # Protected severe disease
  dt[, c("elderly_effective_protected_variant","essential_effective_protected_variant","adults_effective_protected_variant") :=
       lapply(.SD, function(x) x * (1 - weighted_protected_location)),
     .SDcols = c("elderly_effective_variant","essential_effective_variant","adults_effective_variant")]
  
  dt[, c("elderly_effective_protected_wildtype","essential_effective_protected_wildtype","adults_effective_protected_wildtype") :=
       lapply(.SD, function(x) x * (1 - weighted_protected_location)),
     .SDcols = c("elderly_effective_wildtype","essential_effective_wildtype","adults_effective_wildtype")]
  
  # Make mutually exclusive
  dt[, elderly_effective_wildtype := elderly_effective_wildtype - elderly_effective_protected_wildtype]
  dt[, essential_effective_wildtype := essential_effective_wildtype - essential_effective_protected_wildtype]
  dt[, adults_effective_wildtype := adults_effective_wildtype - adults_effective_protected_wildtype]
  
  dt[, elderly_effective_variant := elderly_effective_variant - elderly_effective_protected_variant]
  dt[, essential_effective_variant := essential_effective_variant - essential_effective_protected_variant]
  dt[, adults_effective_variant := adults_effective_variant - adults_effective_protected_variant]

  #--------------------------------------------------------------------------------------------------------------------------------
  ## Aggregate results ##
  # Preparing columns to aggregate, and an NA interpolation function to ease aggregation from child leaf nodes up to parent country nodes
  
  agg_cols <- c("cumulative_elderly_vaccinated","cumulative_essential_vaccinated","cumulative_adults_vaccinated",
                "cumulative_all_vaccinated","scale_up_delivery", "supply_and_delivery_doses",
                "count_accept_o65","count_accept_essential","count_accept_all",
                "elderly_vaccinated","essential_vaccinated","adults_vaccinated",
                "elderly_effective","essential_effective","adults_effective",
                "over65_population","under65_population","adult_population",
                "reported_vaccinations", "people_vaccinated",  "fully_vaccinated", 
                paste0(c("elderly_","essential_","adults_"), "effective_wildtype"),
                paste0(c("elderly_","essential_","adults_"), "effective_protected_wildtype"),
                paste0(c("elderly_","essential_","adults_"), "effective_variant"),
                paste0(c("elderly_","essential_","adults_"), "effective_protected_variant"),
                "essential_workers","delivery_count")
  

  
  
  
  #--------------------------------------------------------------------------------------------------------------------------------
  ## Aggregate results ##
  
  dt[is.na(pct_2_dose) | is.nan(pct_2_dose), pct_2_dose := mean(dt$pct_2_dose, na.rm=T)]
  
  ## Number of people "fully vaccinated" (received full course of vaccination)
  dt[, daily_all_vaccinated := elderly_vaccinated + essential_vaccinated + adults_vaccinated]
  dt[, daily_all_full_one_dose := daily_all_vaccinated * (1-pct_2_dose)]
  dt[, daily_all_full_two_dose := daily_all_vaccinated * pct_2_dose * (1-loss_followup)]
  
  # Lag fully vaccinated by location-specific empirical lag
  
  dt[is.na(daily_all_full_two_dose), daily_all_full_two_dose := 0]
  dt[, daily_all_fully_vaccinated := daily_all_full_one_dose + daily_all_full_two_dose]
  dt[is.na(daily_all_fully_vaccinated), daily_all_fully_vaccinated := 0]
  dt[, cumulative_all_fully_vaccinated := cumsum(daily_all_fully_vaccinated), by = "location_id"]

  message('Estimating x-axis shift to fit fully vaccinated')
  dt <- do.call(
    rbind,
    pbapply::pblapply(split(dt, by='location_id'), function(x) {
      
      tryCatch( { 
        
        t_current <- max(x$date[!is.na(x$fully_vaccinated)])
        sel_data <- which(x$date == t_current)
        sel_model <- which.min(abs(x$cumulative_all_fully_vaccinated - x$fully_vaccinated[sel_data]))
        
        # Shift model to match data at t_current
        x[, cumulative_all_fully_vaccinated := shift(cumulative_all_fully_vaccinated, n = as.integer(x$date[sel_data] - x$date[sel_model]))]
        
        # Set model to observed data prior to t_current
        x[date <= t_current, cumulative_all_vaccinated := people_vaccinated]
        x[date <= t_current, cumulative_all_fully_vaccinated := fully_vaccinated]
        
        x$cumulative_all_vaccinated <- .make_cumulative(x$cumulative_all_vaccinated)
        x$cumulative_all_fully_vaccinated  <- .make_cumulative(x$cumulative_all_fully_vaccinated)
        
        
      }, error=function(e){
        
        cat("Warning :", unique(x$location_id), ":", unique(x$location_name), "\n")
        
      })
      
      return(x)
      
    })
  )
  
  
  
  ## Find the proportion effectively vaccinated, protected against variants, wildtype.
  # INDIVIDUAL_NAME uses daily
  # Number vaccinated
  # Number immunized to wild-type
  # Number immunized to wild-type + variant
  # Number protected from severe disease due to wild type
  # Number protected from severe disease due to wild type + variant
  
  # Cumulative totals for all
  dt[, c("elderly_effective", "essential_effective","adults_effective",
         paste0(c("elderly","essential","adults"),"_effective_variant"),
         paste0(c("elderly","essential","adults"),"_effective_protected_variant"),
         paste0(c("elderly","essential","adults"),"_effective_wildtype"),
         paste0(c("elderly","essential","adults"),"_effective_protected_wildtype"),
         "adults_unprotected") :=
       lapply(.SD, function(x) ifelse(is.na(x), 0, x)),
     .SDcols = c("elderly_effective", "essential_effective","adults_effective", 
                 paste0(c("elderly","essential","adults"),"_effective_variant"),
                 paste0(c("elderly","essential","adults"),"_effective_protected_variant"),
                 paste0(c("elderly","essential","adults"),"_effective_wildtype"),
                 paste0(c("elderly","essential","adults"),"_effective_protected_wildtype"),
                 "adults_unprotected")]
  
  dt[, c("cumulative_elderly_effective", "cumulative_essential_effective","cumulative_adults_effective",
         paste0("cumulative_",c("elderly","essential","adults"),"_effective_variant"),
         paste0("cumulative_",c("elderly","essential","adults"),"_effective_wildtype"),
         paste0("cumulative_",c("elderly","essential","adults"),"_effective_protected_variant"),
         paste0("cumulative_",c("elderly","essential","adults"),"_effective_protected_wildtype"),
         "cumulative_adults_unprotected") := 
       lapply(.SD, function(x) cumsum(x)),
     .SDcols = c("elderly_effective", "essential_effective","adults_effective", 
                 paste0(c("elderly","essential","adults"),"_effective_variant"),
                 paste0(c("elderly","essential","adults"),"_effective_wildtype"),
                 paste0(c("elderly","essential","adults"),"_effective_protected_variant"),
                 paste0(c("elderly","essential","adults"),"_effective_protected_wildtype"),
                 "adults_unprotected"),
     by = "location_id"]
  
  ## Make observed column (already there with observed dataframe)
  dt[, observed := ifelse(!is.na(daily_reported_vaccinations), 1, 0)]

  ###
  # This used to be the 'clean_up' function below but having this outside this function
  # was making hot fixes more difficult
  
  dt <- dt[, c("location_id","location_name","date","observed",
               "reported_vaccinations","fully_vaccinated","people_vaccinated",
               paste0("elderly_", c("vaccinated","effective","unprotected")),
               paste0("essential_", c("vaccinated","effective","unprotected")),
               paste0("adults_", c("vaccinated","effective","unprotected")),
               paste0(c("elderly_","essential_","adults_"), "effective_wildtype"),
               paste0(c("elderly_","essential_","adults_"), "effective_variant"),
               paste0(c("elderly_","essential_","adults_"), "effective_protected_wildtype"),
               paste0(c("elderly_","essential_","adults_"), "effective_protected_variant"),
               paste0("cumulative_",c("elderly_","essential_","adults_"), "effective_wildtype"),
               paste0("cumulative_",c("elderly_","essential_","adults_"), "effective_variant"),
               paste0("cumulative_",c("elderly_","essential_","adults_"), "effective_protected_wildtype"),
               paste0("cumulative_",c("elderly_","essential_","adults_"), "effective_protected_variant"),
               "cumulative_elderly_vaccinated","cumulative_essential_vaccinated","cumulative_adults_vaccinated",
               "cumulative_all_fully_vaccinated", #"cumulative_fully_vaccinated",
               "vaccine_doses_available","effective_doses_available","scale_up_delivery",
               "essential", "smooth_combined_yes", "any_vaccinated_pct", #"predicted_reject_all","predicted_reject_o60",
               "adult_population","over65_population","under65_population","essential_workers","non_essential",
               "count_accept_o65", "count_accept_essential", "count_accept_all","delivery_rate","delivery_count",
               "supply_and_delivery_doses")]
  
  dt[is.na(elderly_effective), elderly_effective := 0]
  dt[is.na(essential_effective), essential_effective := 0]
  dt[is.na(adults_effective), adults_effective := 0]
  dt[, cumulative_elderly_effective := cumsum(elderly_effective), by = "location_id"]
  dt[, cumulative_essential_effective := cumsum(essential_effective), by = "location_id"]
  dt[, cumulative_adults_effective := cumsum(adults_effective), by = "location_id"]
  dt[, cumulative_all_vaccinated := cumulative_elderly_vaccinated + cumulative_essential_vaccinated + cumulative_adults_vaccinated]
  dt[, cumulative_all_effective := cumulative_elderly_effective + cumulative_essential_effective + cumulative_adults_effective]
  
  ## Make some aggregates
  dt[, cumulative_all_effective_variant := cumulative_elderly_effective_variant + cumulative_essential_effective_variant +
       cumulative_adults_effective_variant]
  dt[, cumulative_all_effective_protected_variant := cumulative_elderly_effective_protected_variant + cumulative_essential_effective_protected_variant +
       cumulative_adults_effective_protected_variant]
  dt[, cumulative_all_effective_wildtype := cumulative_elderly_effective_wildtype + cumulative_essential_effective_wildtype +
       cumulative_adults_effective_wildtype]
  dt[, cumulative_all_effective_protected_wildtype := cumulative_elderly_effective_protected_wildtype + cumulative_essential_effective_protected_wildtype +
       cumulative_adults_effective_protected_wildtype]
  
  # Prep new collapsed columns
  setnames(dt, paste0("elderly_", c("vaccinated","unprotected","effective_variant","effective_protected_variant",
                                    "effective_wildtype","effective_protected_wildtype")), 
           paste0("hr_", c("vaccinated","unprotected","effective_variant","effective_protected_variant",
                           "effective_wildtype","effective_protected_wildtype")))
  dt[, lr_vaccinated := essential_vaccinated + adults_vaccinated]
  dt[, lr_unprotected := essential_unprotected + adults_unprotected]
  dt[, lr_effective_variant := essential_effective_variant + adults_effective_variant]
  dt[, lr_effective_protected_variant := essential_effective_protected_variant + adults_effective_protected_variant]
  dt[, lr_effective_wildtype := essential_effective_wildtype + adults_effective_wildtype]
  dt[, lr_effective_protected_wildtype := essential_effective_protected_wildtype + adults_effective_protected_wildtype]
  
  dt[, c(paste0("cumulative_", paste0("lr_", c("effective_variant","effective_protected_variant",
                                               "effective_wildtype","effective_protected_wildtype"))),
         paste0("cumulative_", paste0("hr_", c("effective_variant","effective_protected_variant",
                                               "effective_wildtype","effective_protected_wildtype")))) :=
       lapply(.SD, function(x) cumsum(x)),
     by = c("location_id"),
     .SDcols = c(paste0("lr_", c("effective_variant","effective_protected_variant",
                                 "effective_wildtype","effective_protected_wildtype")),
                 paste0("hr_", c("effective_variant","effective_protected_variant",
                                 "effective_wildtype","effective_protected_wildtype")))]
  
  ###
  
  
  message('Ensuring models are cumulative...')
  dt <- do.call(
    rbind,
    pbapply::pblapply(split(dt, by='location_id'), function(x) {
      
      tryCatch( { 
        
        x$cumulative_all_vaccinated <- .make_cumulative(x$cumulative_all_vaccinated)
        x$cumulative_all_fully_vaccinated  <- .make_cumulative(x$cumulative_all_fully_vaccinated)
        x$cumulative_all_fully_vaccinated <- pmin(x$cumulative_all_fully_vaccinated, x$cumulative_all_vaccinated*(1-loss_followup))
        
      }, error=function(e){
        
        cat("Warning :", unique(x$location_id), ":", unique(x$location_name), "\n")
        
      })
      
      return(x)
      
    })
  )
  
  
  
  message('Applying hot-fixes')

  dt <- .fix_lead_trail_na(dt)
  dt <- .interp_na(dt)
  dt <- .fix_non_cumulative(dt)
  
  # Set upper threshold for prop vaccinated
  
  
  
  # x <- dt[location_id == 531] 
  # plot(x$date, x$reported_vaccinations, main=x$location_id[1])
  # lines(x$date, .make_cumulative(x$cumulative_all_vaccinated), type='l', lwd=3)
  # lines(x$date, x$cumulative_all_fully_vaccinated, col='red', lwd=3, lty=1)
  # lines(x$date, x$cumulative_all_effective, col='blue', lwd=3, lty=1)
  # 
  # abline(h=x$adult_population)
  
  #points(x$date, x$people_vaccinated)
  #points(x$date, x$fully_vaccinated)
  #points(x$date, x$reported_vaccinations)
  
  validate_projections(dt, hierarchy)

  return(dt)
}




# This function takes a vector with leading and/or trailing NAs and sets the NAs to first and last observed value respectively
.fix_lead_trail_na <- function(vaccine_model_output) {
  
  f <- function(x) {
    
    min_val <- min(x, na.rm=T)
    max_val <- max(x, na.rm=T)
    
    nas_index <- which(is.na(x))
    min_index <- min(which(x == min_val))
    max_index <- max(which(x == max_val))
    
    nas_lead <- nas_index[nas_index < min_index]
    nas_trail <- nas_index[nas_index > max_index]
    
    x[nas_lead] <- min_val
    x[nas_trail] <- max_val
    
    return(x)
  }
  
  out <- do.call(
    rbind,
    lapply(split(vaccine_model_output, by='location_id'), function(x) {
      
      tryCatch( { 
        
        # Set leading/trailing NAs to first/last observed value
        x$cumulative_all_vaccinated <- f(x$cumulative_all_vaccinated)
        x$cumulative_all_fully_vaccinated <- f(x$cumulative_all_fully_vaccinated)
        x$cumulative_all_effective <- f(x$cumulative_all_effective)
        
      }, error=function(e){})
      
      return(x)
      
    })
  )
  
  return(out)
}

#tmp <- vaccine_data$load_scenario_forecast(.output_path, scenario = 'slow')
#x <- dt[location_name == 'Malta']
#plot(x$date, x$cumulative_all_fully_vaccinated)
#x  <- x$cumulative_all_fully_vaccinated

# This function takes a vector that is supposed to be a cumulative quantity and smooths out non-cumulative elements 
.fix_non_cumulative <- function(vaccine_model_output) {
  
  f <- function(x) {
    
    #first_obs <- x[1]
    delta <- diff(x)
    
    if (any(delta < 0)) {
      for (i in which(delta < 0)) {

        out <- x
        out[i:length(out)] <- out[i]
        x <- pmax(x, out)
        
      }
    }
    
    #if (exists('out')) return(c(first_obs, out)) else return(x)
    return(x)
  }
  
  out <- do.call(
    rbind,
    lapply(split(vaccine_model_output, by='location_id'), function(x) {
      
      tryCatch( { 
        
        # Fix non-cumulative projections
        x$cumulative_all_vaccinated <- f(x$cumulative_all_vaccinated)
        x$cumulative_all_fully_vaccinated <- f(x$cumulative_all_fully_vaccinated)
        x$cumulative_all_effective <- f(x$cumulative_all_effective)
        
      }, error=function(e){})
      
      return(x)
      
    })
  )
  
  return(out)
}

# Check for missed NAs in time series and fill
.interp_na <- function(vaccine_model_output) {
  
  out <- do.call(
    rbind,
    lapply(split(vaccine_model_output, by='location_id'), function(x) {
      
      tryCatch( { 

        x$cumulative_all_vaccinated <- zoo::na.approx(x$cumulative_all_vaccinated)
        x$cumulative_all_fully_vaccinated <- zoo::na.approx(x$cumulative_all_fully_vaccinated)
        x$cumulative_all_effective <- zoo::na.approx(x$cumulative_all_effective)

      }, error=function(e){})
      
      return(x)
      
    })
  )
  
  return(out)
}


##########################################################
## Booster dose scenario for variants ##
##########################################################

# Commented out and moved to archived function folder on 2022-06-24

# booster <- function(estimates, 
#                     booster_ve = 1, 
#                     start_date = as.Date("2021-10-01"), 
#                     scale_up = 30){
#   
#   estimates <- copy(estimates)
#   estimates[, lr_increase := cumulative_adults_effective_protected_variant[date == start_date] +
#               cumulative_essential_effective_protected_variant[date == start_date], by = "location_id"]
#   estimates[, hr_increase := cumulative_elderly_effective_protected_variant[date == start_date], by = "location_id"]
#   
#   estimates[date >= start_date & date < start_date + scale_up, 
#             lr_boosted := lr_increase / scale_up]
#   estimates[date >= start_date & date < start_date + scale_up, 
#             hr_boosted := hr_increase / scale_up]
#   
#   estimates[, booster_date := start_date]
#   estimates[, booster_period := scale_up]
#   estimates <- estimates[,c("location_id","location_name","date","booster_date",
#                             "booster_period","lr_increase","hr_increase",
#                             "lr_boosted","hr_boosted")]
#   estimates <- estimates[order(location_id, date)]
#   estimates[is.na(lr_boosted), lr_boosted := 0]
#   estimates[is.na(hr_boosted), hr_boosted := 0]
#   estimates[, cumulative_lr_boosted := cumsum(lr_boosted), by = "location_id"]
#   estimates[, cumulative_hr_boosted := cumsum(hr_boosted), by = "location_id"]
#   
#   return(estimates)
#   
# }



###################################################
## Validations
###################################################

validate_projections <- function(model, hierarchy){
  
  if(nrow(model[cumulative_all_vaccinated < 0]) > 0) message("Negative cumulative vaccinated")
  
  if(nrow(model[cumulative_all_effective < 0]) > 0) message("Negative effective vaccinated")
  
  if(nrow(model[hr_vaccinated < 0]) > 0) message("Negative hr vaccinated")
  
  if(nrow(model[lr_vaccinated < 0]) > 0) message("Negative lr vaccinated")
  
  if(nrow(model[hr_effective_protected_wildtype < 0]) > 0) message("Negative hr effective protected wildtype")
  
  if(nrow(model[lr_effective_protected_variant < 0]) > 0) message("Negative lr effective protected variant")
  
  # Are there any duplicate dates?
  dedup <- unique(model[ , c("location_id","date")])
  
  if(nrow(model) != nrow(dedup)) message("There are duplicate date/location_ids somewhere in the dataset!")
  
  tmp <- setdiff(hierarchy[most_detailed == 1, location_id], unique(model$location_id))
  if(length(tmp) > 0) message("There are most detailed locations missing from projections")
  
  tmp2 <- setdiff(unique(model$location_id), hierarchy[most_detailed == 1, location_id])
  
}


###################################################
## Calculate number of people that are protected
## from disease but not immune from infection
###################################################

make_protected_not_immune <- function(model){
  
  model[, cumulative_all_effective_variant := 
          cumulative_elderly_effective_variant + cumulative_essential_effective_variant + cumulative_adults_effective_variant]
  model[, cumulative_all_effective_wildtype := 
          cumulative_elderly_effective_wildtype + cumulative_essential_effective_wildtype + cumulative_adults_effective_wildtype]
  model[, cumulative_all_effective_protected_variant := 
          cumulative_elderly_effective_protected_variant + cumulative_essential_effective_protected_variant + cumulative_adults_effective_protected_variant]
  model[, cumulative_all_effective_protected_wildtype := 
          cumulative_elderly_effective_protected_wildtype + cumulative_essential_effective_protected_wildtype + cumulative_adults_effective_protected_wildtype]
  
  model[, hr_protected_not_immune_wildtype := 
          cumulative_elderly_effective_protected_wildtype + cumulative_elderly_effective_protected_variant]
  model[, lr_protected_not_immune_wildtype := 
          cumulative_essential_effective_protected_wildtype + cumulative_essential_effective_protected_variant +
          cumulative_adults_effective_protected_wildtype + cumulative_adults_effective_protected_variant]
  
  model[, hr_protected_not_immune_variant := cumulative_elderly_effective_protected_variant]
  model[, lr_protected_not_immune_variant := cumulative_essential_effective_protected_variant + 
          cumulative_adults_effective_protected_variant]
  
  return(model)
}

#########################################################
## Apply estimates of people vaccinated to US counties
#########################################################

split_us_counties <- function(model, model_parameters){
  ## Need to grab the percent of US population by state
  
  population <- model_inputs_data$load_total_population(model_parameters$model_inputs_path)
  
  # Manually add some populations
  
  split_columns <- c("hr_vaccinated","lr_vaccinated",
                     "hr_unprotected","lr_unprotected",
                     "hr_effective_variant","lr_effective_variant",
                     "hr_effective_wildtype","lr_effective_wildtype",
                     "hr_effective_protected_variant", "lr_effective_protected_variant",
                     "hr_effective_protected_wildtype", "lr_effective_protected_wildtype",
                     "cumulative_all_vaccinated", "cumulative_all_fully_vaccinated", "cumulative_all_effective",
                     "cumulative_all_effective_variant","cumulative_all_effective_protected_variant",
                     "cumulative_all_effective_wildtype","cumulative_all_effective_protected_wildtype",
                     'cumulative_essential_vaccinated',
                     'cumulative_adults_vaccinated',
                     'cumulative_elderly_vaccinated',
                     'cumulative_essential_effective',
                     'cumulative_adults_effective',
                     'cumulative_elderly_effective',
                     'cumulative_hr_effective_wildtype',
                     'cumulative_hr_effective_protected_wildtype',
                     'cumulative_hr_effective_variant',
                     'cumulative_hr_effective_protected_variant',
                     'cumulative_lr_effective_wildtype',
                     'cumulative_lr_effective_protected_wildtype',
                     'cumulative_lr_effective_variant',
                     'cumulative_lr_effective_protected_variant')

  counties <- gbd_data$get_counties_modeling_hierarchy()
  # counties <- counties[!(location_id %in% unique(model$location_id))]
  pop <- merge(counties[most_detailed == 1, c("parent_id","location_id")], population, by = "location_id")
  pop <- merge(pop, population, by.x = "parent_id", by.y = "location_id")
  pop[, pop_frac := population.x / population.y]
  setnames(pop, "population.x", "population")
  
  county_dt <- expand.grid(location_id = counties[most_detailed == 1, location_id],
                           date = seq(min(model$date), max(model$date), by = "1 day"))
  county_dt <- data.table(county_dt)
  county_dt <- merge(county_dt, counties[,c("location_id","location_name","parent_id")], by = "location_id")
  
  ## Fix some Chilean, Colombian, Peruvian level 5
  county_dt[location_id == 54740, parent_id := 125]
  county_dt[location_id %in% c(60092, 60117), parent_id := 98]
  county_dt[location_id == 60913, parent_id := 123]
  
  out_counties <- merge(county_dt, model[,c("location_id","date","observed",
                                            ..split_columns)],
                        by.x = c("parent_id","date"), by.y = c("location_id","date"))
  out_counties <- merge(out_counties, pop[,c("location_id","pop_frac","population")], by = "location_id")
  out_counties <- out_counties[order(location_id, date)]
  out_counties[, parent_id := NULL]
  
  out_counties[, c(split_columns) := lapply(.SD, function(x) x * pop_frac),
               .SDcols = c(split_columns)]
  
  return(out_counties)
  # hr_vaccinated: daily high-risk (65+) people vaccinated
  # hr_unproteced: daily high-risk (65+) people who receive no benefit from vaccine
  # hr_effective_variant: daily high-risk (65+) people immune and protected from wildtype and variant
  # hr_effective_protected_variant: daily high-risk (65+) people protected severe disease from wildtype and variant
  # hr_effective_wildtype: daily high-risk (65+) people immune and protected from wildtype only
  # hr_effective_protected_wildtype: daily high-risk (65+) people protected severe disease from wildtype only
  
  # lr_vaccinated: daily low-risk (<65) people vaccinated
  # lr_unproteced: daily low-risk (<65) people who receive no benefit from vaccine
  # lr_effective_variant: daily low-risk (<65) people immune and protected from wildtype and variant
  # lr_effective_protected_variant: daily low-risk (<65) people protected severe disease from wildtype and variant
  # lr_effective_wildtype: daily low-risk (<65) people immune and protected from wildtype only
  # lr_effective_protected_wildtype: daily low-risk (<65) people protected severe disease from wildtype only
}

#######################################
# Copy national level, assign to GBD
# locations not in covid hierarchy
#######################################
split_gbd_locations <- function(model, model_parameters){
  
  split_columns <- c("hr_vaccinated","lr_vaccinated",
                     "hr_unprotected","lr_unprotected",
                     "hr_effective_variant","lr_effective_variant",
                     "hr_effective_wildtype","lr_effective_wildtype",
                     "hr_effective_protected_variant", "lr_effective_protected_variant",
                     "hr_effective_protected_wildtype", "lr_effective_protected_wildtype",
                     "cumulative_all_vaccinated","cumulative_all_effective",
                     "cumulative_all_effective_variant","cumulative_all_effective_protected_variant",
                     "cumulative_all_effective_wildtype","cumulative_all_effective_protected_wildtype",
                     'cumulative_essential_vaccinated',
                     'cumulative_adults_vaccinated',
                     'cumulative_elderly_vaccinated',
                     'cumulative_essential_effective',
                     'cumulative_adults_effective',
                     'cumulative_elderly_effective',
                     'cumulative_hr_effective_wildtype',
                     'cumulative_hr_effective_protected_wildtype',
                     'cumulative_hr_effective_variant',
                     'cumulative_hr_effective_protected_variant',
                     'cumulative_lr_effective_wildtype',
                     'cumulative_lr_effective_protected_wildtype',
                     'cumulative_lr_effective_variant',
                     'cumulative_lr_effective_protected_variant')
  
  # Find missing GBD locations
  gbd_hier <- gbd_data$get_gbd_hierarchy()

  missing_locs <- setdiff(gbd_hier[most_detailed == 1]$location_id, model$location_id)
  
  missing_hier <- gbd_hier[location_id %in% missing_locs]
  
  
  admin1 <- missing_hier[level == 4]
  
  ## Set parents to country level in hierarchy to calculate proportion of country represented
  ## These seem to be India
  
  admin2 <- missing_hier[level == 5]
  for (i in admin2$location_id){
    loc_parent_id <- parent_of_child(i, gbd_hier, parent_level = 3)
    admin2[location_id == i, parent_id := loc_parent_id]
  }
  # admin2[location_id %in% c(44539, 44540), parent_id := 165] # 165 is Pakistan, also not flexible to hierarchy updates
  
  ## These seem to be UK
  
  admin3 <- missing_hier[level == 6]
  for (i in admin3$location_id){
    loc_parent_id <- parent_of_child(i, gbd_hier, parent_level = 3)
    admin3[location_id == i, parent_id := loc_parent_id]
  }
  # admin3$parent_id <- 95 # This is not flexible to hierarchy updates
  
  
  ## Okay, take the parent level, adjust for population size
  all_pop <- model_inputs_data$load_all_populations(model_parameters$model_inputs_path)
  gbd_pops <- model_inputs_data$load_adult_population(model_parameters$model_inputs_path, model_parameters)
  gbd_o65 <- model_inputs_data$load_o65_population(model_parameters$model_inputs_path)
  setnames(gbd_o65, "over65_population", "hr_population")
  gbd_pops <- merge(gbd_pops, gbd_o65, by = "location_id")
  
  child_locs <- rbind(admin1, admin2, admin3)
  pop <- merge(child_locs, gbd_pops, by = "location_id")
  pop <- merge(pop, gbd_pops, by.x = "parent_id", by.y = "location_id")
  pop[, pop_frac_adult := adult_population.x / adult_population.y]
  pop[, pop_frac_hr := hr_population.x / hr_population.y]
  
  setnames(pop, c("adult_population.x", "hr_population.x"), c("adult_population","hr_population"))
  
  gbd_dt <- expand.grid(location_id = child_locs$location_id,
                        date = seq(min(model$date), max(model$date), by = "1 day"))
  gbd_dt <- data.table(gbd_dt)
  gbd_dt <- merge(gbd_dt, child_locs[,c("location_id","location_name","parent_id")], by = "location_id")
  
  out_gbd <- merge(gbd_dt, model[,c("location_id","date","observed",
                                    ..split_columns)], 
                   by.x = c("parent_id","date"), by.y = c("location_id","date"))
  out_gbd <- merge(out_gbd, pop[,c("location_id","pop_frac_adult","adult_population","pop_frac_hr","hr_population")], by = "location_id")
  out_gbd <- out_gbd[order(location_id, date)]
  out_gbd[, parent_id := NULL]
  
  # Apply HR population fraction and overall separately
  out_gbd[, c(split_columns[!split_columns %like% "hr"]) := lapply(.SD, function(x) x * pop_frac_adult),
          .SDcols = c(split_columns[!split_columns %like% "hr"])]
  out_gbd[, c(split_columns[split_columns %like% "hr"]) := lapply(.SD, function(x) x * pop_frac_hr),
          .SDcols = c(split_columns[split_columns %like% "hr"])]
  
  return(out_gbd)
}


################################################################################

## Fix missing aggregate values by filling in missing sums
.fix_aggregates <- function(agg_dt, interp_cols) {
  
  message('Interpolating missing observations in aggregates')
  
  do.call(
    rbind,
    pbapply::pblapply(split(agg_dt, by='location_id'), function(x) {
      
      tryCatch( { 
        
        t_current <- max(x$date[!is.na(x$fully_vaccinated)])
        t_min <- min(x$date)
        
        sel1 <- x$date == t_min
        sel2 <- x$date <= t_current
        
        for (interp_col in interp_cols) {
          if (is.na(x[[interp_col]][sel1])) x[[interp_col]][sel1] <- 0
          x[[interp_col]][sel2] <- zoo::na.approx(x[[interp_col]][sel2])
        }
        
      }, error=function(e){
        
        cat("Warning :", unique(x$location_id), ":", unique(x$location_name), "\n")
        
      })
      
      return(x)
      
    })
  )
  
}
