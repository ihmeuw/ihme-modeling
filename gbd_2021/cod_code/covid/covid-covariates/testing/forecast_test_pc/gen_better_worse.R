gen_better_worse_all <- function(dt, out_dt, projection_end_date=NULL) {
  
  if (is.null(projection_end_date)) projection_end_date <- Sys.Date() + 180

  last_date <- dt[, .(max_date = max(date)), by = .(location_name, location_id)]
  merge_dt <- merge(out_dt, last_date, by = c("location_id", "location_name"))
  
  # Calculate difference after last observed date to get post-raked slope
  date_cap <- max(merge_dt[(observed) == T & date < Sys.Date()]$date)
  last_plus_one_dt <- merge_dt[date == max_date + 1, .(location_id, raked_ref_test_pc)]
  arc_dt <- merge_dt[(date == date_cap + 1 | date == date_cap + 2), .(location_name, location_id, date, raked_ref_test_pc), by = .(location_name, location_id)]
  diff_dt <- arc_dt[, .(diff = diff(raked_ref_test_pc)), by = .(location_id)]
  
  # Calculate last val associated with post-raked reference
  last_dt <- merge(last_plus_one_dt, diff_dt)
  last_dt[, last_val := raked_ref_test_pc - diff]
  
  # Combine relevant data
  scenario_dt <- merge(last_date, last_dt, by = "location_id")

  subnats <- intersect(hierarchy[level > 3]$location_id, scenario_dt$location_id)
  nats <- unique(hierarchy[location_id %in% subnats]$parent_id)

  subnats_better_worse_dt <- rbindlist(lapply(nats, function(nat) {
    
    tryCatch( { 
      
      locs <- hierarchy[level > 3 & parent_id == nat]$location_id
      
      # This function appears to take the slope observed at the last time point and extends a linear line with the same slope out to a defined stop date
      subnat_better_worse <- gen_better_worse(loc_ids = locs, 
                                              dt = scenario_dt,
                                              projection_end_date = projection_end_date)
      
    }, warning=function(w){
      
      cat("Warning :", nat, ":", conditionMessage(w), "\n")
      
    }, error=function(e){
      
      cat("Error :", nat, ":", conditionMessage(e), "\n")
      
    })
    
  }))
  
  other_better_worse <- gen_better_worse(loc_ids = setdiff(unique(diff_dt$location_id), subnats), 
                                         dt = scenario_dt,
                                         projection_end_date = projection_end_date)
  
  better_worse_dt <- rbindlist(list(subnats_better_worse_dt, other_better_worse))
  better_worse_dt <- subnats_better_worse_dt
  out_dt <- merge(out_dt, better_worse_dt, by = c("location_id", "date"), all.x = T)

  # Replace better or worse with reference if outside range
  out_dt[raked_ref_test_pc > better_fcast, better_fcast := raked_ref_test_pc]
  out_dt[raked_ref_test_pc < worse_fcast, worse_fcast := raked_ref_test_pc]
  out_dt[(observed), c("better_fcast", "worse_fcast") := raked_ref_test_pc]

  return(out_dt)
  
}

gen_better_worse <- function(loc_ids, dt, projection_end_date) {
  
  subset_dt <- dt[location_id %in% loc_ids]
  
  # Calculate lwr and upper rates
  lwr_rate <- quantile(subset_dt$diff, 0.15, na.rm = T)
  upr_rate <- quantile(subset_dt$diff, 0.85, na.rm = T)
  
  # Identify a safe number of days to project
  extend_date <- as.Date(projection_end_date) - min(subset_dt$max_date)
  
  # Make projections
  last_mat <- matrix(rep(subset_dt$last_val, extend_date), ncol = extend_date)
  lwr_mat <- last_mat + matrix(rep(t(1:extend_date * lwr_rate), nrow(subset_dt)), ncol = extend_date, byrow = T)
  upr_mat <- last_mat + matrix(rep(t(1:extend_date * upr_rate), nrow(subset_dt)), ncol = extend_date, byrow = T)
  lwr_vec <- c(t(lwr_mat))
  upr_vec <- c(t(upr_mat))
  
  # Calculate dates for each location
  date_mat <- subset_dt$max_date + matrix(rep(seq(extend_date), nrow(subset_dt)), ncol = extend_date, byrow = T)
  date_vec <- c(t(date_mat))
  
  # Make an output table
  loc_vec <- rep(loc_ids, each = extend_date)
  
  temp_dt <- data.table(
    location_id = loc_vec, 
    date = date_vec,
    worse_fcast = lwr_vec, 
    better_fcast = upr_vec
  )

  return(temp_dt)
}
