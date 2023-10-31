##################################################
## Project: CVPDs
## Script purpose: Calculate 2020 measles cases using modeled ratio to account for COVID
## This script produces annual, draw-wise scalars.
## Date: April 2021
## Author: username
##################################################
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <-"FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2, plyr, dplyr, pbapply)

# ARGUMENTS -------------------------------------  
username <- Sys.info()[["user"]]
date <- gsub("-", "_", Sys.Date())
gbd_round_id <- 7
decomp_step <- "step3"
cause <- "measles" # pertussis, lri, or measles
fatality <- "fatal" # fatal or nonfatal
year_fill <- NULL # NULL or 2021; i.e., to forecast or not to forecast? 

if(cause == "measles") {
  subnational <- F 
  cause_compare <- "measles"
  seas_date <- "2021_05_24" 
} else {
  subnational <- T
  cause_compare <- "flu" 
  seas_date <- "2021_06_08"
}
out_dir <- paste0("FILEPATH")
dir.create(out_dir, showWarnings = F)
# Input model versions and filepaths
ratio_model_version <- "2021-06-14"
ratio_path <- paste0("FILEPATH")
seas_path <- paste0("FILEPATH")
measles_expected_path <- paste0("FILEPATH")

# SOURCE SHARED FUNCTIONS ------------------------------
invisible(sapply(list.files("FILEPATH"), source))
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))
source("FILEPATH")

# Function to duplicate selected national rows subnationally
# Duplicate annual ratios for the subnationals
expand_subnational <- function(dt, hierarchy){
  loc_estimate <- hierarchy[most_detailed == 1]
  l <- strsplit(as.character(loc_estimate$path_to_top_parent), ',')
  df1new <- data.frame(country_id = as.integer(unlist(l)), 
                       path_to_top_parent = rep(loc_estimate$path_to_top_parent, lengths(l)),
                       location_id = rep(loc_estimate$location_id, lengths(l)))
  dt <- merge(dt, df1new, by.x = "location_id", by.y = "country_id", all.x = TRUE, allow.cartesian = T)
  dt[, location_id := NULL]
  setnames(dt, "location_id.y", "location_id")
  return(dt)
}

# Function to scale cascade spline results for trusted locs for Jan-Apr 2021 for locs missing case reports for those months
# Using the ratio (sum(mar-dec 2020 case notifs for country x)/sum(mar-dec 2020 cases as predicted by the cascade spline for country x)
scale_cascade_spline <- function(adjusted){
  notifications <- get_trusted_measles(years = 2020,
                                       gbd_round_id = 7,
                                       decomp_step = "iterative",
                                       time = "monthly",
                                       se = T,
                                       draws = F)
  notifications <- notifications[month >= 3]
  notifications <- notifications[, .(cases = sum(cases)), .(location_id, ihme_loc_id, year_id)]
  # summarize the cascade-spline-ratio-adjusted-ST-GPR cases for Mar-Nov
  drawnames <- names(adjusted)[names(adjusted) %like% "draw_"]
  cascade_result <- adjusted[year_id==2020 & month>=3]
  cascade_result <- cascade_result[, lapply(.SD, sum), by = c("location_id", "year_id"), .SDcols = drawnames]
  cascade_result <- collapse_point(cascade_result)
  cascade_result <- cascade_result[location_id %in% unique(notifications$location_id)]
  merged <- merge(notifications, cascade_result, by = c("location_id", "year_id"))
  merged[, trusted_spline_scalar := cases/mean]
  merged[cases == 0 & mean == 0, trusted_spline_scalar := 0] # n the total absence of 2021 data, countries without cases in 2020 should end up with final cases of 0 in early 2021
  return(merged)
}

replace_trusted_measles <- function(adjusted, fatality, time, hierarchy, month_start, month_end, year_start, year_end){
  if (fatality == "fatal") stop("This only works for nonfatal.")
  adjusted_notifications <- get_trusted_measles(years = year_start:year_end,
                                                gbd_round_id = 7,
                                                decomp_step = "iterative",
                                                time = time,
                                                se = T,
                                                draws = T)
  if(time == "monthly"){
    # Keep only trusted notifs from month_start year_start to month_end year_end
    if(year_start != year_end) adjusted_notifications <- adjusted_notifications[(month >= month_start & year_id == year_start) | (month <= month_end & year_id == year_end) | (year_id > year_start & year_id < year_end)]
    if(year_start == year_end) adjusted_notifications <- adjusted_notifications[month >= month_start & month <= month_end & year_id == year_start]
    adjusted[, matchvar := paste(location_id, month, year_id, sep = "_")]
    adjusted_notifications[, matchvar := paste(location_id, month, year_id, sep = "_")]
  } else if (time == "annual"){
    # Keep only trusted notifs from year_start to year_end
    adjusted_notifications[year_id %in% year_start:year_end]
    adjusted[, matchvar := paste(location_id, year_id, sep = "_")]
    adjusted_notifications[, matchvar := paste(location_id, year_id, sep = "_")]
  }
  
  # Drop the location-month-year combinations that are in adjusted_notifications_expanded
  adjusted <- adjusted[!matchvar %in% unique(adjusted_notifications$matchvar)]
  counts <- rbind(adjusted, adjusted_notifications, fill = T)
  return(counts)
}

get_monthly <- function(data, pred_seas, drawnames){
  # initialize empty list with length == nrow(dataframe)
  list_date_dfs <- vector(mode = "list", length = nrow(data))
  
  # for-loop generates new dates and adds as dataframe to list
  list_date_dfs <- pblapply(1:length(list_date_dfs), function(i){
    
    # transfer dataframe row to variable `row`
    row <- data[i,]
    
    # add tibble to list
    date_df <- data.table(row, month = 1:12)
    return(date_df)
  }
  , cl = 8)
  
  # bind dataframe list elements into single dataframe
  df_monthly <- rbindlist(list_date_dfs)
  
  # now merge in the seasonality, which was generated for all locs in predict_annual_cases! 
  df_monthly <- merge(df_monthly, pred_seas, by=c("month", "location_id"))
  
  # multiply the mean by the seasonality and divide by 12 to get monthly
  df_monthly[, (drawnames) := (.SD*lat_ratio)/12, .SDcols = drawnames]
  return(df_monthly)
}

# Fill the monthly ratios with scalars of 1 (zero effect) for missing months in future
# Also check for month completeness
fill_monthly_ratio <- function(ratio_path, partial_year_fill = 2021){
  # Read in predicted monthly ratios (month will not be NA for rows with that month's official covid team mob estimate)
  pred_month_disrup_ratio <- fread(ratio_path)[!is.na(month)]
  pred_month_disrup_ratio[is.na(year), year := year(date)]
  setnames(pred_month_disrup_ratio, "year", "year_id")
  if(is.null(partial_year_fill)){
    # Keep only complete years
    count_check <- length(unique(pred_month_disrup_ratio$location_id))*12
    count_check <- pred_month_disrup_ratio[, .N, by = "year_id"][N == count_check]
    pred_month_disrup_ratio <- pred_month_disrup_ratio[year_id == count_check$year_id]
  } else {
    # Fill missing months with predicted zeros
    add_grid <- lapply(partial_year_fill, function(check_year){
      months_add <- setdiff(unique(pred_month_disrup_ratio$month), unique(pred_month_disrup_ratio[year_id==check_year]$month))
      if(length(months_add)!=0)add_grid <- expand.grid(year_id = check_year, month = months_add, location_id = unique(pred_month_disrup_ratio$location_id)) else add_grid <- data.table()
      return(add_grid)
    }
    )
    fill_list <- rbindlist(add_grid)
    fill_list[, paste0("ratio_draw_", 0:999) := 1]
    pred_month_disrup_ratio <- rbind(pred_month_disrup_ratio, fill_list, fill = T)
  }
  return(pred_month_disrup_ratio)
}

# Function to get the annual ratio by location, from seasonality and the monthly ratios
get_annual_ratio <- function(seas_dt, pred_month_disrup_ratio, hierarchy = hierarchy){
  # Merge seasonality with pred ratio and use wts to calc annual ratio, draw-wise
  drawnames <- names(pred_month_disrup_ratio)[names(pred_month_disrup_ratio) %like% "draw"]
  seas_dt <- merge(seas_dt, pred_month_disrup_ratio[, c(drawnames, "location_id", "month", "mask_avg_month", "mob_avg_month", "year_id"), with = F], 
                   by = c("location_id", "month"))
  annual_avg_ratio <- seas_dt[, lapply(.SD, weighted.mean, w = wt), .(location_id, year_id), .SDcols = drawnames]
  return(annual_avg_ratio)
}

# get location data
hierarchy <- get_location_metadata(35, gbd_round_id = 7, decomp_step = "iterative")
countries <- hierarchy[level == 3]
age_meta <- get_age_metadata(age_group_set_id = 19, gbd_round_id = 7)

# get annual ratios from monthly ratios
monthly_ratio <- fill_monthly_ratio(ratio_path, partial_year_fill = year_fill)
seas_dt <- as.data.table(read.xlsx(seas_path))
annual_ratio <- get_annual_ratio(seas_dt, monthly_ratio, hierarchy = hierarchy)
# Duplicate annual ratios for the subnationals
if(subnational){
  annual_ratio <- expand_subnational(annual_ratio, hierarchy)
  ids <- c("location_id", "year_id", "path_to_top_parent")
} else ids <- c("location_id", "year_id")
drawnames <- names(annual_ratio)[names(annual_ratio) %like% "draw"]
# Reshape to long, get into correct format for draw manipulation
annual_long <- melt(annual_ratio, id.vars = ids, variable.name = "draw",
                    value.name = "ratio")
annual_long$draw <- as.numeric(substr(annual_long$draw, 12, nchar(as.character(annual_long$draw))))

# FOR MEASLES ONLY - recalculate ratios from trusted locs case counts (fatal) or use case counts directly (nonfatal)
if(cause == "measles"){
  custom_nf_results <- fread(measles_expected_path)
  if(fatality == "fatal"){ stop ("This only works for fatal")} else if(fatality == "nonfatal"){
    pred_month_disrup_ratio <- fill_monthly_ratio(ratio_path, partial_year_fill = 2021)
    # Get expected case counts from ST-GPR results and ratio
    adjusted <- get_adjusted_measles(expected_dir = measles_expected_path, 
                                     ratio_dt = pred_month_disrup_ratio,
                                     years = 2020:2021, 
                                     time = "monthly",
                                     seas_path = seas_path)
    # Replace ratios with trusted counts for measles up to desired month and year:
    month_start <- 1
    month_end <- 4
    year_start <- 2020
    year_end <- 2021
    # This function also uses the ratio_dt and the ST-GPR results to get expected cases
    pred_month_disrup_ratio_trusted <- replace_trusted_measles(adjusted = adjusted, 
                                                               fatality = "nonfatal", 
                                                               time = "monthly",
                                                               hierarchy = hierarchy,
                                                               month_start = month_start, 
                                                               month_end = month_end,
                                                               year_start = year_start,
                                                               year_end = year_end)
    elimination_locs <- c("BTN", "LKA", "MDV", "CHN_354", "CHN_361", "TLS", "BHR", "KHM", "PRK", "OMN", "IRN", "JOR", "CHN") # elimination locations outside of trusted CN superregions. 
    trusted_srs <- c(64, 31, 103)
    # set up trusted_locs
    trusted_locs <- hierarchy[super_region_id %in% trusted_srs | ihme_loc_id %in% elimination_locs, location_id]
    pred_month_disrup_ratio_trusted <- pred_month_disrup_ratio_trusted[location_id %in% trusted_locs]
    # Deal with trusted locations that are missing Jan-Apr 2021 reports: apply scalar
    scalar <- scale_cascade_spline(adjusted)
    # lat_ratio will be non-NA for locations using cascade_spline and seasonality and STGPR for predict
    pred_month_disrup_ratio_trusted <- merge(scalar[,.(location_id, trusted_spline_scalar)], pred_month_disrup_ratio_trusted, by = "location_id", all.y = T)
    drawnames <- names(pred_month_disrup_ratio_trusted)[names(pred_month_disrup_ratio_trusted) %like% "draw_"]
    pred_month_disrup_ratio_trusted[year_id == 2021 & month <= 4 & !is.na(lat_ratio) &!is.na(trusted_spline_scalar), (drawnames) := lapply(.SD, "*", trusted_spline_scalar), .SDcols = drawnames]
    # aggregate to annual cases for each year by summing monthly
    pred_month_disrup_ratio_trusted <- pred_month_disrup_ratio_trusted[, lapply(.SD, sum), by = c("location_id", "year_id"), .SDcols = drawnames]
    # Merge the expected draws with the ratio draws and multiply to get "observed" counts for non-trusted locations
    expected <- custom_nf_results[year_id %in% unique(annual_ratio$year_id) & location_id %in% unique(annual_ratio$location_id)]
    # Drop the trusted locations from expected_counts
    annual_ratio <- annual_ratio[!location_id %in% unique(pred_month_disrup_ratio_trusted$location_id)]
    expected_counts <- merge(expected, annual_ratio, by=c("location_id", "year_id"))
    # DO NOT duplicate for subnationals OR age/sex since we are in count space - leave that to Emma
    end <- 999
    expected_counts <- expected_counts[, paste0("inc_draw_",0:end) := lapply(0:end, function(x) {get(paste0("ratio_draw_", x)) * get(paste0("case_draw_",x))})]
    expected_counts <- expected_counts[,c(paste0("ratio_draw_",0:end), paste0("case_draw_",0:end)):=NULL]
    counts <- rbind(expected_counts, pred_month_disrup_ratio_trusted, fill = T)
    all_wide <- cbind(cause_id = 341, counts)
  }
  write.csv(all_wide, paste0(out_dir, date, "_", cause, ".csv"), row.names = F)
}

# FOR LRI ONLY - deal with mutliple etiologies
if (cause == "lri"){
  # apply annual ratio to both flu and rsv draws for all ages, sexes, locations
  # NEED to use % - otherwise, the draws do not line up in # space and a flu draw may be > than a parent LRI draw
  if(fatality == "fatal") measure <- 1 else if(fatality == "nonfatal") measure <- 3
  etio_draws <- get_draws(gbd_id_type = c("rei_id", "rei_id", "cause_id"),
                          gbd_id = c(187,190,322),
                          metric_id = 2,
                          measure_id = measure,
                          source = "burdenator",
                          version_id = 215,
                          gbd_round_id = 7,
                          decomp_step = "iterative",
                          location_id = annual_ratio$location_id,
                          age_group_id = age_meta$age_group_id,
                          sex_id = c(1,2),
                          year_id = c(2020),
                          num_workers = 10)
  etio_copy <- copy(etio_draws)
  cols.remove <- c("measure_id", "metric_id", "version_id")
  etio_draws[,(cols.remove) := NULL]
  # reshape from wide to long
  id_vars <- c("location_id", "year_id", "sex_id", "age_group_id", "cause_id", "rei_id")
  flu_dt <- melt(etio_draws[rei_id == 187], id.vars = id_vars, variable.name = "draw", 
                 value.name = "flu")
  rsv_dt <- melt(etio_draws[rei_id == 190], id.vars = id_vars, variable.name = "draw", 
                 value.name = "rsv")
  lri_dt <- merge(flu_dt, rsv_dt, by= c("location_id", "year_id", "sex_id", "age_group_id", "cause_id", "draw"))
  lri_dt$draw <- as.numeric(substr(lri_dt$draw, 6, nchar(as.character(lri_dt$draw))))
  if(max(lri_dt$draw)<100){ # If we only have 100 burdenator draws
    # rescale annual_long draws from 0 to 99 repeated
    annual_long$draw_original <- annual_long$draw
    annual_long$draw <- annual_long$draw - 100*(floor(annual_long$draw/100))
  }
  # don't merge on year - apply 2020 proportions to counterfactual for both years
  test <- merge(lri_dt, annual_long, by = c("location_id", "draw"), allow.cartesian = T)
  # keep only years from the annual ratio
  setnames(test, "year_id.y", "year_id")
  if(max(lri_dt$draw)<100){ 
    # get the draw numbering back to normal
    test$draw <- test$draw_original
  }
  if(fatality == "fatal"){
    lri_draws <- get_draws(gbd_id_type = "cause_id",
                           gbd_id = 322,
                           source = "codcorrect",
                           measure_id = 1, 
                           metric_id = 1,
                           version_id = 253, # MUST use version before scalars applied
                           gbd_round_id = 7,
                           decomp_step = "step3",
                           location_id = unique(test$location_id),
                           age_group_id = age_meta$age_group_id,
                           sex_id = c(1,2),
                           year_id = c(2020, year_fill), 
                           num_workers = 10)
  } else if(fatality == "nonfatal"){
    lri_draws <- get_draws(gbd_id_type = "modelable_entity_id",
                           gbd_id = 1258,
                           source = "epi",
                           measure_id = 6, 
                           metric_id = 3,
                           status = "best",
                           gbd_round_id = 7,
                           decomp_step = "iterative",
                           location_id = unique(annual_ratio$location_id),
                           age_group_id = age_meta$age_group_id,
                           sex_id = c(1,2),
                           year_id = c(2020, year_fill), 
                           num_workers = 10)
    # Convert rate to number space with population 
    pop <- get_population(age_group_id = age_meta$age_group_id, gbd_round_id = 7, decomp_step = "step3", sex_id = c(1,2), 
                          location_id = unique(hierarchy[most_detailed==1]$location_id), year_id = 2020)
    lri_draws <- merge(lri_draws, pop, by = c('location_id', 'sex_id', 'year_id', 'age_group_id'))
    drawnames <- paste0("draw_", 0:999)
    lri_draws[, (drawnames) := lapply(.SD, "*", population), .SDcols = drawnames]
  }
  
  # reshape LRI draws to long
  lri_draws[,`:=`(model_version_id = NULL, modelable_entity_id = NULL, population = NULL, run_id = NULL, cause_id = NULL, version_id = NULL)]
  id_vars <- c("location_id", "year_id", "sex_id", "age_group_id", "measure_id", "metric_id")
  lri_long <- melt(lri_draws, id.vars = id_vars, variable.name = "draw", 
                   value.name = "lri_count")
  lri_long$draw <- as.numeric(substr(lri_long$draw, 6, nchar(as.character(lri_long$draw))))
  # merge with the etiology fractions
  all_long <- merge(lri_long, test, by = c("location_id", "year_id", "draw", "sex_id", "age_group_id"))
  # perform operations
  # get mort counts from the percent
  all_long[, `:=` (flu_count = flu*lri_count, rsv_count = rsv*lri_count)]
  # multiply by annual ratio to get flu and rsv adjusted counts
  all_long[, `:=` (flu_count_adj = flu_count*ratio, rsv_count_adj = rsv_count*ratio)]
  # how many LRI deaths to remove in count space
  all_long[, lri_remove := (flu_count+rsv_count-(flu_count_adj+rsv_count_adj))]
  all_long[, lri_adjusted := lri_count - lri_remove]
  all_long[, lri_ratio := lri_adjusted/lri_count]
  # Take care of case where adjusted and original count both = 0
  all_long[lri_adjusted == 0 & lri_count == 0, lri_ratio := 1]
  # TEMP - CAP at 90% disruption
  # ISSUE - this still does not reflect if flu_count + rsv_count > lri_count, need to fix
  all_long[lri_ratio < 0.1, lri_ratio := 0.1]
  # cast from long to wide
  all_wide <- data.table::dcast(all_long, location_id + year_id + sex_id + age_group_id ~ draw, 
                                value.var = "lri_ratio")
  all_wide <- cbind(cause_id = 322, all_wide)
  setnames(all_wide, paste0(0:999), paste0("ratio_draw_", 0:999), skip_absent = T)
  write.csv(all_wide, paste0(out_dir, date, "_", cause, ".csv"), row.names = F)
}

# FOR PERTUSSIS ONLY this is the simplest
if (cause == "pertussis"){
  # apply flu annual ratio to pertussis draws for all ages, sexes, locations
  pertussis <- get_draws(gbd_id_type = "modelable_entity_id",
                         gbd_id = 1424,
                         source = "epi",
                         measure_id = 6, 
                         metric_id = 3,
                         status = "best",
                         gbd_round_id = 7,
                         decomp_step = "iterative",
                         location_id = unique(annual_ratio$location_id),
                         age_group_id = age_meta$age_group_id,
                         sex_id = c(1,2),
                         year_id = c(2020, year_fill), 
                         num_workers = 10)
  
  # reshape pertussis draws to long
  pertussis[,`:=`(model_version_id = NULL, modelable_entity_id = NULL)]
  id_vars <- names(pertussis)[!names(pertussis)%like%"draw"]
  pertussis_long <- melt(pertussis, id.vars = id_vars, variable.name = "draw", 
                         value.name = "pertussis_rate")
  pertussis_long$draw <- as.numeric(substr(pertussis_long$draw, 6, nchar(as.character(pertussis_long$draw))))
  
  # add missing age groups
  missing_ages_draws <- expand.grid(age_group_id=setdiff(age_meta$age_group_id, pertussis$age_group_id), 
                                    location_id = unique(pertussis$location_id),
                                    measure_id = 6, metric_id = 3, sex_id = 1:2, year_id = c(2020, year_fill), 
                                    draw = unique(pertussis_long$draw), pertussis_rate = 0)
  pertussis_long <- rbind(pertussis_long, missing_ages_draws)
  # merge with the annual ratio 
  if(max(pertussis_long$draw)<100){ # If we only have 100 burdenator draws
    # rescale annual_long draws from 0 to 99 repeated
    annual_long$draw_original <- annual_long$draw
    annual_long$draw <- annual_long$draw - 100*(floor(annual_long$draw/100))
  }
  all_long <- merge(pertussis_long, annual_long, by = c("location_id", "year_id", "draw"), allow.cartesian = T)
  if(max(pertussis_long$draw)<100){ 
    # get the draw numbering back to normal
    all_long$draw <- all_long$draw_original
  }
  # multiply by the ratio to get the adjusted incidence rate
  all_long[, pertussis_adjusted_rate := pertussis_rate * ratio]
  if(fatality == "fatal"){
    # keep the ratios 
    all_wide <- data.table::dcast(all_long, location_id + year_id + sex_id + age_group_id ~ draw, 
                                  value.var = "ratio")
    setnames(all_wide, paste0(0:999), paste0("ratio_draw_", 0:999), skip_absent = T)
  } else if(fatality == "nonfatal"){
    # keep the adjusted incidence rates
    all_wide <- data.table::dcast(all_long, location_id + year_id + sex_id + age_group_id ~ draw, 
                                  value.var = "pertussis_adjusted_rate")
    setnames(all_wide, paste0(0:999), paste0("inc_draw_", 0:999), skip_absent = T)
  }
  all_wide <- cbind(cause_id = 339, all_wide)
  write.csv(all_wide, paste0(out_dir, date, "_", cause, ".csv"), row.names = F)
}