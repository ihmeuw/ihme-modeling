##################################################
## Project: CVPDs
## Script purpose: Test approaches for calculating and applying RegMod Scalar
## Date: May 2022
## Author: USERNAME
##################################################

# Uncomment, paste into console, comment out, and then launch the submit_job
#source("FILEPATH")
# submit_job("FILEPATH")

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2, magrittr, pbapply)
username <- Sys.info()[["user"]]

date <- gsub("-", "_", Sys.Date())

gbd_round_id <- 7
decomp_step <- "step3"
approach <- "all"

# SOURCE SHARED FUNCTIONS ------------------------------
shared_functions <- "FILEPATH"
functions <- c("get_location_metadata.R", "get_model_results.R", "get_draws.R", "get_age_metadata.R", "get_population.R")
for (func in functions) {
  source(paste0(shared_functions, func))
}

source("FILEPATH")
repo_dir <- ifelse(username == "USERNAME", paste0(h, "/code/"), paste0(h, "00_repos/"))
source(paste0(repo_dir, "/covid_impact/predictions/get_flu_rsv_cases.R"))
source(paste0(repo_dir, "/covid_impact/predictions/fill_regional_average.R"))
source(paste0(repo_dir, "/covid_impact/predictions/calculate_urr.R"))
source(paste0(repo_dir, "/covid_impact/predictions/get_trusted_measles.R"))
source(paste0(repo_dir, "/covid_impact/predictions/get_adjusted_measles.R"))
source(paste0(repo_dir, "/covid_impact/predictions/aggregate_to_national.R"))

# Function to duplicate selected national rows subnationally
# Duplicate annual ratios for the subnationals
expand_subnational <- function(dt, hierarchy, keep_national_parents){
  loc_estimate <- hierarchy[most_detailed == 1]
  l <- strsplit(as.character(loc_estimate$path_to_top_parent), ',')
  df1new <- data.frame(country_id = as.integer(unlist(l)), 
                       path_to_top_parent = rep(loc_estimate$path_to_top_parent, lengths(l)),
                       location_id = rep(loc_estimate$location_id, lengths(l)))
  national_parents <- dt[location_id %in% hierarchy[level == 3 & !(location_id %in% loc_estimate$location_id)]$location_id]
  dt <- merge(dt, df1new, by.x = "location_id", by.y = "country_id", all.x = TRUE, allow.cartesian = T)
  dt[, c("location_id","path_to_top_parent") := NULL]
  setnames(dt, "location_id.y", "location_id")
  
  if(keep_national_parents) dt <- rbind(dt, national_parents)
  return(dt)
}

# Split draws of annual GBD cases into draws of monthly GBD cases (takes data with draws long)
get_monthly <- function(data, pred_seas, drawnames = NA, format){ 
  #format - specify long or wide
  #drawnames must be specified if format is long
  
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
  
  # now merge in the seasonality, which was generated for all locs in predict_annual_cases 
  df_monthly <- merge(df_monthly, pred_seas, by=c("month", "location_id"))
  
  if(format == "long"){
    df_monthly[, gbd_cases := gbd_cases*wt/12]
  } else {
    # multiply the draws by the seasonality and divide by 12 to get monthly draws
    df_monthly[, (drawnames) := (.SD*wt)/12, .SDcols = drawnames]
  }
  
  return(df_monthly)
}

# ARGUMENTS -------------------------------------  
cause <- "measles"

seas_path <- paste0("FILEPATH") 

cause_id <- ifelse(cause == "measles", 341, ifelse(cause == "flu", 322, 339))
year_start <- 2020
year_end <- if(cause == "measles") 2021 else 2022
subnational <- TRUE
urr_drawwise <- FALSE
floor_regmod_draws <- TRUE
trim_extreme_regmod <- TRUE # drop RegMod draws by location-year that are below Q1 - 1.5(IQR) or above Q3+1.5(IQR) and resample randomly
subset_2020 <- TRUE # calculate scalars for 2020 using Apr-Dec rather than full year (ie assume jan-mar are not affected by COVID)
jan_mar_std <- FALSE # standardize monthly ratios for 2020 to Jan-Mar average?

reference_pd_start <- 2015  # ran with 2015 for measles, 2017 for flu/pertussis
fatal_nonfatal_toggle <- "nonfatal" #Toggle for fatal or nonfatal scalars, only relevant for flu/LRI


out_dir <- paste0("FILEPATH")
if(!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

hierarchy <-  get_location_metadata(35, gbd_round_id = 7, decomp_step = "iterative")
countries <- hierarchy[level == 3]$location_id
age_meta <- get_age_metadata(19, gbd_round_id = 7)

measles_expected_path <- paste0(j, "FILEPATH")

if(cause %in% c("flu", "measles")){
  regmod <- fread(paste0("FILEPATH"))
} else if (cause == "pertussis"){
  regmod <- fread("FILEPATH")
}

drawnames <- names(regmod)[names(regmod) %like% "draw_"]
regmod[,(drawnames) := lapply(.SD, as.numeric), .SDcols = drawnames]

setnames(regmod, "deaths", "observed_cases")
setnames(regmod, "cases", "predicted_cases")

# Drop locations with at least 6 months of missing data in any yr from start of reference period to end of predicting for all causes
regmod[, sumNA := sum(is.na(observed_cases)), by = c("location_id", "year_id")]
regmod <- regmod[year_id %in% reference_pd_start:year_end][, max_missing := max(sumNA), by = "location_id"]
locs_drop <- unique(regmod[max_missing > 6]$location_id) # 67 when go back to 2015
regmod <- regmod[!(location_id %in% locs_drop)]

# go to long
regmod_long <- melt.data.table(regmod[year_id %in% reference_pd_start:2022], id.vars = c("location_id", "year_id", "month"), measure.vars = drawnames, value.name = "regmod_cases", variable.name = "draw") 
regmod_long$draw <- as.numeric(substr(regmod_long$draw, 12, nchar(as.character(regmod_long$draw))))

if(trim_extreme_regmod){
  # drop top and bottom outside of boxplot outlier threshold (q3 +1.5*IQR and q1-1.5*IQR)
  regmod_long[, `:=` (lower = quantile(regmod_cases, probs = 0.25) - (1.5*(quantile(regmod_cases, probs = 0.75) - quantile(regmod_cases, probs = 0.25))), 
                      upper = quantile(regmod_cases, probs = 0.75) + (1.5*(quantile(regmod_cases, probs = 0.75) - quantile(regmod_cases, probs = 0.25)))), by = c("location_id", "year_id", "month")]
  regmod_long <- regmod_long[regmod_cases > upper | regmod_cases < lower, trim := 1][order(location_id, year_id, month)]
  regmod_long[trim == 1, regmod_cases := NA]
  
  # replace with draws randomly sampled with replacement from the remaining 950 draws
  regmod_long[, temp_cases := sample(regmod_cases[!is.na(regmod_cases)], 1000, replace = TRUE), by = c("location_id", "year_id", "month")]
  regmod_long[trim == 1, regmod_cases := temp_cases]
  regmod_long[, c("trim", "temp_cases", "lower", "upper") := NULL]
  
}



# floor any remaining small draws
if(floor_regmod_draws) regmod_long[regmod_cases < 0.1, regmod_cases := 0.1] #regmod[, (drawnames) := lapply(.SD, function(x) ifelse(x < 0.1, 0.1, x)), .SDcols= drawnames]


# sum to annual cases
# regmod <- regmod[, lapply(.SD, sum), by = c("location_id", "year_id"), .SDcols = drawnames]
regmod_long_month <- copy(regmod_long)[year_id == 2020]
regmod_long <- regmod_long[, .(regmod_cases = sum(regmod_cases)),.(location_id, year_id, draw)]


# calculate scalar draw wise

start_time<-Sys.time()

###### APPROACH 1: for 2020, 2021, 2022 calculate scalar of RegMod est/avg RegMod est 2017-2019 and multiply 2020, 2021, 2022's GBD estimates by the year-specific scalar
# assumes that w/0 covid reported cases in 2020, 21, 22 would have been same as 2017-2019 average 
# (since that's denom of scalar we calculate)

# this chunk will run even if approach 1 is not selected because things generated in this chunk are needed in other approaches
prev_yr_avg_long <- regmod_long[year_id %in% reference_pd_start:2019, .(prevyrs_avg_cases = mean(regmod_cases)), .(location_id, draw)] 

prev_yr_avg <- dcast(prev_yr_avg_long, location_id ~ draw, value.var = "prevyrs_avg_cases")
setnames(prev_yr_avg, paste0(0:999), paste0("prevyrs_draw_", 0:999))

scalar_a1 <- merge(regmod_long[year_id %in% year_start:year_end], prev_yr_avg_long, by = c("location_id", "draw"))
scalar_a1[, ratio := regmod_cases/prevyrs_avg_cases]
scalar_a1[, regmod_cases := NULL][, prevyrs_avg_cases := NULL]
scalar_a1 <- dcast(scalar_a1, ...~draw, value.var = "ratio")
setnames(scalar_a1, paste0(0:999), paste0("ratio_draw_", 0:999))


# Fill missing countries with average draws of countries in the region or super region if none in region
scalar_a1 <- fill_regional_median(dt = scalar_a1,
                                  countries = countries,
                                  draw_prefix = "ratio_draw_")
scalar_a1 <- fill_regional_median(dt = scalar_a1,
                                  countries = countries,
                                  draw_prefix = "ratio_draw_",
                                  id_cols = c("super_region_id", "year_id"))

# Duplicate annual ratios for the subnationals
if(subnational){
  scalar_a1 <- expand_subnational(scalar_a1, hierarchy, keep_national_parents = FALSE)
}


if(cause == "measles"){
  scalar_a1 <- scalar_a1[year_id %in% 2020:2021]
  scalar_a1 <- cbind(cause_id = 341, scalar_a1)
  # save approach 1 scalars
  fwrite(scalar_a1, paste0(out_dir, date, "_", cause, "_all_locs_scalar_a1.csv"))
  
} else if(cause == "flu"){
  
  ## translate the flu-specific all-age, both-sex scalars to age-specific, sex-specific LRI scalars
  scalar_a1_long <- melt(scalar_a1, id.vars = c("location_id", "year_id"), measure.vars = paste0("ratio_draw_", 0:999), 
                         variable.name = "draw", value.name = "ratio")
  scalar_a1_long$draw <- as.numeric(substr(scalar_a1_long$draw, 12, nchar(as.character(scalar_a1_long$draw))))
  
  lri_list <- lapply(2020:2022, function(yr){
    
    gbd_flu_rsv_cases <- get_flu_rsv_cases(year = yr, age_meta = age_meta, fatal_nonfatal = fatal_nonfatal_toggle) 
    # scalar_a1_long is not age or sex-specific so don't merge on those
    lri_scalar_long <- merge(scalar_a1_long, gbd_flu_rsv_cases, by = c("location_id", "year_id", "draw"))
    
    # multiply by annual ratio to get flu and rsv adjusted counts
    lri_scalar_long[, `:=` (flu_count_adj = flu_count*ratio, rsv_count_adj = rsv_count*ratio)]
    # how many LRI deaths to remove in count space
    lri_scalar_long[, lri_remove := (flu_count+rsv_count-(flu_count_adj+rsv_count_adj))]
    lri_scalar_long[, lri_adjusted := lri_count - lri_remove]
    
    lri_scalar_long[, lri_ratio := lri_adjusted/lri_count]
    
    
    # Take care of case where adjusted and original count both = 0
    lri_scalar_long[lri_adjusted == 0 & lri_count == 0, lri_ratio := 1]
    # TEMP - CAP at 90% disruption
    # ISSUE - this still does not reflect if flu_count + rsv_count > lri_count, need to fix
    lri_scalar_long[lri_ratio < 0.1, lri_ratio := 0.1]
    # cast from long to wide
    lri_scalar_a1 <- data.table::dcast(lri_scalar_long, location_id + year_id + sex_id + age_group_id ~ draw, 
                                       value.var = "lri_ratio")
    lri_scalar_a1 <- cbind(cause_id = 322, lri_scalar_a1)
    setnames(lri_scalar_a1, paste0(0:999), paste0("ratio_draw_", 0:999), skip_absent = T)
    
  })
  lri_scalar_a1 <- rbindlist(lri_list)
  write.csv(lri_scalar_a1, paste0(out_dir, date, "_lri_scalar_a1.csv"), row.names = F)
  rm(lri_scalar_a1); rm(lri_list)
  gc()
  
} else if (cause == "pertussis"){
  
  ## translate the flu-specific all-age, both-sex scalars to age-specific, sex-specific flu scalars to be used for pertussis
  scalar_a1_long <- melt(scalar_a1, id.vars = c("location_id", "year_id"), measure.vars = paste0("ratio_draw_", 0:999), 
                         variable.name = "draw", value.name = "ratio")
  scalar_a1_long$draw <- as.numeric(substr(scalar_a1_long$draw, 12, nchar(as.character(scalar_a1_long$draw))))
  
  # copy to all sexes and age groups
  full_locs <- data.table(expand.grid(location_id = unique(scalar_a1_long$location_id),
                                      age_group_id = age_meta$age_group_id,
                                      year_id = 2020:2022,
                                      sex_id = 1:2,
                                      draw = 0:999))
  pertussis_scalar_a1_long <- merge(scalar_a1_long, full_locs, by = c("location_id", "draw", "year_id"), allow.cartesian = TRUE)
  
  # pertussis_list <- lapply(2020:2022, function(yr){
  #   
  #   gbd_flu_rsv_cases <- get_flu_rsv_cases(year = yr, age_meta = age_meta)
  #   # scalar_a1_long is not age or sex-specific so don't merge on those
  #   pertussis_scalar_long <- merge(scalar_a1_long, gbd_flu_rsv_cases, by = c("location_id", "year_id", "draw"))
  #   
  #   # multiply by annual ratio to get flu and rsv adjusted counts
  #   pertussis_scalar_long[, flu_count_adj := flu_count*ratio]
  #   
  #   pertussis_scalar_long[, pertussis_ratio := flu_count_adj/flu_count]
  #   
  #   # Take care of case where adjusted and original count both = 0
  #   pertussis_scalar_long[flu_count_adj == 0 & flu_count == 0, flu_ratio := 1]
  #   
  #   # add missing age groups
  #   missing_ages_draws <- expand.grid(age_group_id=setdiff(age_meta$age_group_id, pertussis_scalar_long$age_group_id), 
  #                                     location_id = unique(pertussis_scalar_long$location_id),
  #                                     sex_id = 1:2, year_id = unique(pertussis_scalar_long$year_id), 
  #                                     draw = unique(pertussis_scalar_long$draw), pertussis_rate = 0)
  #   pertussis_scalar_long <- rbind(pertussis_scalar_long, missing_ages_draws)
  # 
  #   # cast from long to wide
  #   pertussis_scalar_a1 <- data.table::dcast(pertussis_scalar_long, location_id + year_id + sex_id + age_group_id ~ draw, 
  #                                      value.var = "pertussis_ratio")
  #   pertussis_scalar_a1 <- cbind(cause_id = 322, pertussis_scalar_a1)
  #   setnames(pertussis_scalar_a1, paste0(0:999), paste0("ratio_draw_", 0:999), skip_absent = T)
  #   
  # })
  # lri_scalar_a1 <- rbindlist(lri_list)
  
  # save flu-specific scalars to apply to pertussis
  pert_scalar_a1 <- dcast(pertussis_scalar_a1_long, ...~draw, value.var = "ratio")
  setnames(pert_scalar_a1, paste0(0:999), paste0("ratio_draw_", 0:999))
  pert_scalar_a1 <- cbind(cause_id = 339, pert_scalar_a1)
  write.csv(pert_scalar_a1, paste0(out_dir, date, "_pertussis_scalar_a1.csv"), row.names = F)
  
}


######### APPROACH 3 #######
if (approach == 3 | approach == "all"){
  if(cause == "measles"){
    
    custom_nf_results <- fread(measles_expected_path)
    drawnames <- names(custom_nf_results)[names(custom_nf_results) %like% "draw_"]
    
    gbd_prev_yr_avg <- custom_nf_results[year_id %in% reference_pd_start:2019][, lapply(.SD, mean), by = "location_id", .SDcols = drawnames]
    setnames(gbd_prev_yr_avg, drawnames, paste0("prevyrs_draw_", 0:999))
    
    # calculate underreporting ratio - will only be calculated for countries that we are using RegMod estimates from (ie no more than 6 mos missing data)
    urr <- calculate_urr(true_cases = gbd_prev_yr_avg, reported_cases = prev_yr_avg, cause = cause, drawwise = urr_drawwise)
    # use under-reporting ratio and GBD estimates to calculate the number of reported cases we would have expected in covid-affected years
    custom_nf_long <- melt(custom_nf_results[year_id %in% year_start:year_end], id.vars = c("location_id", "year_id"), 
                           value.name = "gbd_cases", variable.name = "draw")
    custom_nf_long$draw <- as.numeric(substr(custom_nf_long$draw, 11, nchar(as.character(custom_nf_long$draw))))
    
    if(subset_2020){
      # split gbd case est for 2020 by month using seasonality pattern generated separately from RegMod, then cast long
      seas_dt <- as.data.table(read.xlsx(seas_path))
      monthly_gbd <- get_monthly(data = custom_nf_long[year_id == 2020], pred_seas = seas_dt, format = "long") # this should be 2448000 rows long becauase it should have 1000 rows for each month, location combo in 2020 (204*12)
      #use URR
      if ("draw" %in% colnames(urr)) nocovid_reporting_monthly <- merge(urr, monthly_gbd, by = c("location_id", "draw")) else nocovid_reporting_monthly <- merge(urr, monthly_gbd, by = "location_id", allow.cartesian = TRUE)
      
      #use monthly regmod results to get monthly ratios
      nocovid_reporting_monthly[, est_report := gbd_cases * urr]
      
      monthly_scalar_a3 <- merge(nocovid_reporting_monthly, regmod_long_month, by = c("location_id", "year_id", "month", "draw")) 
      monthly_scalar_a3[, meas_ratio := regmod_cases/est_report]
      
      write.csv(monthly_scalar_a3, paste0(out_dir, date, "_", cause, "_temp_monthly_scalar_a3.csv"), row.names = F)
      
      # fix infintie ratios at month level
      monthly_scalar_a3[is.infinite(meas_ratio) & (urr == 0 | gbd_cases == 0), meas_ratio := 0]
      
      #standardize rest of month ratios to jan feb, if toggled on
      if(jan_mar_std){
        jan_mar <- monthly_scalar_a3[month %in% 1:3, .(jan_mar = mean(meas_ratio)), by = "location_id"]
        monthly_scalar_a3 <- merge(monthly_scalar_a3, jan_mar, by = "location_id")
        monthly_scalar_a3[, meas_ratio := meas_ratio/jan_mar]
      }
      
      #make dt of ratio of 1 for jan, feb, mar in all locs (key diff with the monthly method vs annual is we are assuming jan-mar 2020 measles unaffected by covid)
      monthly_scalar_a3_apr_dec <- monthly_scalar_a3[!(month %in% c(1,2,3))]
      early_2020 <- expand.grid(location_id = unique(monthly_scalar_a3$location_id), 
                                year_id = 2020,
                                month = 1:3,
                                meas_ratio = 1,
                                draw = 0:999)
      early_2020 <- merge(early_2020, seas_dt, by = c("location_id", "month"))
      monthly_scalar_a3 <- rbind(monthly_scalar_a3_apr_dec, early_2020, fill = TRUE)
      
      #annualize ratios using monthly cases as weights (aka using seasonality values as weights)
      scalar_a3_2020 <- monthly_scalar_a3[,.(meas_ratio = weighted.mean(meas_ratio, w = wt)), .(location_id, year_id, cause, draw)]
      # subset to just later years before proceeding with annual scalar cacluation (later rbind on 2020)
      custom_nf_long <- custom_nf_long[year_id > 2020]
      
    }
    
    ##### Calculate ratio annually for years not being calculated monthly in if statement above
    # merge on URR, if didn't calculate it drawwise, merge on same URR value to all case draws for a location
    if ("draw" %in% colnames(urr)) nocovid_reporting <- merge(urr, custom_nf_long, by = c("location_id", "draw")) else nocovid_reporting <- merge(urr, custom_nf_long, by = "location_id", allow.cartesian = TRUE)
    
    # estimate how many cases would have been reported in the absence of COVID
    nocovid_reporting[, est_report := gbd_cases * urr]
    
    scalar_a3 <- merge(nocovid_reporting, regmod_long, by = c("location_id", "year_id", "draw")) 
    scalar_a3[, meas_ratio := regmod_cases/est_report]
    
    #bind on 2020 scalar if using month-specific method for 2020 instead of annual method
    if(subset_2020) scalar_a3 <- rbind(scalar_a3, scalar_a3_2020, fill = TRUE)
    
    # fix inifintes that arise when urr or gbd_cases are 0 which make est_report 0 causing division by 0
    scalar_a3[is.infinite(meas_ratio) & (urr == 0 | gbd_cases == 0), meas_ratio := 0]
    if(nrow(scalar_a3[is.infinite(meas_ratio)]) > 0) message("STOP you have infinite values for meas_ratio in rows where neither of URR or GBD cases are 0, check where it's coming from!")
    
    # cast from long to wide
    scalar_a3 <- data.table::dcast(scalar_a3, location_id + year_id ~ draw, 
                                   value.var = "meas_ratio")
    setnames(scalar_a3, paste0(0:999), paste0("ratio_draw_", 0:999), skip_absent = T)
    
    # only keep 2020 and 2021 for measles because 2022 was unreasonable
    scalar_a3 <- scalar_a3[year_id %in% 2020:2021]
    
    # Fill missing countries with average draws of countries in the region or super region if none in region
    scalar_a3 <- fill_regional_median(dt = scalar_a3,
                                      countries = countries,
                                      draw_prefix = "ratio_draw_")
    scalar_a3 <- fill_regional_median(dt = scalar_a3,
                                      countries = countries,
                                      draw_prefix = "ratio_draw_",
                                      id_cols = c("super_region_id", "year_id"))
    
    # Duplicate annual ratios for the subnationals
    if(subnational){
      scalar_a3 <- expand_subnational(scalar_a3, hierarchy, keep_national_parents = TRUE)
    }
    
    scalar_a3 <- cbind(cause_id = 341, scalar_a3)
    write.csv(scalar_a3, paste0(out_dir, date, "_", cause, "_all_locs_scalar_a3.csv"), row.names = F)
    
  } else if (cause == "flu"){
    
    #just use 2019 cases as true cases instead of the avg of 2017-2019 that was used for measles, don't have burdenator for prev 3 years
    flu_rsv_cases_2019 <- get_flu_rsv_cases(year = 2019, age_meta = age_meta, fatal_nonfatal = fatal_nonfatal_toggle)
    
    urr <- calculate_urr(true_cases = flu_rsv_cases_2019, reported_cases = prev_yr_avg, cause = cause, drawwise = urr_drawwise)
    
    #get age- and sex- specific LRI scalars
    lri_list <- lapply(2020:2022, function(yr){
      
      #pull COVID-free GBD estimates and aggregate to national level, all-age so that matches RegMod granularity
      # merge on URR, if didn't calculate URR drawwise, merge on same URR value to all case draws for a location
      gbd_flu_rsv_cases <- get_flu_rsv_cases(year = yr, age_meta = age_meta, fatal_nonfatal = fatal_nonfatal_toggle)
      gbd_flu_cases_aa <- copy(gbd_flu_rsv_cases)[, flu_count := sum(flu_count), by = c("location_id", "year_id", "draw")] %>% unique(., by = c("location_id", "year_id", "draw"))
      
      id_cols <- c("year_id", "draw", "sex_id", "age_group_id", "measure_id", "metric_id") # no loc bc locs are aggregated to country
      agg_cols <- c("flu_count", "rsv_count")
      gbd_flu_cases_aa <- aggregate_to_national(gbd_flu_cases_aa, id_cols = id_cols, agg_cols = agg_cols)
      
      if("draw" %in% colnames(urr)) nocovid_reporting <- merge(urr, gbd_flu_cases_aa, by = c("location_id", "draw")) else nocovid_reporting <- merge(urr, gbd_flu_cases_aa, by = "location_id", allow.cartesian = TRUE)
      
      # estimate number of reported cases in the absence of COVID from COVID-free GBD est and URR
      nocovid_reporting[, covid_free_reporting := flu_count*urr]
      
      # merge on RegMod to calculate all-age, both-sex ratio of regmod cases to no-covid reported cases
      flu_ratio <- merge(nocovid_reporting, regmod_long, by = c("location_id", "year_id", "draw"))
      flu_ratio[, flu_ratio := regmod_cases/covid_free_reporting]
      
      
      # Fill missing countries with average draws of countries in the region or super region if none in region
      flu_ratio_wide <- dcast(flu_ratio, location_id + year_id~ draw, value.var = "flu_ratio")
      setnames(flu_ratio_wide, paste0(0:999), paste0("flu_ratio_draw_", 0:999), skip_absent = T)
      flu_ratio_wide <- fill_regional_median(dt = flu_ratio_wide,
                                             countries = countries,
                                             draw_prefix = "flu_ratio_draw_")
      
      flu_ratio_wide <- fill_regional_median(dt = flu_ratio_wide,
                                             countries = countries,
                                             draw_prefix = "flu_ratio_draw_",
                                             id_cols = c("super_region_id", "year_id"))
      flu_ratio <- melt(flu_ratio_wide, id.vars = c("location_id", "year_id"), value.name = "flu_ratio", variable.name = "draw")
      flu_ratio$draw <- as.numeric(substr(flu_ratio$draw, 16, nchar(as.character(flu_ratio$draw))))
      
      # Duplicate annual ratios for the subnationals
      if(subnational){
        flu_ratio <- expand_subnational(flu_ratio, hierarchy, keep_national_parents = FALSE)
      }
      
      # translate from all-age both sex to age- and sex-specific LRI scalars
      # merge flu_ratio onto age- and sex-specific flu and LRI case estimates
      # each row of flu_ratio will match to 50 of gbd_flu_rsv_cases (25 age groups * 2 sexes for that location-year-draw)
      scalar_a3 <- merge(gbd_flu_rsv_cases, flu_ratio, by = c("location_id", "year_id", "draw"), allow.cartesian = TRUE)
      
      # multiply by annual ratio to get flu and rsv adjusted counts
      scalar_a3[, `:=` (flu_count_adj = flu_count*flu_ratio, rsv_count_adj = rsv_count*flu_ratio)]
      
      
      # how many LRI deaths to remove in count space
      scalar_a3[, lri_remove := (flu_count+rsv_count-(flu_count_adj+rsv_count_adj))]
      scalar_a3[, lri_adjusted := lri_count - lri_remove]
      
      scalar_a3[, lri_ratio := lri_adjusted/lri_count]
      
      
      # Take care of case where adjusted and original count both = 0
      scalar_a3[lri_adjusted == 0 & lri_count == 0, lri_ratio := 1]
      # TEMP - CAP at 90% disruption
      # ISSUE - this still does not reflect if flu_count + rsv_count > lri_count, need to fix
      scalar_a3[lri_ratio < 0.1, lri_ratio := 0.1]
      # cast from long to wide
      scalar_a3 <- data.table::dcast(scalar_a3, location_id + year_id + sex_id + age_group_id ~ draw, 
                                     value.var = "lri_ratio")
      scalar_a3 <- cbind(cause_id = 322, scalar_a3)
      setnames(scalar_a3, paste0(0:999), paste0("ratio_draw_", 0:999), skip_absent = T)
    })
    lri_scalar_a3 <- rbindlist(lri_list)
    
    #replace ratios greater than 1 with 1
    draws <- lri_scalar_a3[,paste0("ratio_draw_", 0:999)]
    draws[draws > 1] <- 1
    lri_scalar_a3 <- lri_scalar_a3[,-paste0("ratio_draw_",0:999)]
    lri_scalar_a3 <- cbind(lri_scalar_a3, draws)
    
    write.csv(lri_scalar_a3, paste0(out_dir, date, "_lri_scalar_a3.csv"), row.names = F)
    rm(lri_scalar_a3); rm(lri_list)
    gc()
    
    #get age- and sex- specific flu scalars
    flu_list <- lapply(2020:2022, function(yr){
      
      #pull COVID-free GBD estimates and aggregate to national level, all-age so that matches RegMod granularity
      # merge on URR, if didn't calculate URR drawwise, merge on same URR value to all case draws for a location
      gbd_flu_rsv_cases <- get_flu_rsv_cases(year = yr, age_meta = age_meta, fatal_nonfatal = fatal_nonfatal_toggle)
      gbd_flu_cases_aa <- copy(gbd_flu_rsv_cases)[, flu_count := sum(flu_count), by = c("location_id", "year_id", "draw")] %>% unique(., by = c("location_id", "year_id", "draw"))
      
      id_cols <- c("year_id", "draw", "sex_id", "age_group_id", "measure_id", "metric_id") # no loc bc locs are aggregated to country
      agg_cols <- c("flu_count", "rsv_count")
      gbd_flu_cases_aa <- aggregate_to_national(gbd_flu_cases_aa, id_cols = id_cols, agg_cols = agg_cols)
      
      if("draw" %in% colnames(urr)) nocovid_reporting <- merge(urr, gbd_flu_cases_aa, by = c("location_id", "draw")) else nocovid_reporting <- merge(urr, gbd_flu_cases_aa, by = "location_id", allow.cartesian = TRUE)
      
      # estimate number of reported cases in the absence of COVID from COVID-free GBD est and URR
      nocovid_reporting[, covid_free_reporting := flu_count*urr]
      
      # merge on RegMod to calculate all-age, both-sex ratio of regmod cases to no-covid reported cases
      flu_ratio <- merge(nocovid_reporting, regmod_long, by = c("location_id", "year_id", "draw"))
      flu_ratio[, flu_ratio := regmod_cases/covid_free_reporting]
      
      
      # Fill missing countries with average draws of countries in the region or super region if none in region
      flu_ratio_wide <- dcast(flu_ratio, location_id + year_id~ draw, value.var = "flu_ratio")
      setnames(flu_ratio_wide, paste0(0:999), paste0("flu_ratio_draw_", 0:999), skip_absent = T)
      flu_ratio_wide <- fill_regional_median(dt = flu_ratio_wide,
                                             countries = countries,
                                             draw_prefix = "flu_ratio_draw_")
      
      flu_ratio_wide <- fill_regional_median(dt = flu_ratio_wide,
                                             countries = countries,
                                             draw_prefix = "flu_ratio_draw_",
                                             id_cols = c("super_region_id", "year_id"))
      flu_ratio <- melt(flu_ratio_wide, id.vars = c("location_id", "year_id"), value.name = "flu_ratio", variable.name = "draw")
      flu_ratio$draw <- as.numeric(substr(flu_ratio$draw, 16, nchar(as.character(flu_ratio$draw))))
      
      # Duplicate annual ratios for the subnationals
      if(subnational){
        flu_ratio <- expand_subnational(flu_ratio, hierarchy, keep_national_parents = FALSE)
      }
      
      # translate from all-age both sex to age- and sex-specific LRI scalars
      # merge flu_ratio onto age- and sex-specific flu and LRI case estimates
      # each row of flu_ratio will match to 50 of gbd_flu_rsv_cases (25 age groups * 2 sexes for that location-year-draw)
      scalar_a3 <- merge(gbd_flu_rsv_cases, flu_ratio, by = c("location_id", "year_id", "draw"), allow.cartesian = TRUE)
      
      # multiply by annual ratio to get flu and rsv adjusted counts
      scalar_a3[, `:=` (flu_count_adj = flu_count*flu_ratio, rsv_count_adj = rsv_count*flu_ratio)]
      
      # how many LRI deaths to remove in count space
      scalar_a3[, lri_remove := (flu_count+rsv_count-(flu_count_adj+rsv_count_adj))]
      scalar_a3[, lri_adjusted := lri_count - lri_remove]
      
      scalar_a3[, lri_ratio := lri_adjusted/lri_count]
      
      
      # Take care of case where adjusted and original count both = 0
      scalar_a3[lri_adjusted == 0 & lri_count == 0, lri_ratio := 1]
      # TEMP - CAP at 90% disruption
      # ISSUE - this still does not reflect if flu_count + rsv_count > lri_count, need to fix
      scalar_a3[lri_ratio < 0.1, lri_ratio := 0.1]
      # cast from long to wide
      scalar_a3 <- data.table::dcast(scalar_a3, location_id + year_id + sex_id + age_group_id ~ draw, 
                                     value.var = "flu_ratio")
      scalar_a3 <- cbind(cause_id = 322, scalar_a3)
      setnames(scalar_a3, paste0(0:999), paste0("ratio_draw_", 0:999), skip_absent = T)
    })
    flu_scalar_a3 <- rbindlist(flu_list)
    
    #replace ratios greater than 1 with 1
    draws <- flu_scalar_a3[,paste0("ratio_draw_", 0:999)]
    draws[draws > 1] <- 1
    flu_scalar_a3 <- flu_scalar_a3[,-paste0("ratio_draw_",0:999)]
    flu_scalar_a3 <- cbind(flu_scalar_a3, draws)
    
    write.csv(flu_scalar_a3, paste0(out_dir, date, "_flu_scalar_a3.csv"), row.names = F)
    rm(flu_scalar_a3); rm(flu_list)
    gc()
    
    
  } else if (cause == "pertussis"){
    
    #just use 2019 cases as true cases instead of the avg of 2017-2019 that was used for measles, don't have burdenator for prev 3 years
    flu_rsv_cases_2019 <- get_flu_rsv_cases(year = 2019, age_meta = age_meta, fatal_nonfatal = "nonfatal") 
    
    urr <- calculate_urr(true_cases = flu_rsv_cases_2019, reported_cases = prev_yr_avg, cause = cause, drawwise = urr_drawwise)
    
    #get age- and sex- specific LRI scalars
    pertussis_list <- lapply(2020:2022, function(yr){
      
      #pull COVID-free GBD estimates and aggregate to national level, all-age so that matches RegMod granularity
      # merge on URR, if didn't calculate URR drawwise, merge on same URR value to all case draws for a location
      gbd_flu_rsv_cases <- get_flu_rsv_cases(year = yr, age_meta = age_meta, fatal_nonfatal = "nonfatal")
      gbd_flu_cases_aa <- copy(gbd_flu_rsv_cases)[, flu_count := sum(flu_count), by = c("location_id", "year_id", "draw")] %>% unique(., by = c("location_id", "year_id", "draw"))
      
      # aggregate to national to match geographies we have RegMod results for
      id_cols <- c("year_id", "draw", "sex_id", "age_group_id", "measure_id", "metric_id") # no loc bc locs are aggregated to country
      agg_cols <- c("flu_count", "rsv_count")
      gbd_flu_cases_aa <- aggregate_to_national(gbd_flu_cases_aa, id_cols = id_cols, agg_cols = agg_cols)
      
      if("draw" %in% colnames(urr)) nocovid_reporting <- merge(urr, gbd_flu_cases_aa, by = c("location_id", "draw")) else nocovid_reporting <- merge(urr, gbd_flu_cases_aa, by = "location_id", allow.cartesian = TRUE)
      
      # estimate number of reported cases in the absence of COVID from COVID-free GBD est and URR
      nocovid_reporting[, covid_free_reporting := flu_count*urr]
      
      # merge on RegMod to calculate all-age, both-sex ratio of regmod cases to no-covid reported cases
      flu_ratio <- merge(nocovid_reporting, regmod_long, by = c("location_id", "year_id", "draw"))
      flu_ratio[, flu_ratio := regmod_cases/covid_free_reporting]
      
      # Fill missing countries with average draws of countries in the region or super region if none in region
      flu_ratio_wide <- dcast(flu_ratio, location_id + year_id~ draw, value.var = "flu_ratio")
      setnames(flu_ratio_wide, paste0(0:999), paste0("flu_ratio_draw_", 0:999), skip_absent = T)
      flu_ratio_wide <- fill_regional_median(dt = flu_ratio_wide,
                                             countries = countries,
                                             draw_prefix = "flu_ratio_draw_")
      flu_ratio_wide <- fill_regional_median(dt = flu_ratio_wide,
                                             countries = countries,
                                             draw_prefix = "flu_ratio_draw_",
                                             id_cols = c("super_region_id", "year_id"))
      flu_ratio <- melt(flu_ratio_wide, id.vars = c("location_id", "year_id"), value.name = "flu_ratio", variable.name = "draw")
      flu_ratio$draw <- as.numeric(substr(flu_ratio$draw, 16, nchar(as.character(flu_ratio$draw))))
      
      # Duplicate annual ratios for the subnationals
      if(subnational){
        flu_ratio <- expand_subnational(flu_ratio, hierarchy, keep_national_parents = FALSE)
      }
      
      scalar_a3_long <- copy(flu_ratio)
      
      # copy flu scalars to all sexes and age groups for use in pertussis model
      full_locs <- data.table(expand.grid(location_id = unique(scalar_a3_long$location_id),
                                          age_group_id = age_meta$age_group_id,
                                          year_id = 2020:2022,
                                          sex_id = 1:2,
                                          draw = 0:999))
      scalar_a3_long <- merge(scalar_a3_long, full_locs, by = c("location_id", "year_id", "draw"))
      scalar_a3 <- dcast(scalar_a3_long, location_id + year_id + sex_id + age_group_id ~ draw, 
                         value.var = "flu_ratio")
      setnames(scalar_a3, paste0(0:999), paste0("ratio_draw_", 0:999))
      scalar_a3 <- cbind(cause_id = 339, scalar_a3)
    })
    pertussis_scalar_a3 <- rbindlist(pertussis_list)
    
    write.csv(pertussis_scalar_a3, paste0(out_dir, date, "_pertussis_scalar_a3.csv"), row.names = F)
    rm(pertussis_scalar_a3); rm(pertussis_list)
    gc()
    
    
  }
}

##### MEASLES SPECIFIC SECTION ################
## measles is complicated because of locations where we trust case notifications and use those directly
# Get cases from the scalars, pulling directly from reports for trusted locations
# For all locations: get expected case counts from GBD flat file results and ratio
# these counts will need to be run through the measles pipeline with covid_adjustment == TRUE to get the fatal scalars to hand off (via the implied CFR appraoch)
if(cause == "measles"){
  if(approach == 1){
    my_ratio_dt = scalar_a1
  } else if (approach == 2){
    my_ratio_dt = scalar_a2
  } else if (approach == 3){
    my_ratio_dt = scalar_a3
  }
  
  # calculate adjusted measles cases using scalars
  adjusted <- get_adjusted_measles(expected_dir = measles_expected_path, 
                                   ratio_dt = my_ratio_dt,
                                   years = 2020:2021, 
                                   time = "annual")
  
  # For trusted locations: get trusted case counts
  trusted_path <-  "FILEPATH"
  
  # This function will also fill NAs in sporadic/partial years with 0, based on decision made with Jon because these NAs were all 0s in previous data downloads from WHO
  trusted_notifications <- get_trusted_measles(years = 2020:2021,
                                               gbd_round_id = 7,
                                               decomp_step = "iterative",
                                               time = "annual",
                                               se = T,
                                               draws = T,
                                               measles_path = trusted_path,
                                               fill_partial_year_zero = T)
  
  # Drop the location-year combinations that are in trusted_notifications
  adjusted[, matchvar := paste(location_id, year_id, sep = "_")]
  trusted_notifications[, matchvar := paste(location_id, year_id, sep = "_")]
  
  adjusted <- adjusted[!matchvar %in% unique(trusted_notifications$matchvar)] # removed 142 location-years
  counts <- rbind(adjusted, trusted_notifications, fill = T)
  counts[, cause_id := 341]
  
  fwrite(counts, paste0(out_dir, date, "_", cause, "_counts_w_trusted_a", as.character(approach), ".csv"))
}

