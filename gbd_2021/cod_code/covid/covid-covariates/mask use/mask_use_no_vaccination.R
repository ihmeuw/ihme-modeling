#########################################################################
### Smooth and project mask use
### Uses Premise, Facebook (global) data to make estimates.
### Smooths over those data using the "Barber Smooth" which is 
### essentially a neighbor-averaging spline. Also smooths the
### tails, which is nice, and projects flat estimates backwards
### at 0 and forwards at approximately the last reported level.
### Written by Austin Carter, edited by Chris Troeger 

## Instructions:
## Run this script!
## It will create its own output folder
## We are no longer updating PREMISE or YouGov, so this will just
## take existing data from those sources

#########################################################################
# source("/ihme/code/covid-19/user/ctroeger/covid-beta-inputs/src/covid_beta_inputs/mask_use/mask_use.R")
rm(list=ls())

##------------------------------------------------------------------------    
## Arguments
  mask_version <- "" # If blank, the script will generate a new folder
  update_premise <- F
  save_counties <- T # Should county-specific estimates be saved?
  long_range_scenario <- T # Should long-range scenarios be made?
  lsvid <- 746
  use_yougov <- TRUE
  all_smooth <- FALSE # should all locations be smoothed at the "more flexible" level?
  include_new_fb_us <- TRUE # New Facebook data in US, PREMISE adjusted accordingly if used (crosswalked)
  use_weights <- TRUE # Should Facebook data be weighted?
  rake_usa <- FALSE # Should Premise data be raked to YouGov data?
  reduction_mask_vaccine <- 0.25 # What fraction of vaccinated people will stop wearing masks? (We should look at this empirically!)
  smooth_neighbors <- 5
  smooth_iterations <- 10
  mark_best <- F
  vaccine_model <- "best" # Which version of vaccine to use (probably best)
  date_range <- seq(as.Date("2020-01-01"), as.Date("2023-12-31"), by = "1 day") # What is the range of covariate projections?
  #date_range <- seq(as.Date("2020-01-01"), as.Date("2024-02-01"), by = "1 day") # What is the range of covariate projections?
## Look for the section on manual outliers. It would be better to have up here but I don't know how to do that!

  more_flexible_loc_names <- c("Scotland",
                          "Victoria","Belgium","Croatia","England","Greece","Ireland","Northern Ireland",
                          "Serbia","United Kingdom","Wales","Nova Scotia",
                          "New Zealand","Alberta","Manitoba","Jordan", # Added 8/24 
                          "Czechia", "Slovakia","Romania","Bulgaria", "Lao People's Democratic Republic",
                          "Tunisia",
                          "Poland","Slovakia","Russian Federation","Ukraine","Finland","Switzerland",
                          "Netherlands","Sri Lanka","New Brunswick",
                          "Ghana","Viet Nam",
                          "Hungary",
                          "Israel")
  
  
##--------------------------------------------------------------------
  ## Setup
  Sys.umask("002")
  library(data.table)
  library(ggplot2)
  library(ggrepel)
  library(boot)
  library(gridExtra)
  library(scales)
  invisible(loadNamespace("ihme.covid", lib.loc = "FILEPATH"))
  
## Functions
  user <- Sys.info()[["user"]]
  code_dir <- file.path("FILEPATH", user, "FILEPATH")
  source(file.path("FILEPATH/get_location_metadata.R"))
  source(file.path("FILEPATH/get_population.R"))
  source(file.path(code_dir, "FILEPATH/get_first_case_date.R"))
  source(paste0(code_dir, "FILEPATH/vaccine_behavior_change_function.R"))  
  source(paste0(code_dir, "FILEPATH/split_gbd_locs.R"))
  
## Specify location with fewer iterations (will follow data more closely)
  hierarchy <- get_location_metadata(location_set_id = 115, location_set_version_id = lsvid, release_id = 9)
  more_flexible_locs <- hierarchy[location_name %in% more_flexible_loc_names, location_id]
  # Add Italian, Brazilian, US subnationals
  more_flexible_locs <- c(more_flexible_locs, hierarchy[parent_id==86, location_id],
                          hierarchy[parent_id == 135, location_id],
                          hierarchy[parent_id == 102, location_id])
  
## Testing adding locations > 1 million people as more flexible
  #population <- fread("/ihme/covid-19/model-inputs/best/age_pop.csv")
  population <- fread("FILEPATH/all_populations.csv")
  population <- population[age_group_id == 22 & sex_id == 3]
  population <- population[, lapply(.SD, function(x) sum(x)), by="location_id", .SDcols="population"] # create single all ages
  
  more_flexible_locs <- c(more_flexible_locs, population[population > 1000000, location_id])
  more_flexible_locs <- unique(more_flexible_locs)

## Paths
MODEL_INPUTS_ROOT <- "FILEPATH"
OUTPUT_ROOT <- "FILEPATH"

if (interactive()) {
  model_inputs_version <- "best"
  input_dir <- file.path(MODEL_INPUTS_ROOT, model_inputs_version)
  if(update_premise == T){
    message("You have specified update_premise = T... you must run PREMISE_PROCESSING_CODE.R!")
    stop()
  } else if(mask_version == ""){
    output_dir <- ihme.covid::get_output_dir(
      root = "FILEPATH",
      date = "today")
  } else {
    output_dir <- paste0("FILEPATH", mask_version) 
  }
  message(paste0("The output directory is ", output_dir))
  metadata_path <- file.path(output_dir, "metadata.yaml")
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument("--lsvid", type = "integer", required = TRUE, help = "location set version id to use")
  parser$add_argument("--outputs-directory", default = ihme.covid::get_latest_output_dir(OUTPUT_ROOT), help = "Release name e.g., /ihme/covid-19/testing-outputs/2020_05_08.11")
  parser$add_argument("--model-inputs-version", default = "best", help = "Version of model-inputs. Defaults to 'best'. Pass a full YYYY_MM_DD.VV or 'latest' if you provide")
  parser$add_argument("--inputs-directory", default = NULL, help = "model-inputs directory to use. Overrides --model-inputs-version")
  args <- parser$parse_args()
  lsvid <- args$lsvid
  if (is.null(args$inputs_directory)) {
    input_dir <- file.path(MODEL_INPUTS_ROOT, args$model_inputs_version)
  } else {
    input_dir <- args$inputs_directory
  }
  output_dir <- args$outputs_directory
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  code_dir <- file.path(ihme.covid::get_script_dir(), "../")
}

## The spatstat package messes up the code
#detach("package:spatstat", unload = T)

## Set up a couple output paths
  out_data_path <- file.path(output_dir, "mask_use.csv")
  out_plot_path <- file.path(output_dir, "mask_use_plots.pdf")

## Tables (merge together a covid and gbd hierarchy to get needed information for aggregation)
  hier_supp <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
  hierarchy[, c("super_region_id", "super_region_name", "region_id", "region_name") := NULL]
  hier_supp[, merge_id := location_id]
  hierarchy[location_id %in% hier_supp$location_id, merge_id := location_id]
  hierarchy[is.na(merge_id), merge_id := parent_id]
  hier_supp <- unique(hier_supp[, .(merge_id, super_region_id, super_region_name, region_id, region_name)])
  hier_full <- merge(hierarchy, hier_supp, by = c("merge_id"), all.x = T)

#########################################################################
# Read data
  source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/import_mask_use_data.R"))

## I've tried to leave most of the data manipulation in this script so it is more easily modifiable.
# dataframes created in that script are:
# dt = Premise data (US only)
# yougov = YouGov data (about 30 countries)
# global = global facebook survey responses
# fb_us = US facebook survey responses
# prev_fb = Facebook survey responses before a major survey revision
    
# You don't want this active before pulling hierarchies or it crashes
  #library(ihme.covid)

##----------------------------------------------------------------
# Do some data manipulations before smoothing
  
## TODO: Review this -------------------------------------
# Need to capture single response fields
  g_all_responses <- global[variable == "C5" & value %in% 1:6]
  g_all_responses[, rows := .N, by = c("location_id","date")]
  
  g_scaffold <- g_all_responses[rows < 5]
  g_scaffold[, trick_find_missing := max(prop_nm[value == 1]), by = c("location_id","date")]
  
  fill_zeros <- g_scaffold[trick_find_missing == "-Inf"]
  fill_zeros <- unique(fill_zeros[,c("location_id","location_name","date","denom_nm","obs_nm")])
  fill_zeros[, prop_nm := 0]
  fill_zeros[,variable := "C5"]
  fill_zeros[,value := 1]
  
  global <- rbind(global, fill_zeros, fill = T)
  
##--------------------------------------------------------
# Capture people who didn't leave their homes in the last week  
  g_no_public <- global[variable == "C5" & value == 6]
    setnames(g_no_public, "prop_nm", "prop_mask_7days_no_public")
  global <- global[variable=="C5" & value==1]
  setnames(global, c("prop_nm","obs_nm"), c("prop_mask_7days_all", "unweighted_N"))
  
  global <- merge(global, g_no_public[,c("location_id","date","prop_mask_7days_no_public")],
                    by = c("location_id", "date"), all.x = T)
  
  global <- global[order(location_id, date)]
  
  ## TODO: Is this an okay assumption?
  global[is.na(prop_mask_7days_no_public), prop_mask_7days_no_public := 0]
  
  global[, N := denom_nm]
  global[, mask_7days_all := prop_mask_7days_all * denom_nm]
  global[, mask_7days_no_public := prop_mask_7days_no_public * denom_nm]
  global_dt <- copy(global)

# If using weighted data, replace
  small_sample <- global_dt[unweighted_N < 30 & unweighted_N > 0]
  global_dt <- global_dt[(unweighted_N >= 30)]

##-------------------------------------------------------------------------------
# Aggregate small sample sizes to monthly (per Emm)
  small_sample[, count_all := prop_mask_7days_all * denom_nm]
  small_sample[, count_no_public := prop_mask_7days_no_public * denom_nm]
  small_sample[, month := month(date)]
  small_sample[, year := year(date)]
  small_sample <- small_sample[, lapply(.SD, function(x) sum(x)), by=c("location_id","location_name","month","year"),
                               .SDcols = c("count_all","count_no_public","denom_nm","unweighted_N","N")]
  small_sample[, date := as.Date(paste0(year,"-",month,"-15"))]
  small_sample[, prop_mask_7days_all := count_all / denom_nm]
  small_sample[, prop_mask_7days_no_public := count_no_public / denom_nm]
  small_sample[, mask_7days_no_public := count_no_public]
  setnames(small_sample, "count_all", "mask_7days_all")
  
  small_sample <- small_sample[date > "2020-05-01" & unweighted_N >= 20]
  
  only_small_locs <- setdiff(unique(small_sample$location_id), unique(global_dt$location_id))
  
  global_dt <- rbind(global_dt, small_sample, fill = T)

  loc_date_table <- table(global_dt[, .(location_name, date)])
  drop_locs <- rownames(loc_date_table)[unlist(apply(loc_date_table, 1, function(r) any(r > 1)))]

##--------------------------------------------------------------------------------
# Make region data
  region_dt <- merge(global_dt, hier_full[, .(location_id, region_id, region_name)], by = "location_id", all.x = T)
  #region_dt[is.na(region_id), c("region_id", "region_name") := .(5, "East Asia")]
  region_dt <- region_dt[, lapply(.SD, sum), by = .(region_id, region_name, date), 
                         .SDcols = c("mask_7days_all", "mask_7days_no_public", "N")]
  region_dt <- subset(region_dt, !is.na(region_id))
  setnames(region_dt, c("region_id", "region_name"), c("location_id", "location_name"))

# Make super-region data
  super_region_dt <- merge(global_dt, hier_full[, .(location_id, super_region_id, super_region_name)], by = "location_id", all.x = T)
  super_region_dt[is.na(super_region_id), c("super_region_id", "super_region_name") := .(4, "Southeast Asia, East Asia, and Oceania")]
  super_region_dt <- super_region_dt[, lapply(.SD, sum), by = .(super_region_id, super_region_name, date), 
                                     .SDcols = c("mask_7days_all", "mask_7days_no_public", "N")]
  setnames(super_region_dt, c("super_region_id", "super_region_name"), c("location_id", "location_name"))
  
  global_dt <- rbind(global_dt, region_dt, fill = T)
  global_dt <- rbind(global_dt, super_region_dt, fill = T)

# Recalculate prop_always
  global_dt[, prop_always := mask_7days_all / (N - mask_7days_no_public)]
  global_dt[, source := "Facebook"]

  message(paste0("The max date of data from Facebook was ", max(global_dt$date)))
  subset_global_dt <- global_dt[,.(location_name, location_id, date, prop_always, source, N)]

### drop the last day of data from Facebook ###
  subset_global_dt <- subset(subset_global_dt, date < max(date))

# Bind on global data 
  dt <- rbind(dt, subset_global_dt, fill = T)
  dt <- dt[prop_always < 1]

# Bind on old Facebook
  if(use_weights == F){
    dt <- rbind(dt, prev_fb)
  } 

##-------------------------------------------------------------------
# match premise and US fb
  premise_dt <- dt[source == "Premise"]
# Make a dataframe for crosswalks
  us_dt <- merge(fb_us, premise_dt, by=c("location_id","date"))
  us_dt[, xw_ratio := mask_use / prop_always]
  us_dt[, logit_ratio := logit(mask_use) - logit(prop_always)]
  
  country_crosswalk <- us_dt[, lapply(.SD, function(x) median(x)), by="location_id", .SDcols = c("xw_ratio","logit_ratio")]
  
  premise_dt <- merge(premise_dt, country_crosswalk, by="location_id")
  premise_dt[, prop_always := inv.logit(logit(prop_always) + logit_ratio)]
  premise_dt[, source := "Adjusted Premise"]

# Remove original PREMISE, bind on US FB and Adjusted PREMISE
  dt <- rbind(dt[source != "Premise"], premise_dt, fb_us, fill = T)
  dt[,c("xw_ratio","logit_ratio")] <- NULL

dt <- dt[order(location_id, date)]

#########################################################################
## Manual outliers
  dt <- dt[!(location_id == 558 & date == "2020-11-11")]
  dt <- dt[!(source == "Facebook - US" & date == "2020-11-23")]
  dt <- dt[!(location_id == 43865 & date == "2020-11-15")]
  dt <- dt[!(location_id == 554 & date == "2020-12-08" & source == "Facebook - US")]
  dt <- dt[!(location_id %in% c(43865, 30))]
  # Eritrea (May 3, 2021)
  dt <- dt[!(location_id == 178 & date == "2021-04-15")]
  # Mongolia (May 3, 2021)
  dt <- dt[!(location_id == 38 & date == "2021-04-15")]
  # Bhutan (May 3, 2021)
  dt <- dt[!(location_id == 162 & date == "2021-04-15")]
  # Roraima (May 3, 2021)
  dt <- dt[!(location_id == 4771 & date %in% as.Date(c("2021-04-20","2021-04-26")))]
  
  ## Drop PREMISE data per Steve, CJLM (3/9/2021)
  dt <- dt[!(location_id %in% c(528,570) & date == "2021-03-02")]
  dt <- dt[!(location_name == "Illinois" & date > "2021-01-01" & source == "Adjusted Premise")]
  
  dt[is.na(prop_always), prop_always := mask_use]

# drop some locations from Yougov dataset (7/7/2020)
  yougov <- subset(yougov, !(location_name %in% c("Australia", "Denmark","Sweden","Netherlands","Norway")))
  yougov <- yougov[!(location_id %in% hierarchy[parent_id == 102, location_id])]

# The last date drags up in some locations
  yougov <- subset(yougov, date < max(yougov$date))

# Keep track of where we have yougov locations!
# don't add YouGov today! (6/21/2020)
  if(use_yougov == T){
    dt <- rbind(dt, yougov, fill = T)
  }

## Merge on date of first case. This will be the pinned date for 0% mask use that the model
## will essentially be forced to follow. 
  first_case_date <- get_first_case_date(unique(dt$location_id), file.path(MODEL_INPUTS_ROOT, "best"))
  missing_case_date <- setdiff(unique(dt$location_id), first_case_date$location_id)
  add_case_date <- data.table(location_id = missing_case_date, first_case_date = as.Date("2020-01-01"))
  first_case_date <- rbind(first_case_date, add_case_date)
  
  first_case_date[, loc_rows := 1:.N, by = "location_id"]
  first_case_date <- first_case_date[loc_rows == 1]
  first_case_date[, loc_rows := NULL]

  dt <- merge(dt, first_case_date, by = "location_id", all.x = T)

## Kaiser Family Foundation (kff) data.
  kff_dt <- data.table(
    date = as.Date(c("2020-02-15", "2020-03-14", "2020-04-17")),
    prop_always = c(0.09, 0.12, 0.52), # Only the last data point is from a survey
    source = "kff"
  )

## Intercept-shift those data. This means, essentially, pushing the
## KFF data up or down according to the observed values in each location.
## As KFF is predominantly a US organization,
  # Make state-specific KFF data
  kff_loc_dt <- rbindlist(lapply(unique(dt$location_id), function(loc) {
    #print(loc)
    # Intercept shift in logit space
    loc_dt <- copy(kff_dt)[, location_id := loc]
    loc_dt[, logit_val := log(prop_always / (1 - prop_always))]
    first_premise <- dt[location_id == loc][date == min(date)]$prop_always
    logit_first_premise <- log(first_premise / (1 - first_premise))
    shift <- logit_first_premise - loc_dt[date == max(date)]$logit_val
    loc_dt[, shift_logit_val := logit_val + shift]
    loc_dt[, prop_always := exp(shift_logit_val) / (1 + exp(shift_logit_val))]
    loc_dt[, first_case_date := unique(dt[location_id == loc]$first_case_date)]
    loc_dt[, location_name := unique(dt[location_id == loc]$location_name)]
    loc_dt[, .(location_id, location_name, date, prop_always, first_case_date)]
  }))
  kff_loc_dt$source <- "KFF"

# Remove KFF from locations with YouGov data
  if(use_yougov){
    kff_loc_dt <- subset(kff_loc_dt, !(location_id %in% unique(yougov$location_id)))
  }

dt <- rbind(dt, kff_loc_dt, fill = T)

# Want to 'impute' the other sources of data so that the line data from YouGov
# don't drive the past trend (avoid compositional bias).
  if(use_yougov){
    ## Find average relationship 
    crosswalk <- data.table(merge(yougov, dt[source=="Facebook"], by=c("location_name","date")))
    crosswalk[, ratio := prop_always.x / prop_always.y]
    
    country_crosswalk <- aggregate(ratio ~ location_id.x, data=crosswalk, function(x) median(x))
    setnames(country_crosswalk, "location_id.x", "location_id")
    
    ggplot(crosswalk, aes(x=date, y=ratio, col=location_name, group=location_name)) + geom_line() +
      facet_wrap(~location_name) + ylab("Weekly ratio") + guides(col=F) + theme_minimal() +
      geom_hline(yintercept = 1, lty=2)
    
    # Add imputed facebook data for locations that have YouGov before Facebook starts
    tmp <- dt[source=="Adjusted Website" & date < min(dt[source=="Facebook"]$date)]
    adjusted_fb <- merge(tmp, country_crosswalk, by="location_id")
    adjusted_fb[, prop_always := prop_always * ratio]
    adjusted_fb$source <- "Imputed Facebook"
    dt <- rbind(dt, adjusted_fb, fill=T)
  }

## There are rogue data in Oceania
  dt <- dt[location_id != 21]

## What this does is to set the day with the first case by location as
## having 0 proportion always wear a mask. 
# Subset to data after first case and add on first case date as zero
  dt <- dt[date > first_case_date]
  first_case_dt <- copy(first_case_date)[location_id %in% dt$location_id]
  setnames(first_case_dt, "first_case_date", "date")
  first_case_dt[, prop_always := 1e-5]
  first_case_dt <- merge(unique(dt[, .(location_id, location_name)]), first_case_dt, by = "location_id")

# Remove this 'pin' from countries in SE Asia, East Asia
  first_case_dt <- subset(first_case_dt, !(location_id %in% hier_full[region_name %in% c("East Asia")]$location_id))
  first_case_dt <- subset(first_case_dt, !(location_id %in% hier_full[region_name %in% c("High-income Asia Pacific")]$location_id)) # Singapore, Japan, S Korea, Hong Kong (High-income Asia)

  dt <- rbind(dt, first_case_dt, fill = T)
  dt <- dt[order(location_name, date)]

## Inflate 0s because the smoothing algorithm can't handle them
# Remove most recent data and inflate zeros
  dt[prop_always == 0, prop_always := 1e-5]

##-----------------------------------------------------------------------------
# Save all input data
  write.csv(dt, paste0(output_dir, "/used_data.csv"), row.names=F)
  input_files <- ihme.covid::get.input.files()
  #detach("package:ihme.covid", unload = T)
  
##------------------------------------------------------------------------------------------------------------------
## Major change (8/4): Previously, the level projected into the future was at the level of the last data point.
## Now, we will use the last value of the Barber Smoothed projections. 

# Convert to daily
  full_dt <- rbindlist(lapply(split(dt, by = "location_id"), function(loc_dt) {
    #print(unique(loc_dt$location_id))
    date_trunc <- date_range[date_range <= max(loc_dt$date)] # Only make projections through the last date of data for location
    full_ts <- approx(loc_dt$date, loc_dt$prop_always, xout = date_trunc, rule = 2)$y
    data.table(location_id = unique(loc_dt$location_id), location_name = unique(loc_dt$location_name), date = date_trunc, prop_always = full_ts, N = loc_dt$N)
  }))

### Smooth ###
# We need to make some locations more flexible than others
## Empirical approach to setting locations based on cumulative sample size
  full_dt[, N := ifelse(is.na(N), 0, N)]
  full_dt[, n_total := sum(N, na.rm=T), by=c("location_id")]
  ss_q <- quantile(full_dt$n_total, c(0.25,0.5,0.75))

#-----------------------------------------------------------------------------
## "Expert" approach based on locations with large observed changes
  more_flex <- subset(full_dt, location_id %in% more_flexible_locs)
  more_flex <- more_flex[!(location_id %in% only_small_locs)]
  more_flex[, type := "More flexible (2 iterations & 5 neighbors)"]

# Locations with ONLY small, aggregate sample sizes
  vsmooth_dt <- full_dt[location_id %in% only_small_locs]
  vsmooth_dt[, smooth_prop_always := ihme.covid::barber_smooth(prop_always, n_neighbors = 10, times = 10), by = "location_id"]
  vsmooth_dt[, type := "Sparse data: Smooth (10 iterations & 10 neighbors)"]

  smoother_dt <- subset(full_dt, n_total < ss_q[1] & !(location_id %in% c(only_small_locs, unique(more_flex$location_id))))
#-----------------------------------------------------------------------------

  smoother_dt[, smooth_prop_always := ihme.covid::barber_smooth(prop_always, n_neighbors = 7, times = 10), by = "location_id"]
  smoother_dt[, type := "<25% SS, Smooth (10 iterations & 7 neighbors)"]

  norm_flex <- subset(full_dt, !(location_id %in% more_flexible_locs) & !(location_id %in% c(only_small_locs, unique(smoother_dt$location_id))))
  norm_flex[, type := "Normal flexibility (10 iterations and 5 neighbors)"]

  more_flex[, smooth_prop_always := ihme.covid::barber_smooth(prop_always, n_neighbors = smooth_neighbors, times = 2), by = "location_id"]
  norm_flex[, smooth_prop_always := ihme.covid::barber_smooth(prop_always, n_neighbors = smooth_neighbors, times = smooth_iterations), by = "location_id"]

# Smooth all more flexibly
  all_flex <- copy(full_dt)
  all_flex[, smooth_prop_always := ihme.covid::barber_smooth(prop_always, n_neighbors = smooth_neighbors, times = 2), by = "location_id"]

  if(all_smooth == T){
    full_dt <- copy(all_flex)
  } else {
    full_dt <- rbind(more_flex, norm_flex, smoother_dt, vsmooth_dt)
  }
  
  full_dt[smooth_prop_always <= 1e-5, smooth_prop_always := 0]

# Project forward indefinitely
  complete_ts <- data.table(date = date_range)
  full_ts <- rbindlist(lapply(split(full_dt, by = "location_id"), function(loc_dt) {
    tmp <- merge(complete_ts, loc_dt, by="date", all.x=T)
    tmp$location_id <- unique(loc_dt$location_id)
    tmp$location_name <- unique(loc_dt$location_name)
    tmp[, smooth_prop_always := ifelse(is.na(smooth_prop_always), tail(loc_dt$smooth_prop_always,1), smooth_prop_always)] # Set values into the future at the level of last smoothed value
    return(tmp)
  }))

  full_dt <- copy(full_ts)
  
##-----------------------------------------------------------------------------------------------------------
# Add on observed column
  max_date <- dt[, .(max_date = max(date)), by = location_id]
  full_dt <- merge(full_dt, max_date)
  full_dt[, observed := as.integer(date <= max_date)]
  full_dt[, max_date := NULL]

## Make sure all locations have values
# Use national values for missing subnationals
  missing_children <- setdiff(hierarchy[most_detailed == 1]$location_id, unique(full_dt$location_id))
  parents <- unique(hierarchy[location_id %in% missing_children]$parent_id)
  missing_children_dt <- rbindlist(lapply(parents, function(p) {
    parent_dt <- copy(full_dt[location_id == p])
    # Fill in missing Oceania region with Super Region?
    if(nrow(parent_dt) == 0 & p == 21){
      parent_dt <- copy(full_dt[location_id == 70]) # This is filling Oceania with Australasia values
    }
    all_children <- setdiff(hierarchy[parent_id == p]$location_id, p)
    impute_locs <- intersect(missing_children, all_children)
    rbindlist(lapply(impute_locs, function(i) {
      child_dt <- copy(parent_dt)
      child_dt[, location_id := i]
      child_dt[, location_name := hierarchy[location_id == i]$location_name]
    }))
  }))

full_dt <- rbind(full_dt, missing_children_dt)

## Check for missing locations
  missing_locs <- setdiff(hierarchy[most_detailed == 1]$location_id, unique(full_dt$location_id))
  if (length(missing_locs) > 0) {
    message(paste0("Missing: ", paste(hierarchy[location_id %in% missing_locs]$location_name, collapse = ", ")))
  }

# Impute regional average for missing locations
  message("Imputing missing locations with a regional average")
  regions <- unique(hier_full[location_id %in% missing_locs]$region_id)
  missing_dt <- rbindlist(lapply(regions, function(r) {
    region_locs <- setdiff(hier_full[region_id == r]$location_id, missing_locs)
    region_dt <- full_dt[location_id == r]
    region_missing_children <- hier_full[region_id == r & location_id %in% missing_locs]$location_id
    rbindlist(lapply(region_missing_children, function(c) {
      child_dt <- copy(region_dt)
      child_dt[, location_id := c]
      child_dt[, location_name := hierarchy[location_id == c]$location_name]
    }))
  }))

full_dt <- rbind(full_dt, missing_dt, fill = T)

## Generate aggregates from most_detailed locations (Update to population-weighted)
  #pop <- fread("/ihme/covid-19/model-inputs/best/age_pop.csv")
  #pop <- aggregate(population ~ location_id, data=pop, function(x) sum(x))
  #full_dt <- merge(full_dt, pop[,c("location_id","population")], by="location_id", all.x = T)
  
  full_dt <- merge(full_dt, population, by="location_id", all.x = T)
  full_dt$number <- full_dt$smooth_prop_always * full_dt$population

  most_dt <- full_dt[location_id %in% hierarchy[most_detailed == 1]$location_id]
   
  aggs <- hierarchy[most_detailed != 1 & order(level) & !(location_id %in% parents), .(location_id, level)]
  # Don't forget China, Australia!
  add_parents <- c(6)
  for(ap in add_parents){
    if((ap %in% aggs$location_id)==F){
      aggs <- rbind(aggs, data.table(location_id = ap, level = 3))
    }
  }
  # Remove Australia
  aggs <- aggs[location_id != 71]

## We might start trying to rake estimates. Start with the USA
  if(rake_usa == T){
    aggs <- aggs[location_id != 102]
  
    us_full_dt <- full_dt[location_id == 102]
    us_full_dt$nat_count <- us_full_dt$number
    
    us_states_full_dt <- full_dt[location_id %in% hierarchy[parent_id==102, location_id]]
    state_date <- us_states_full_dt[, lapply(.SD, function(x) sum(x)), by="date", .SDcols = c("population","number")]
    state_date <- merge(state_date, us_full_dt[,c("date","nat_count")])
    state_date[, ratio_nat := number / nat_count]
    setnames(state_date, c("number","population"), c("state_count","pop_nat"))
    us_states_full_dt <- merge(us_states_full_dt, state_date, by="date")
    us_states_full_dt[, prop_nat_pop := population / pop_nat]
    us_states_full_dt[, raked_count := number / ratio_nat]
    us_states_full_dt[, smooth_prop_always := raked_count / population]
    us_states_full_dt[, number := raked_count]
    us_states_full_dt <- us_states_full_dt[, c("date","location_id","location_name","prop_always","N","n_total","type","smooth_prop_always","observed","population","number")]
    
    ggplot(full_dt[location_id == 555], aes(x=date, y=smooth_prop_always)) + geom_line() +
      geom_line(data=us_states_full_dt[location_id==555], aes(x=date, y=smooth_prop_always), col="red")
    
    full_dt <- full_dt[!(location_id %in% hierarchy[parent_id==102, location_id])]
    full_dt <- rbind(full_dt, us_states_full_dt)
  }

  agg_dt <- data.table()
  for(loc in rev(aggs$location_id)) {
    children <- setdiff(hierarchy[parent_id == loc]$location_id, loc)
    child_dt <- most_dt[location_id %in% children]
    parent_dt <- child_dt[, lapply(.SD, sum, na.rm = T), by = .(date), .SDcols = c("population","number","observed")]
    parent_dt[, smooth_prop_always := number / population]
    parent_dt[, location_id := loc]
    parent_dt[, location_name := hierarchy[location_id == loc]$location_name]
    parent_dt[observed != 0, observed := 1]
    parent_dt[, prop_always := NA]
    agg_dt <- rbind(agg_dt, parent_dt, use.names = T)
  }
  agg_dt[, source := "Parent aggregate"]
  full_dt <- rbind(full_dt[!(location_id %in% unique(agg_dt$location_id))], agg_dt, fill = T)

  setnames(full_dt, c("smooth_prop_always"), c("mask_use"))

##################################################################
## Keep only some columns
  out_dt <- full_dt[, .(location_id, location_name, date, observed, mask_use)]

## Check for missing locations
  missing_locs <- setdiff(hierarchy[most_detailed == 1]$location_id, unique(out_dt$location_id))
  if (length(missing_locs) > 0) {
    stop(paste0("Missing from most_detailed: ", paste(hierarchy[location_id %in% missing_locs]$location_name, collapse = ", ")))
  }

##################################################################
## Modify projections based on vaccine coverage (assume that some fraction of people vaccinated don't wear masks
## any longer). Because this script is a mess already, see sourced function.
  checkpoint <- copy(out_dt)

## Test more impact of vaccine on mask use.
#  out_dt <- vaccine_behavior_change(out_dt, vaccine_version = vaccine_model, reduction_mask_vaccine = 0.25, end_coverage = 1,
#                                    lag_vaccine = 90)
#
#  out_dt[, base_estimate_mask_use := mask_use]
#  
#  out_dt[option_100 < 0, option_100 := 0]
#  out_dt[, option_exp := ifelse(option_exp < option_100, option_100, option_exp)]
#  out_dt[, mask_use := ifelse(date > Sys.Date(), option_exp, mask_use)]
#
### Make a "worse" scenario
#  worse_dt <- vaccine_behavior_change(checkpoint, vaccine_version = vaccine_model, reduction_mask_vaccine = 1, end_coverage = 1,
#                                    lag_vaccine = 30, end_scalar = 1)
#  worse_dt[, base_estimate_mask_use := mask_use]
#
#  worse_dt[option_100 < 0, option_100 := 0]
#  worse_dt[, option_exp := ifelse(option_exp < option_100, option_100, option_exp)]
#  worse_dt[, mask_use := ifelse(date > Sys.Date(), option_exp, mask_use)]
#  worse_dt[, mask_use := ifelse(date > "2022-01-01", mask_use[date == "2022-01-01"], mask_use), by = "location_id"]
  
  
  #x <- out_dt[location_id == 81]
  
  test <- do.call(
    rbind,
    pbapply::pblapply(split(out_dt, by='location_id'), function(x) {
      
      tryCatch( { 
        
        t_delta <- 5
        t_current <- as.Date(max(x$date[x$observed == 1]))
        #t_min <- as.Date("2020-04-01")
        t_min <- t_current - 90
        t_max <- as.Date(max(x$date))
        
        sel <- which(x$date %in% (t_current - t_delta):t_current)
        mask_use_current <- median(x$mask_use[sel], na.rm=T) # Best = mean of past week
        
        sel <- which(x$date %in% t_min:t_current)
        mask_use_hi <- quantile(x$mask_use[sel], probs=0.75, na.rm=T) # Best = high quantile of tmin to tmax
        mask_use_lo <- quantile(x$mask_use[sel], probs=0.25, na.rm=T) # Worse = low quantile of of tmin to tmax
        
        
        #sel <- which(x$date %in% (t_min:t_current))
        #mask_use_past_min <- min(x$mask_use[sel], na.rm=T) # Worse = min since June 2020
        t_gap <- 30

        sel <- which(x$date %in% (t_current+1):(t_current+t_gap))
        x$mask_use[sel] <- NA
        
        x$mask_use_current <- x$mask_use_best <- x$mask_use_worse <- x$mask_use
        
        sel <- which(x$date %in% (t_current+t_gap):t_max)
        x$mask_use_current[sel] <- mask_use_current
        x$mask_use_best[sel] <- mask_use_hi
        x$mask_use_worse[sel] <- mask_use_lo
        
        # Smooth
        sel <- which(x$date %in% (t_current - t_delta):t_max)
        x$mask_use_current <- zoo::na.spline(x$mask_use_current)
        x$mask_use_best <- zoo::na.spline(x$mask_use_best)
        x$mask_use_worse <- zoo::na.spline(x$mask_use_worse)
        
        # If current mask use is better than past best, keep current as best
        if (mask_use_current < mask_use_lo) x$mask_use_worse <- x$mask_use_current
        if (mask_use_current > mask_use_hi) x$mask_use_best <- x$mask_use_current
        
        return(x)
        
      }, error=function(e){
        
        cat("Warning :", unique(x$location_id), ":", conditionMessage(e), "\n")
        
      })
      
      return(x)
    })
  )
  
  
  
  x <- test[location_id == 4]
  plot(x$mask_use_best, type='l', col='blue', lwd=2, ylim=c(0,1),
       main=x$location_name[1])
  lines(x$mask_use_worse, col='red', lwd=2)
  
  current_dt <- test
  current_dt$mask_use <- current_dt$mask_use_current
  
  out_dt <- test
  out_dt$mask_use <- out_dt$mask_use_best
  
  worse_dt <- test
  worse_dt$mask_use <- worse_dt$mask_use_worse
  
  
  
###############################################################################
## Make a couple alternative long range scenarios
if(long_range_scenario == T){
  source(paste0(code_dir, "FILEPATH/make_long_range_scenarios.R"))
}

###############################################################################
## US values should be average of worse and best scenarios
  us <- rbind(out_dt[location_id %in% hierarchy[parent_id %in% c(102, 570), location_id]],
              worse_dt[location_id %in% hierarchy[parent_id %in% c(102, 570), location_id]])
  
  us <- us[, lapply(.SD, function(x) mean(x)),
           by = c("location_id","location_name","date","observed"),
           .SDcols = "mask_use"]
  
  out_dt <- rbind(out_dt[!(location_id %in% unique(us$location_id))],
                  us, fill = T)
  
###############################################################################
## Copy values to counties!
  counties <- get_location_metadata(120, 841, release_id = 9)
# Drop WA subs (?)
#  counties <- counties[!(location_id %in% unique(out_dt$location_id))]
  county_dt <- expand.grid(location_id = counties[most_detailed == 1, location_id],
                           date = seq(min(out_dt$date), max(out_dt$date), by = "1 day"))
  county_dt <- data.table(county_dt)
  county_dt <- merge(county_dt, counties[,c("location_id","location_name","parent_id")], by = "location_id")
  
## Fix some Chilean, Colombian, Peruvian level 5
  county_dt[location_id == 54740, parent_id := 125]
  county_dt[location_id %in% c(60092, 60117), parent_id := 98]
  county_dt[location_id == 60913, parent_id := 123]
  
  out_counties <- merge(county_dt, out_dt[,c("location_id","date","observed","mask_use")], by.x = c("parent_id","date"), by.y = c("location_id","date"))  
  out_counties <- out_counties[order(location_id, date)]
  out_counties[, parent_id := NULL]

  worse_counties <- merge(county_dt, worse_dt[,c("location_id","date","observed","mask_use")], by.x = c("parent_id","date"), by.y = c("location_id","date"))  
  worse_counties <- worse_counties[order(location_id, date)]
  worse_counties[, parent_id := NULL]


###############################################################################
## Apply national levels to GBD locations  
# Find missing GBD locations


  # Find missing GBD locations
  gbd_hier <- fread("FILEPATH/gbd_analysis_hierarchy.csv")
  
  # Add problem subnats here
  add_parents <- hierarchy$location_id[hierarchy$location_name %in% c('South Africa', 'Indonesia')]
  add_gbd_locs <- gbd_hier$location_id[gbd_hier$parent_id %in% add_parents]
    
  out_dt <- out_dt[!(out_dt$location_id %in% add_gbd_locs)]
  worse_dt <- worse_dt[!(worse_dt$location_id %in% add_gbd_locs)]
  
  out_gbd <- split_gbd_locations(out_dt, hierarchy)
  worse_gbd <- split_gbd_locations(worse_dt, hierarchy)
  
  out_dt <- rbind(out_dt, out_gbd, fill = T)
  worse_dt <- rbind(worse_dt, worse_gbd, fill = T)
  

  
###############################################################################
## Save!
  # Are there any duplicate dates?
  dedup <- unique(out_dt[ , c("location_id","date")])
  dups <- out_dt[duplicated(out_dt, by=c("location_id","date"))]
  
  if(nrow(out_dt) != nrow(dedup)){
    message("There are duplicate date/location_ids somewhere in the dataset!")
    stop()
  }

  write.csv(out_dt[, c("location_id","date","location_name","observed","mask_use")], out_data_path, row.names = F)
  write.csv(worse_dt[, c("location_id","date","location_name","observed","mask_use")], 
            paste0(output_dir, "/mask_use_worse.csv"), row.names = F)

  
  
  # Check
  #d <- read.csv("/ihme/covid-19/mask-use-outputs/2021_07_02.10/mask_use.csv")
  
  #tmp <- d[d$location_name == 'Jakarta',]
  #tmp <- d[d$location_name == 'Indonesia',]

  #tmp <- d[d$location_name == 'Western Cape',]
  #tmp <- d[d$location_name == 'South Africa',]

  #plot(as.Date(tmp$date), tmp$mask_use)
  
  
  
  
###############################################################################
## Compare to previous 
## Submit QC jobs
  error_path <- paste0("FILEPATH",Sys.info()['user'],"FILEPATH")
  output_path <- paste0("FILEPATH",Sys.info()['user'],"FILEPATH")
  
## Submit job for line plot comparing versions/scenarios ##
  version <- unlist(strsplit(output_dir, "/"))[6]
  compare_version <- "best"
  plot_script <- paste0("FILEPATH",Sys.info()['user'],"FILEPATH/diagnostic_line_plots.R")
  job_name <- paste0("mask_use_lines")
  command <- paste0(
    "qsub -l m_mem_free=10G -l archive=TRUE -l fthread=2 -l h_rt=00:20:00",
    " -q d.q -P proj_covid -e ",error_path," -o ",output_path,
    " -N ",job_name," -terse /ihme/singularity-images/rstudio/shells/execRscript.sh ",
    "-i /ihme/singularity-images/rstudio/ihme_rstudio_3602.img -s ",
    plot_script," --version ", version, " --compare_version ", compare_version
  )
  system(command)

yaml::write_yaml(
  list(
    script = "mask_use.R",
    output_dir = output_dir,
    input_files = input_files
  ),
  file = metadata_path
)


##---------------------------------------------------------------

## Create a new directory, save county estimates there ##
if(save_counties == T){
  message("saving counties...")
  true_dir <- output_dir

  county_dir <- ihme.covid::get_output_dir(
    root = "FILEPATH",
    date = "today")
  file.copy(list.files(output_dir, full.names = T),
            county_dir,
            recursive = T)
  write.csv(out_counties[, c("location_id","date","location_name","observed","mask_use")], 
            paste0(county_dir,"/mask_use.csv"), row.names = F)
  write.csv(worse_counties[, c("location_id","date","location_name","observed","mask_use")], 
            paste0(county_dir, "/mask_use_worse.csv"), row.names = F)
  output_dir <- county_dir
  universal_level <- 0.95
  source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/best_scenario_masks.R"))
  message(paste0("County outputs saved to this directory: ", county_dir))
  output_dir <- true_dir
}



message(paste("Output directory:", output_dir))
message(paste0("The last observed date of Facebook data was ",
               max(dt[source=="Facebook"]$date),
               " and the last observed date of PREMISE data was "),
        max(dt[source == "Adjusted Premise"]$date))
