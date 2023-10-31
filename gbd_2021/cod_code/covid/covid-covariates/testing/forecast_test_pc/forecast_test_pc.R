#######################################################################
### Project testing per capita ########################################
## The purpose of this code is to take smoothed testing per capita 
## from prep_data.R and project testing into the future and to fill
## testing for missing locations.
## To forecast test per capita, the code linearly extends the 
## trend in testing rates. It caps testing rate at a frontier
## identified using SDI from back in summer/fall 2020. 
## For locations without any testing data and non-subnationals,
## it uses SDI to determine an average rate of change ARC to 
## make projections into the future.
## Written by INDIVIDUAL_NAME INDIVIDUAL_NAME, maintained and adapted by INDIVIDUAL_NAME INDIVIDUAL_NAME

## Requires: current_version
## File created in "FILEPATH/prep_data.R"
# source(paste0("FILEPATH", Sys.info()['user'],"FILEPATH/prep_data.R"))
#######################################################################
# source(paste0("FILEPATH", Sys.info()['user'],"FILEPATH/forecast_test_pc.R"))

## Arguments
if (interactive()) {
  
  user <- Sys.info()["user"]
  code_dir <- paste0("FILEPATH", user, "FILEPATH")
  lsvid <- lsvid_covar
  seir_covariates_version <- "2021_11_16.01" CLUSTER_NAME
  
} else {
  
  parser <- argparse::ArgumentParser()
  parser$add_argument("--lsvid", type = "integer", required = TRUE, help = "location set version id to use")
INTERNAL_COMMENT)
  parser$add_argument("--seir-covariates-version", default = "best", help = "Version of seir-covariates. Defaults to 'best'. Pass a full YYYY_MM_DD.VV or 'latest' if you provide")
  args <- parser$parse_args()
  code_dir <- ihme.covid::get_script_dir()
  lsvid <- args$lsvid
  output_dir <- args$outputs_directory
  seir_covariates_version <- args$seir_covariates_version
  current_version <- basename(output_dir)
  
}

## Paths
seir_testing_reference <- file.path(SEIR_COVARIATES_ROOT, seir_covariates_version, "FILEPATH/reference_scenario.csv") 
input_data_path <- file.path(output_dir, "data_smooth.csv") 
first_case_path <- file.path(output_dir, "first_case_date.csv")
detailed_out_path <- file.path(output_dir, "forecast_raked_test_pc_detailed.csv")
simple_out_path <- file.path(output_dir, "forecast_raked_test_pc_simple.csv")
scenario_dict_out_path <- file.path(output_dir, "testing_scenario_dict.csv")
plot_scenarios_out_path <- file.path(output_dir, "forecast_test_pc.pdf")
metadata_path <- file.path(output_dir, "forecast_test_pc.metadata.yaml")

## Functions
source(file.path("FILEPATH/get_location_metadata.R"))
source(file.path("FILEPATH/get_covariate_estimates.R"))
source(file.path(code_dir, "forecast_all_locs.R"))
source(file.path(code_dir, "rake_vals.R"))
source(file.path(code_dir, "gen_better_worse.R"))
source(file.path(code_dir, "diagnostics.R"))
source(paste0("FILEPATH", Sys.info()['user'],"FILEPATH/make_all_gbd_locations.R"))

## Tables
hierarchy <- get_location_metadata(location_set_id = 115, location_set_version_id = lsvid, release_id = 9)
hier_supp <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
hierarchy[, c("super_region_id", "super_region_name", "region_id", "region_name") := NULL]
hier_supp[, merge_id := location_id]
hierarchy[location_id %in% hier_supp$location_id, merge_id := location_id]
hierarchy[is.na(merge_id), merge_id := parent_id]
hier_supp <- unique(hier_supp[, .(merge_id, super_region_id, super_region_name, region_id, region_name)])
hierarchy <- merge(hierarchy, hier_supp, by = c("merge_id"), all.x = T)

## Pull in SDI covariate, frontier for testing/capita
sdi <- get_covariate_estimates(location_id = "all", year_id=2019, gbd_round_id = 6, decomp_step = "step4", covariate_id = 881)
setnames(sdi, "mean_value","sdi")

## Read in predicted frontiers by LDI
frontier_df <- fread("FILEPATH/testing_frontier_analysis_full.csv") # Location will probably need to change
frontier_df[, frontier := sfa_fit / 1e5]
frontier_df <- merge(frontier_df, hierarchy[,c("location_id","parent_id")], by="location_id")
# Group by parent_id, assign parent_frontier = the max among them
frontier_df[, parent_frontier := max(frontier, na.rm=T), by=parent_id]

##--------------------------------------------------------------------------------------------
## Code
# Prep data
dt <- fread(input_data_path)
dt[, date := as.Date(date)]
in_dt <- copy(dt)
dt[, total_pc_100 := daily_total / population * 1e5]

## Forecast

# Hot drop for Brazil national
dt <- subset(dt, location_id != 135)

out_dt <- forecast_all_locs(dt, var_name = "total_pc_100", ncores = ncores, end_date = end_date)

# Don't pass on negatives
out_dt[fcast_total_pc_100 < 0, fcast_total_pc_100 := 0]

out_dt[, raked_ref_test_pc := fcast_total_pc_100 / 1e5]

## Generate better worse
out_dt <- gen_better_worse_all(dt, out_dt)

## Use national values for missing subnationals
missing_children <- setdiff(hierarchy[most_detailed == 1 & level > 3]$location_id, unique(out_dt$location_id))
# Inspect missing children
# hierarchy[location_id %in% missing_children] %>% view()
parents <- unique(hierarchy[location_id %in% missing_children]$parent_id)


# but as is it replaces missing subnationals for its parent which may not
# necessarily be the national value, i.e. China province under mainland China
missing_children_dt <- rbindlist(lapply(parents, function(p) {
  parent_dt <- copy(out_dt[location_id == p])
  all_children <- setdiff(hierarchy[parent_id == p]$location_id, p)
  impute_locs <- intersect(missing_children, all_children)
  rbindlist(lapply(impute_locs, function(i) {
    child_dt <- copy(parent_dt)
    child_dt[, location_id := i]
    child_dt[, location_name := hierarchy[location_id == i]$location_name]
    child_dt[, pop := NA]
  }))
}))

# Inspect the difference between missing_children and missing_children_dt
# missing_locs_diff <- setdiff(missing_children, missing_children_dt$location_id)
# hierarchy %>%
#   filter(location_id %in% missing_locs_diff) %>%
#   view()

out_dt <- rbind(out_dt, missing_children_dt, fill=TRUE)

## Replace negatives with zeros
out_dt[raked_ref_test_pc < 0, raked_ref_test_pc := 0]
out_dt[better_fcast < 0, better_fcast := 0]
out_dt[worse_fcast < 0, worse_fcast := 0]

out_dt <- merge(out_dt, hierarchy[,c("location_id","level","parent_id")], by="location_id")

## Cap at frontier
if(use_estimated_frontier == T){
  # For locations in out_dt that are missing a frontier, take the parent frontier
  
  missing_from_frontier <- setdiff(unique(out_dt$location_id), frontier_df$location_id)
  out_dt[, merge_id := ifelse(location_id %in% missing_from_frontier, parent_id, location_id)] 
  # Set counties in WA to WA
  out_dt[, merge_id := ifelse(parent_id == 570, 102, merge_id)]
  out_dt <- merge(out_dt, frontier_df[,c("location_id","frontier")], by.x="merge_id", by.y="location_id", all.x=T) 
  
  message(paste("Missing frontiers in ", paste(unique(out_dt[is.na(frontier)]$location_name), collapse=", ")))
  
  # Determine the subnational with the highest frontier by parent_id
  out_dt[, parent_frontier := max(frontier, na.rm=T), by=parent_id]
  out_dt[, loc_frontier := frontier]
  
  if(use_parent_frontier == T){
    
    # of a national, but should this actually be for level > 4?
    out_dt[level == 4, frontier := parent_frontier]
  }
  
  # Manually set frontier in Brazil, I think this is the only tricky location
  out_dt[, frontier := ifelse(parent_id == 135, frontier_df[location_name=="Brazil"]$frontier, frontier)]
  frontier_df[, frontier := ifelse(parent_id == 135, frontier_df[location_name=="Brazil"]$frontier, frontier)]
  
  # Set frontier to 500/100k for US locations
  out_dt[, frontier := ifelse(parent_id %in% c(570, 102), 500/1e5, frontier)]
  
  # Set frontier to 147/100k for India states
  
  out_dt[, frontier := ifelse(parent_id %in% c(163), 147/1e5, frontier)]
  
  out_dt[raked_ref_test_pc > frontier & observed == F, raked_ref_test_pc := frontier]
  out_dt$merge_id <- NULL
} else {
  out_dt$frontier <- frontier
  out_dt[raked_ref_test_pc > frontier & observed == F, raked_ref_test_pc := frontier]
}

## Check for missing locations
missing_locs <- setdiff(hierarchy[most_detailed == 1]$location_id, unique(out_dt$location_id))
if (length(missing_locs) > 0) {
  message(paste0("Missing: ", paste(hierarchy[location_id %in% missing_locs]$location_name, collapse = ", ")))
}

write.csv(hierarchy[location_id %in% missing_locs], paste0(output_dir, "/locations_with_sdi_predictions.csv"), row.names=F)

## Flatline locations where Testing > Frontier (eg South Korea and Japan)
# Also flatline locations that exceed frontier at last observation
obs_dt <- out_dt[observed == T]
max_date <- obs_dt[, .(date = max(date)), by = location_id]
max_dt <- merge(obs_dt, max_date)
exceed_locs <- unique(max_dt[raked_ref_test_pc > frontier]$location_id)
# s korea only had 1 row and was dropped! (loc_id 68)
for(loc_id in c(exceed_locs)) {
  if(use_estimated_frontier==T){
    impute_val <- max_dt[location_id == loc_id]$raked_ref_test_pc
  } else {
    impute_dt <- out_dt[location_id == loc_id & observed == T]
    impute_val <- impute_dt[date == max(date)]$raked_ref_test_pc
  }  
  out_dt[location_id == loc_id & observed == F, raked_ref_test_pc := impute_val]
}

## Need to merge with SDI ##
out_dt <- merge(out_dt, sdi[,c("location_id","sdi")], by="location_id", all.x=T)

# Get LDI to ballpark China frontier
# Closest LDI value to Macao is Qatar (set to highest value among children)
chn_frontier <- frontier_df[location_name=="Qatar"]$frontier

## Linear regression ARC and SDI
plot_data <- aggregate(cbind(arc, sdi) ~ location_id, data=data.frame(out_dt), function(x) mean(x))
lm_arc_sdi <- lm(arc ~ sdi, data=plot_data)
log_arc_sdi <- lm(log(arc) ~ sdi, data=plot_data)

out_dt$sdi_pred_arc <- exp(predict(log_arc_sdi, newdata=out_dt))
sdi$sdi_pred_arc <- exp(predict(log_arc_sdi, newdata=sdi))

sdi_pred <- data.frame(sdi = seq(0,1,0.01))
sdi_pred$prediction <- predict(lm_arc_sdi, newdata=sdi_pred)
sdi_pred$log_pred <- exp(predict(log_arc_sdi, newdata=sdi_pred))

plot_data <- aggregate(cbind(arc, sdi) ~ location_id, data=data.frame(out_dt), function(x) mean(x))
ggplot(sdi_pred, aes(x=sdi)) + ylab("ARC") + 
  geom_line(aes(y=prediction), col="purple") +
  geom_line(aes(y=log_pred), col="blue") + 
  theme_minimal() +
  geom_point(data=plot_data, aes(x=sdi, y=arc), alpha=0.4)

## Predict onto full dataset
# Find locations that aren't in SDI
missing_children_sdi <- rbindlist(lapply(parents, function(p) {
  parent_dt <- copy(sdi[location_id == p])
  all_children <- setdiff(hierarchy[parent_id == p]$location_id, p)
  impute_locs <- intersect(missing_children, all_children)
  rbindlist(lapply(impute_locs, function(i) {
    child_dt <- copy(parent_dt)
    child_dt[, location_id := i]
    child_dt[, location_name := hierarchy[location_id == i]$location_name]
  }))
}))

## Make a regional average for missing locations
message("Imputing missing locations with an SDI prediction")
first_case_dt <- fread(first_case_path)
setnames(first_case_dt, "first_case_date", "date")
first_case_dt[, date := as.Date(date)]
regions <- unique(hierarchy[location_id %in% missing_locs]$region_id)

## So this function/lapply uses the regional mean arc (?) to apply to locations without data
missing_dt <- rbindlist(lapply(regions, function(r) {
  # Find all locations that fall under the region
  all_region_locs <- hierarchy[region_id == r]$location_id
  # Find all locations in the region that were not missing data and are most detailed
  region_locs <- setdiff(all_region_locs, missing_locs)
  # Of those, only select the ones with an arc value, this excludes non most detailed
  # locs that didn't have data
  region_dt <- unique(out_dt[!is.na(arc) & location_id %in% region_locs, .(location_id, arc)])
  
  # Take the average arc of the region / 100,000
  
  arc <- mean(region_dt$arc) / 1e5
  
  # Find all the locations in the region that were missing data
  region_missing_children <- hierarchy[region_id == r & location_id %in% missing_locs]$location_id
  
  # For each location with missing data, generate the time series
  rbindlist(lapply(region_missing_children, function(c) {
    child_dt <- first_case_dt[location_id == c]
    if(nrow(child_dt) == 0) { # No first case date => min date of region
      child_dt <- data.table(
        location_id = c,
        date = min(first_case_dt[location_id %in% all_region_locs]$date)
      )
    }
    first_date <- child_dt$date
    extend_date <- end_date - first_date 
    child_dt <- rbind(
      child_dt, 
      data.table(
        location_id = c,
        date = (first_date + seq(extend_date))
      )
    )
    
    # ARC should be the predicted value, clean this up
    arc <- unique(sdi$sdi_pred_arc[sdi$location_id==c]) / 1e5
    child_dt[, raked_ref_test_pc := arc + as.integer(date - first_date) * arc]
    child_dt$arc <- arc
    child_dt$sdi <- sdi$sdi[sdi$location_id==c]
    child_dt[, location_id := c]
    child_dt[, location_name := hierarchy[location_id == c]$location_name]
    child_dt[, observed := 0]
    child_dt[, parent_id := hierarchy[location_id == c]$parent_id]
    child_dt[, level := hierarchy[location_id == c]$level]
    
    return(child_dt)
  }))
}))


# data like China provinces
out_dt <- rbind(out_dt, missing_dt, fill = T)

## INDIVIDUAL_NAME wants ARC to be halved in Central Asia
out_dt[location_id %in% hierarchy[parent_id == 32, location_id] & location_id %in% missing_locs,
       raked_ref_test_pc := raked_ref_test_pc * 0.5]

## Locations without testing data need to be re-merged
frontier_df[, backup_frontier := frontier]
out_dt[, parent_frontier := NULL]
out_dt <- merge(out_dt, frontier_df[,c("location_id","backup_frontier","parent_frontier")], by="location_id", all.x=T)
out_dt[, frontier := ifelse(is.na(frontier), backup_frontier, frontier)]
investigate <- unique(out_dt[is.na(frontier)]$location_name)

pre_second_parent <- copy(out_dt)

if(use_estimated_frontier==T){
  # Determine the subnational with the highest frontier by parent_id
  out_dt[, parent_frontier := max(frontier, na.rm=T), by=parent_id]
  out_dt[, loc_frontier := frontier]
  
  if(use_parent_frontier == T){
    out_dt[level == 4, frontier := parent_frontier]
  } else {
    # China subnationals are missing a frontier
    
    # that could be used?
    out_dt[location_id %in% hierarchy[parent_id==6]$location_id]$frontier <- chn_frontier
    # Brazil subnationals are missing a frontier
    out_dt[location_id %in% hierarchy[parent_id==135]$location_id]$frontier <- frontier_df[location_name=="Brazil"]$frontier
  }
} 

# What to do with the other locations?
out_dt <- merge(out_dt, hierarchy[,c("location_id","region_id","super_region_id")], by="location_id")
out_dt[, regional_frontier := mean(loc_frontier, na.rm=T), by="region_id"]
out_dt[, sr_regional_frontier := mean(loc_frontier, na.rm=T), by="super_region_id"]
out_dt[, frontier := ifelse(is.na(frontier), regional_frontier, frontier)]

# Cap at frontier after regional averaging
out_dt[raked_ref_test_pc > frontier & observed == F & !(location_id %in% exceed_locs), raked_ref_test_pc := frontier]


## Check for missing locations
missing_locs <- setdiff(hierarchy[most_detailed == 1]$location_id, unique(out_dt$location_id))
if (length(missing_locs) > 0) {
  stop(paste0("Missing from most_detailed: ", paste(hierarchy[location_id %in% missing_locs]$location_name, collapse = ", ")))
}

##------------------------------------------------------------------------
## Save
# Is this file used for anything?
write.csv(out_dt, detailed_out_path, row.names = F)

# Generate a simpler file
out_dt[, test_pc := raked_ref_test_pc]
out_dt[, test_pc_better := better_fcast]
out_dt[, test_pc_worse := worse_fcast]


simple_dt <- out_dt[location_id %in% hierarchy$location_id, .(location_id, location_name, date, observed, test_pc, test_pc_better, test_pc_worse, population, frontier)]

## Generate aggregates from most_detailed locations
parents <- subset(parents, parents != 135) # Brazil is missing

most_dt <- simple_dt[location_id %in% hierarchy[most_detailed == 1]$location_id | location_id %in% parents]

## Merge with populations (not sure why some places don't have populations)
#population <- fread("FILEPATH/age_pop.csv")
population <- fread("FILEPATH/all_populations.csv")
population <- population[age_group_id == 22 & sex_id == 3]
population <- population[, lapply(.SD, function(x) sum(x)), by="location_id", .SDcols = "population"]

# This is breaking because there is population column in both data tables here
# Removing population from most_dt assuming that merging in new population data here
# is done to add locations
most_dt <- most_dt[,-'population']
most_dt <- merge(most_dt, population, by = "location_id", all.x = T)
#most_dt[is.na(population), population := population]

aggs <- hierarchy[most_detailed != 1 & order(level) & !(location_id %in% parents), .(location_id, level)]
# Don't forget China!
if((6 %in% aggs$location_id)==F){
  china <- data.table(location_id = 6, level = 3)
  aggs <- rbind(aggs, china)
}

# Also need to add in mainland China
mainland_china <- data.table(location_id = 44533, level = 4)
aggs <- rbind(aggs, mainland_china)

for(loc in rev(aggs$location_id)) {
  children <- setdiff(hierarchy[parent_id == loc]$location_id, loc)
  child_dt <- most_dt[location_id %in% children]
  child_dt[location_id != 354, c("test", "test_better", "test_worse") := .(test_pc * population, test_pc_better * population, test_pc_worse * population)]
  na_dt <- child_dt[, .(all_na = all(is.na(test))), by = .(date)]
  parent_dt <- child_dt[, lapply(.SD, sum, na.rm = T), by = .(date), .SDcols = c("test", "test_better", "test_worse", "observed", "population")]
  parent_dt <- merge(parent_dt, na_dt)
  parent_dt[(all_na), c("test", "test_better", "test_worse") := NA]
  parent_dt[, all_na := NULL]
  parent_dt[, location_id := loc]
  parent_dt[, location_name := hierarchy[location_id == loc]$location_name]
  parent_dt[, c("test_pc", "test_pc_better", "test_pc_worse") := .(test / population, test_better / population, test_worse / population)]
  parent_dt[, c("test", "test_better", "test_worse") := NULL]
  parent_dt[observed != 0, observed := 1]
  most_dt <- rbind(most_dt, parent_dt, use.names = T, fill = T)
}
simple_dt <- most_dt

# REDACTED, China Frontier got dropped somehow, add it back in
# somewhere above the hierarchy changes is causing it to be dropped
china_frontier <- frontier_df[location_id == 6]$frontier
simple_dt[location_id == 6]$frontier <- china_frontier

## Check for missing locations
missing_locs <- setdiff(hierarchy$location_id, unique(simple_dt$location_id))
if (length(missing_locs) > 0) {
  stop(paste0("Missing: ", paste(hierarchy[location_id %in% missing_locs]$location_name, collapse = ", ")))
}

## Check all non-NA values after first non-NA
first_non_na <- simple_dt[!is.na(test_pc), .(min_date = min(date)), by = .(location_id)]
test_dt <- merge(simple_dt, first_non_na)
offending_dt <- test_dt[date > min_date][is.na(test_dt[date > min_date]$test_pc),]
if(nrow(offending_dt) > 0) {
  stop(paste0("NAs after first non-NA in: ", paste(unique(offending_dt$location_name), collpase = ", ")))
}


##-------------------------------------------------------------
## Create US County estimates as just parent level
simple_dt <- simple_dt[!(location_id == 97 & date < "2020-01-15")]
#counties <- get_location_metadata(120, 841, release_id = 9) # Old hierarchy
counties <- fread('FILEPATH/fh_small_area_hierarchy.csv') # New hierarchy
county_dt <- expand.grid(location_id = counties[most_detailed == 1, location_id],
                         date = seq(min(simple_dt$date), max(simple_dt$date), by = "1 day"))
county_dt <- data.table(county_dt)
county_dt <- merge(county_dt, counties[,c("location_id","location_name","parent_id")], by = "location_id")

## Fix some Chilean, Colombian, Peruvian level 5
county_dt[location_id == 54740, parent_id := 125]
county_dt[location_id %in% c(60092, 60117), parent_id := 98]
county_dt[location_id == 60913, parent_id := 123]

simple_counties <- merge(county_dt, simple_dt[,c("location_id","date","observed","test_pc")], by.x = c("parent_id","date"), by.y = c("location_id","date"))  
simple_counties <- simple_counties[order(location_id, date)]
simple_counties[, parent_id := NULL]

simple_counties <- rbind(simple_dt[1,], simple_counties, fill = T)
simple_counties <- simple_counties[2:.N,]

setdiff(unique(simple_counties$location_id), counties[most_detailed == 1, location_id])
setdiff(counties[most_detailed == 1, location_id], unique(simple_counties$location_id))

##-------------------------------------------------------------  

## Save "simpler" file
write.csv(simple_dt, simple_out_path, row.names = F)

# Write out scenario dictionary
dict <- data.table(
  scenario_id = 1:3,
  scenario_name = c("reference", "better", "worse"),
  column_name = c("test_pc", "test_pc_better", "test_pc_worse")
)
write.csv(dict, scenario_dict_out_path, row.names = F)

## Write YAML files
yaml::write_yaml(
  list(
    script = "forecast_test_pc.R",
    location_set_version_id = lsvid,
    output_dir = output_dir,
    input_files = ihme.covid::get.input.files()
  ),
  file = paste0(output_dir, "/forecast_test_pc.metadata.yaml") # sort of cheating here - this also needs a line near the top of the script
)

## Combine YAML files per request from INDIVIDUAL_NAME)
data_prep_yaml <- read_yaml(paste0(output_dir, "/prep_data.metadata.yaml"))
con <- file(paste0(output_dir, "/metadata.yaml"), "w")
write_yaml(list(
  forecast = list(
    script = "forecast_test_pc.R",
    location_set_version_id = lsvid,
    output_dir = output_dir,
    input_files = ihme.covid::get.input.files()
  )
), file = con)
write_yaml(list(prep = list(data_prep_yaml)), file = con)
close(con)

##-----------------------------------------------------------------------------
## Impute missing locations for GBD
source(paste0(code_dir, "make_all_gbd_locations.R"))
# this overwrites saved file with GBD locations appended, should change that probably. 

## Save counties
## I can't figure out how to automate this... explore a bit.
if(save_counties == T){
  
  if(!dir.exists(county_dir)) dir.create(county_dir)
  system(glue(SYSTEM_COMMAND))
  write.csv(simple_counties, paste0(county_dir, "/forecast_raked_test_pc_simple.csv"), row.names = F)
  message(paste("County version saved to:", county_dir))
}  

