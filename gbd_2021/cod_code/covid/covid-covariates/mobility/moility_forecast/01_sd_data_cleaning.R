## Purpose: Combine mandate and mobility data prior to fitting the mobility regression 
## ------------------------------------------------------------------------------------

# load libraries
library(dplyr)
library(data.table)
library(tidyr)
library(zoo)
source(file.path("FILEPATH/get_location_metadata.R"))
library('ihme.covid', lib.loc = 'FILEPATH')
R.utils::sourceDirectory('functions', modifiedOnly = FALSE)

# Some constants we don't expect to change
start_date <- as.Date("2020-01-01")
end_date <- as.Date("2023-12-31")

INPUTS_DIR <- "FILEPATH"
OUTPUTS_ROOT <- "FILEPATH"
loc_set_file <-  "FILEPATH/location_sets.csv"

# location set arguments
loc_sets <- fread(loc_set_file)

lsid <- loc_sets[note == 'use',ls]
lsvid <-loc_sets[note == 'use',lsv]
rid <- loc_sets[note == 'use',ri]

output_date <- "today" # override with a specific date if you need to; will default to the date of running
inputs_version <- "latest" # "best", "latest" or a YYYY_MM_DD.VV release; ETL version used for pulling population
sd_version <- "latest" # "best", "latest" or a YYYY_MM_DD.VV release; Mandates version

# PARENT LOCATIONS TO MODEL
# They have subnats in the covariate hierarchy but their subnats don't have detailed mandates data.
parent_locs_to_model <- c(71, 11, 196)  

# LOCATIONS TO DROP
locs_to_drop <- c()

# Define directories
gpr_root <- ihme.covid::get_latest_output_dir(root = 'FILEPATH')
inputs_dir <- file.path(INPUTS_DIR, inputs_version)
output_dir <- ihme.covid::get_output_dir(OUTPUTS_ROOT, output_date)


#################
## Diagnostics ##
#################
ihme.covid::print_debug(inputs_dir, output_dir, gpr_root)

Sys.umask('0002')

#####################################################
## read in the SD mandates data and clean the data ##
#####################################################

# input and output directory and today's stamp
SMOOTH_SD_PATH <- file.path(gpr_root, 'smoothed_mobility.csv') 
DETAILED_MANDATE_PATH <- file.path(OUTPUTS_ROOT,'mandates',sd_version,'all_mandate_data_prepped.csv')
SOCIAL_DISTANCING_DATA_PATH <- file.path(output_dir, 'social_distancing.csv')
SMOOTHED_MOBILITY_PATH <- file.path(output_dir, 'smooth_mobility_with_sd.csv')
POPULATION_PATH <- file.path(inputs_dir, "output_measures/population/all_populations.csv")
POPULATION_OUT_PATH <- file.path(output_dir, 'population.csv')
HIERARCHY_OUT_PATH <- file.path(output_dir, 'hierarchy.csv')
metadata_path <- file.path(output_dir, "01_sd_data_cleaning.metadata.yaml")

# location hierarchy
hierarchy <- get_location_metadata(location_set_id = lsid, location_set_version_id = lsvid, release_id = rid)

# update hierarchy to make desired locs most-detailed and drop their child locs
child_locs_to_drop <- NULL
for(parent in parent_locs_to_model){
  child_locs <- hierarchy[parent_id==parent, location_id]
  child_locs_to_drop <- c(child_locs_to_drop, child_locs)
}
hierarchy <- hierarchy[!(location_id %in% child_locs_to_drop)]
hierarchy[location_id %in% parent_locs_to_model, most_detailed := 1]
hierarchy <- hierarchy[!(location_id %in% locs_to_drop)]
fwrite(hierarchy, HIERARCHY_OUT_PATH)

# populations
pops <- fread(POPULATION_PATH)
pops <- pops[age_group_id == 22 & sex_id == 3 & year_id == 2019, .(location_id, population)]
fwrite(pops, POPULATION_OUT_PATH)

# read in social distancing data
df_sd <- fread(DETAILED_MANDATE_PATH)[,1:7]
df_sd[, date := as.Date(date)]

# Rename variables to be consistent with original code

# (a) Stay at home order (stay_home)
setnames(df_sd, 'stay_home', 'sd1_lift')

# (b) School closures (educational_fac)
setnames(df_sd, 'educational_fac', 'sd2_lift')

# (c) Non-essential business closures (all_noness_business)
setnames(df_sd, 'all_noness_business', 'sd3_lift')

# (d) Gathering restrictions (any_gathering_restrict)
setnames(df_sd, 'any_gathering_restrict', 'psd1_lift')

# (e) Any business closures (any_business)
setnames(df_sd, 'any_business', 'psd3_lift')

# Last observation carried forward
df_sd[, sd1_lift := na.locf(sd1_lift, na.rm=F), by=location_id]
df_sd[, sd2_lift := na.locf(sd2_lift, na.rm=F), by=location_id]
df_sd[, sd3_lift := na.locf(sd3_lift, na.rm=F), by=location_id]
df_sd[, psd1_lift := na.locf(psd1_lift, na.rm=F), by=location_id]
df_sd[, psd3_lift := na.locf(psd3_lift, na.rm=F), by=location_id]

# Add anticipate variable
df_sd[, num_on := sd1_lift + sd2_lift + sd3_lift + psd1_lift + psd3_lift]
anticipate_dates <- df_sd[num_on>0, min(date) - 7, by='location_id']
setnames(anticipate_dates, 'V1', 'anticipate_date')
df_sd <- merge(df_sd, anticipate_dates, by='location_id', all.x=T)
df_sd[, anticipate := ifelse(date>=anticipate_date, 1, 0)]
df_sd[is.na(anticipate), anticipate := 0][, c('num_on', 'anticipate_date') := NULL]

# Drop parent locations (only model most-detailed)
parent_locs <- hierarchy[most_detailed == 0, location_id]
df_sd <- df_sd[!(location_id %in% parent_locs)]

# Save the cleaned SD data
fwrite(df_sd, SOCIAL_DISTANCING_DATA_PATH)

# Save the unique location_id in the SD dataset
sd_locs <- unique(df_sd[, location_id])

# Check for missing locations
missing_locs <- setdiff(hierarchy[most_detailed == 1, location_id], sd_locs)
if(length(missing_locs > 0)){
  warning(paste0("Missing the following locations from the social distancing data required to model:", paste(hierarchy[location_id %in% missing_locs, location_name], collapse = "\n")))
}


##############################################################
## read in the smoothed mobility metrics and clean the data ##
##############################################################

# read in smoothed mobility metric
smooth_sd <- fread(SMOOTH_SD_PATH)
smooth_sd[, date := as.Date(date, "%Y-%m-%d")]

# check for missing locations
missing_mob <- setdiff(hierarchy[most_detailed == 1, location_id], unique(smooth_sd$location_id))
if(length(missing_mob > 0)){
  stop(paste0("Missing the following locations from the mobility data required to model: \n", paste(hierarchy[location_id %in% missing_mob, location_name], collapse = "\n")))
}

# subset on countries/locations where we have SD mandates data and order the data by location_id and date
smooth_sd <- smooth_sd[location_id %in% sd_locs, .(location_id, date, mean)]
smooth_sd <- smooth_sd[order(location_id, date)]

# fill in any missing dates
ndays <- as.numeric(end_date - start_date) +1 
my.location_id.list <- lapply(smooth_sd[, unique(location_id)],
                              function(x) data.table(location_id = rep(x, ndays), date=seq(start_date, end_date, by="day")))
temp.dt <- rbindlist(my.location_id.list)
smooth_sd_exp <- merge(temp.dt, smooth_sd, by=c("location_id", "date"), all.x = T)


# Merge mobility with SD covariates
merged_df <- merge(smooth_sd_exp, df_sd, by=c("location_id","date"), all.x=T)

# Add ihme_loc_id
merged_df <- merge(merged_df, hierarchy[,.(location_id, ihme_loc_id)], by='location_id', all.x=T)

# Add population for aggregation
merged_df <- merge(merged_df, pops, by="location_id", all.x=T)

missing_pops <- intersect(merged_df[is.na(population), unique(location_id)], hierarchy$location_id)
if(length(missing_pops) > 0){
  stop(paste0("Missing populations from the following locations in the hierarchy: \n", paste(missing_pops, collapse = "\n")))
}

# save the full dataset
fwrite(merged_df, SMOOTHED_MOBILITY_PATH)


yaml::write_yaml(
  list(
    script = "01_sd_data_cleaning.R",
    lsvid = lsvid,
    output_dir = output_dir,
    input_files = ihme.covid::get.input.files()
  ),
  file = metadata_path
)

