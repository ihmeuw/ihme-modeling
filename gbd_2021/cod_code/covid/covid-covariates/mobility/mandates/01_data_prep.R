## Purpose: Prep data for mandate regression
## -------------------------------------------

# Source packages
library(data.table)
library('ihme.covid', lib.loc = 'FILEPATH')
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_covariate_estimates.R")
R.utils::sourceDirectory('functions', modifiedOnly = FALSE)

# Set Args
pneumonia_version <- "best" 
end_date <- "2023-12-31"  #generate mandate forecasts through this date

# Define file paths
save_root <-  "FILEPATH"
save_dir <- ihme.covid::get_output_dir(root = save_root, date = "today")
message(paste0("Saving to ", save_dir))
metadata_path <- file.path(save_dir, "01_data_prep.metadata.yaml")
loc_set_file <-  "FILEPATH/location_sets.csv"

# Location hierarchy
loc_sets <- fread(loc_set_file)

lsid <- loc_sets[note == 'use',ls]
lsvid <-loc_sets[note == 'use',lsv]
rid <- loc_sets[note == 'use',ri]
prod_lsid <- loc_sets[note == 'production',ls]
prod_lsvid <- loc_sets[note == 'production',lsv]
prod_rid <- loc_sets[note == 'production',ri]

hierarchy <- get_location_metadata(lsid,lsvid, release_id = rid) 

#---------Part 1-------------------------
#  Pull latest mandate data available
#----------------------------------------

# Resolve latest ETL version
etl_closure_path <- "FILEPATH/"
linked_version <- system(paste0('readlink -f ', etl_closure_path, "/latest"), intern = T)
data_ver <- gsub(etl_closure_path, "", linked_version)

data_date <- as.Date(gsub("_","-",paste0(strsplit(data_ver, "")[[1]][1:10],  collapse = "")))

# Read in detailed mandates 
df <- fread(file.path(etl_closure_path, data_ver, 'social_distancing/mandates_high_quality.csv'))
df <- df[, date := as.Date(date, format='%d.%m.%Y')]

# Define a list of locations with detailed mandate data 
location_list_mandates <- hierarchy[location_id %in% df$location_id, location_id]

# Check for locs in the hierarchy that are not in the mandate dataset
check_locs <- setdiff(hierarchy[most_detailed==1, location_id], df$location_id)
check_locs_names <- hierarchy[location_id %in% check_locs, location_name]
if(length(check_locs)>0){
  
  message(paste0("These locs are in hierarchy but not in mandate dataset: ",
              paste0(check_locs_names, collapse = ", ")))
}

# reference with the production hierarchy set 

# read in production location set, and drop all locations that have subnats in the set
prod_locs <- get_location_metadata(prod_lsid,prod_lsvid, release_id = prod_rid)
prod_parents_all <- unique(prod_locs$parent_id)
prod_parents <- prod_locs[location_id %in% prod_parents_all,]
prod_locs_modeled <- prod_locs[!(location_id %in% prod_parents$location_id),]

missing_from_prod <- setdiff(prod_locs_modeled$location_id,location_list_mandates)
if(length(missing_from_prod)>0){
  missing_from_prod_names <- prod_locs[location_id %in% missing_from_prod, location_name]
  stop(paste0("These locs are in the producation hierarchy but not in mandate dataset, and therefore will not be modeled - this will cause problem and should be addressed ASAP!: ",
                 paste0(missing_from_prod_names, collapse = ", ")))
}

# Reconstruct the coarse mandates from the detailed

# (a) Stay at home order (stay_home)
setnames(df, 'stay_at_home', 'stay_home')

# (b) School closures (educational_fac)
df[, educational_fac := ifelse(primary_edu==1 & secondary_edu==1 & higher_edu==1, 1, 0)]

# (c) Non-essential business closures (all_noness_business)
df[, all_noness_business := ifelse(gym_pool_leisure_close==1 & non_essential_retail_close==1 &
                                   non_essential_workplace_close==1 & dining_close==1 & bar_close==1, 1, 0)]

# (d) Any business closures (any_business)
df[, any_business := ifelse(gym_pool_leisure_close==1 | non_essential_retail_close==1 |
                                   non_essential_workplace_close==1 | dining_close==1 | bar_close==1, 1, 0)]

# (e) Gathering restrictions (any_gathering_restrict)
setnames(df, 'gatherings9998i9998o', 'any_gathering_restrict')

# Subset to vars of interest
mandate_indicator <- df[, .(location_id, date, stay_home, educational_fac, all_noness_business, any_gathering_restrict,
                            any_business)]

# Assume mandates hold steady for 7 days
max_date <- max(mandate_indicator$date)
one_day <- mandate_indicator[date==max_date]
one_day <- one_day[, date := date + 1]
for (i in 1:6){
  if(i==1){
    next_day <- copy(one_day)
    next_day <- next_day[, date := date + 1]
    one_week <- rbind(one_day, next_day)
  } else{
    next_day[, date := date + 1]
    one_week <- rbind(one_week, next_day)
  }
}
mandate_indicator <- rbind(mandate_indicator, one_week)

# Add placeholder rows for future dates
start_date <-  max(mandate_indicator$date) + 1
date_vec <- seq(as.Date(start_date), as.Date(end_date), by = 1)
future_dates <- as.data.table(expand.grid(date = date_vec, location_id = location_list_mandates))
mandate_indicator <- rbind(mandate_indicator, future_dates, fill=TRUE)
mandate_indicator <- mandate_indicator[order(location_id, date)]

# Add location names
mandate_indicator <- merge(mandate_indicator, hierarchy[,.(location_id, location_name)], by = "location_id", all.x = T)

# Disambiguate Georgia and Punjab
mandate_indicator[location_id == 35, location_name:= "Georgia (country)"]
mandate_indicator[location_id == 53620, location_name := "Punjab (Pakistan)"]

# Add ETL version
mandate_indicator[, etl_version := data_ver]

# Check that all expected locations are present
check_locs <- setdiff(location_list_mandates, mandate_indicator$location_id)
if(length(check_locs)>0){
  
  stop(paste0("These locs are in mandate data and hierarchy but not in processed dataset: ",
              paste0(check_locs, collapse = ",")))
  
}

# Output to csv
fwrite(mandate_indicator, paste0(save_dir, "/all_mandate_data_prepped.csv"))

#--------------------------
# Part 2 - add on latitude
#-------------------------

# Get latitude values 
lat_dt <- get_covariate_estimates(covariate_id = 56, 
                                  gbd_round_id = 6,
                                  decomp_step = "step4")

lat_dt <- lat_dt[, .(location_id, location_name, mean_value)]
setnames(lat_dt, "mean_value", "latitude_value")
lat_dt <- unique(lat_dt)

# Merge the covid location hierarchy with latitude 
lat_dt1 <- merge(hierarchy[,.(location_id, location_name, parent_id)], lat_dt, by = c("location_id", "location_name"), all.x = T)

# Fix locations that do not have a latitude value 
lat_na <- lat_dt1[is.na(latitude_value)]
parent_dt <- copy(lat_dt)
setnames(parent_dt, "location_id", "parent_id")
parent_dt$location_name <- NULL

lat_na$latitude_value <- NULL
lat_na1 <- merge(lat_na, parent_dt, by = "parent_id")

# Bind the abs latitude data tables together 
lat_dt1 <- lat_dt1[!is.na(latitude_value)]
lat_dt1 <- rbind(lat_dt1, lat_na1)
lat_dt1$parent_id <- NULL
setnames(lat_dt1, "latitude_value", "raw_latitude_value")
lat_dt1[, latitude_value := round(raw_latitude_value, 0)]

lat_dt1[, location_name :=NULL]

seasonality <- fread(paste0("FILEPATH/",pneumonia_version,"/pneumonia.csv"))
seasonality[, date:=as.Date(date)]
draw_cols <- grep("draw_", colnames(seasonality), value = T)
seasonality[, (draw_cols):=NULL]
seasonality <- merge(seasonality, lat_dt1)
seasonality <- seasonality[order(latitude_value)]

#find max day of mean_val
max_val <- seasonality[, max(value), by = location_id]
seasonality <- merge(seasonality, max_val, by = "location_id")
date_val <- seasonality[value == V1, .(date, location_id)]
date_val[, date:= date+1]
setnames(date_val, "date", "peak_date_plus1")

lat_dt1 <- merge(lat_dt1, date_val, by = "location_id")
lat_dt1[latitude_value <= -20, s_hemisphere := 1]
lat_dt1[is.na(s_hemisphere), s_hemisphere:=0]
lat_dt1[peak_date_plus1 <= as.Date(data_date) +7 | s_hemisphere==0, peak_date_plus1 := NA ]

lat_dt1 <- unique(lat_dt1)
lat_adj_locs <- lat_dt1[!is.na(peak_date_plus1)]
lat_adj_locs <- merge(lat_adj_locs, hierarchy[,.(location_id, location_name)], by = "location_id")
fwrite(lat_adj_locs, paste0(save_dir, "/latitude_adj_locs.csv"))

#-------------------------
# Combine measures & save
#-------------------------

both_data <- merge(lat_dt1, mandate_indicator, by = c("location_id"), all.y=T)
both_data[, date:=as.Date(date)]
fwrite(both_data, paste0(save_dir, "/all_data_prepped.csv"))

message(save_dir)


yaml::write_yaml(
  list(
    script = "01_data_prep.R",
    output_dir = save_dir,
    input_files = ihme.covid::get.input.files()
  ),
  file = metadata_path
)
