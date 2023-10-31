## Purpose: Load and prep mobility data (Google Only)
## ----------------------------------------------------

# Load libraries
library(ggplot2)
library(data.table)
library('ihme.covid', lib.loc = 'FILEPATH')
source("FILEPATH/get_location_metadata.R")
R.utils::sourceDirectory('functions', modifiedOnly = FALSE)

# Set permissions
Sys.umask('0002')

# Global settings 
time_start_date <- as.Date("2020-01-01")
plot_bool <- TRUE  # If you want to plot along the way

# Define args if interactive. Parse args otherwise.
if (interactive()){
  code_dir <- "FILEPATH"
  model_inputs_version <- "latest" # model version for the new data
  friday <- FALSE 
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument("--code_dir", type = "character", help = "Root of repository.")
  parser$add_argument("--model_inputs_version", type = "character", help = "Version of model-inputs for the new data")
  parser$add_argument("--friday", type="character", choices = c('TRUE', 'FALSE'), help="If TRUE, will pull GPR raw data from /J: drive instead of /share drive.")

  args <- parser$parse_args()
  for (key in names(args)) {
    assign(key, args[[key]])
  }
  
  # Update logical variables
  friday = as.logical(friday)
  
  print("Arguments passed were:")
  for (arg in names(args)){
    print(paste0(arg, ": ", get(arg)))
  }
}

# location set settings
loc_set_file <-  "FILEPATH/location_sets.csv"

loc_sets <- fread(loc_set_file)

lsid <- loc_sets[note == 'use',ls]
lsvid <-loc_sets[note == 'use',lsv]
rid <- loc_sets[note == 'use',ri]

lsid.for.pub <-  loc_sets[note == 'production',ls]
lsvid.for.pub <- loc_sets[note == 'production',lsv] # All of these locations will be plotted for vetting
rid.for.pub <- loc_sets[note == 'production',ri]

# Define dirs
output_dir <- "FILEPATH" 
central_root <- "FILEPATH"
root <- paste0(central_root,"/",model_inputs_version,"/mobility/")

# Create a new output directory
output_subdir <- ihme.covid::get_output_dir(
  root = output_dir,
  date = "today"
)
message(paste0("Creating output directory here: ", output_subdir))
dir.create(output_subdir,showWarnings = F)

# Define metadata path
metadata_path <- file.path(output_subdir, "01_load_data.metadata.yaml")

## PART 1: prep location sets -----------------------------------------------------------------
# Load location sets
locs <- get_location_metadata(location_set_id=lsid, location_set_version_id = lsvid, release_id = rid)
locs_for_pub <- get_location_metadata(lsid.for.pub, location_set_version_id = lsvid.for.pub, release_id = rid.for.pub)

# if there are some locations missing from the super set (there shouldn't be) add them in
if(length(locs_for_pub[!(location_id %in% locs$location_id), location_name])>0){
  warning(paste("These locs are not in the superset and are being added: \n", paste(locs_for_pub[!(location_id %in% locs$location_id), location_name], collapse = "\n")))
  locs <- rbind(locs, locs_for_pub[!(location_id %in% locs$location_id)], fill = T)
}

# write locations out for future use
write.csv(locs, file.path(output_subdir, "locs.csv"), row.names = F)
write.csv(locs_for_pub, file.path(output_subdir, "locs_for_pub.csv"), row.names = F)

# source custom functions (requires locs to be defined)
source(paste0(code_dir,"/mobility/gpr/custom_functions.R"))


## PART 2: load data -----------------------------------------------------------------

# If there's new data available that hasn't been captured by ETL, set friday==T
# in order to read directly from J: drive instead of /share drive
if(friday==F){
  google <- fread(file.path(root,"google_mobility_with_locs.csv"))
} else{
  google <- fread('FILEPATH/google_mobility_with_locs.csv')
}

# If Hong Kong is being modeled as a subnat of China, change the ISO code to reflect that
hk_loc_info <- locs[location_id == 354,]
if(hk_loc_info$parent_id == 6){
  google[location_id == 354, ihme_loc_id := 'CHN_354']
}

google <- google[sub_region_1 == 'State of Rio de Janeiro' & sub_region_2 == "Rio de Janeiro", drop := 1]
google <- google[is.na(drop),]
google[,drop := NULL]

# read in outliers flat file
outliers <- fread(file.path(output_dir, "outlier_locs.csv")) # In this file we mark locations where we have outliered an entire source
write.csv(outliers, file.path(output_subdir, "outlier_locs.csv"), row.names = F)


## PART 3: prep Google -----------------------------------------------------------------

# Define vars of interest
google[, change_from_normal := mean_percent_change]
google <- google[,.(ihme_loc_id, date, change_from_normal)]

# Drop missing location_ids
missing_rows <- nrow(google[is.na(ihme_loc_id)])
if(missing_rows > 0){
  warning(paste0("Google has ", missing_rows, " rows without location_id mapped."))
  google <- google[!is.na(location_id)]
}

# Define source and date
google$source <- "GOOGLE"
google[,date := paste(tstrsplit(date,"[.]")[[3]],tstrsplit(date,"[.]")[[2]],tstrsplit(date,"[.]")[[1]],sep="-")]
google[,date := as.Date(date)]

# Flag duplicates (if any)
if(nrow(unique(google))<nrow(google)){
  warning("google dataset is not unique.")
  google <- unique(google)
}

# Calculate rolling mean if there are at least 7 data points for a given location
google[,count := .N, by="ihme_loc_id"]
google_7 <- google[count >= 7]
google_und_7 <- google[count < 7]
# If cannot perform a rolling mean because not enough data, make the average equal to the data point:
google_und_7[,change_from_normal_avg := change_from_normal]
google_7 <- rolling_fun_simple(google_7,7)
google <- rbind(google_7,google_und_7,fill=T)

print(paste0("Google latest date: ", max(google$date)))

## Add in Baidu! ----------------------------------------------------------
baidu <- fread(paste0(root, 'baidu_mobility.csv'))

setnames(baidu,"mean_percent_change","change_from_normal")
baidu <- merge(baidu,unique(locs[,.(location_id,ihme_loc_id)]),by="location_id",all.x=T)
baidu[,date := paste(tstrsplit(date,"[.]")[[3]],tstrsplit(date,"[.]")[[2]],tstrsplit(date,"[.]")[[1]],sep="-")]
baidu[,date := as.Date(date)]

## DEAL WITH DUPLICATES
if(nrow(unique(baidu))<nrow(baidu)){
  warning("Baidu dataset is not unique.")
  baidu <- unique(baidu)
}

baidu <- rolling_fun_simple(baidu,7)
baidu$source <- "GOOGLE"
baidu <- baidu[,.(ihme_loc_id, date, change_from_normal, change_from_normal_avg, source)]

baidu <- baidu[date >= as.Date('2020-01-01'),]

print(paste0("Baidu latest date: ", max(baidu$date)))

google <- rbind(google, baidu, fill = T)

## PART 4: plot -----------------------------------------------------------------

# subset date range
all_mobility_decrease <- google[date>=time_start_date]

# retrieve location name and parent name
all_mobility_decrease <- merge(all_mobility_decrease, locs[,.(ihme_loc_id, location_name, parent_id)], by= "ihme_loc_id")
all_mobility_decrease <- merge(all_mobility_decrease, locs[,.(parent_id = location_id, location_parent_name = location_name)], by = "parent_id", all.x=T)
all_mobility_decrease <- setorderv(all_mobility_decrease, cols=c("location_parent_name", "ihme_loc_id"))

# drop outlier locations
outliers[,outlier:=1]
all_mobility_decrease <- merge(all_mobility_decrease, outliers, by=c("ihme_loc_id", "source"), all.x=T)
all_mobility_decrease <- all_mobility_decrease[is.na(outlier)]
all_mobility_decrease[, c("Notes", "outlier") := NULL]

# subset to locations in the covid covariates hierarchy
all_mobility_decrease <- all_mobility_decrease[ihme_loc_id %in% locs$ihme_loc_id]

# output csv
write.csv(all_mobility_decrease, file.path(output_subdir, "time_series_raw_with_rolling_mean.csv"), row.names = F)

# plot the data
if(plot_bool){
  global_path_plot <- paste0(output_subdir,"/time_series_raw_with_rolling_mean.pdf")
  time_series_raw_data(global_path_plot,all_mobility_decrease, loc_list = unique(locs_for_pub$ihme_loc_id))
}


yaml::write_yaml(
  list(
    script = "01_load_data.R",
    location_set_version_id = lsvid,
    output_dir = output_subdir,
    input_files = ihme.covid::get.input.files()
  ),
  file = metadata_path
)
