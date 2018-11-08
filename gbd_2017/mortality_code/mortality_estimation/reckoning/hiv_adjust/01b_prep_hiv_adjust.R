## Create HIV adjustment summary results
rm(list=ls())
library(readr); library(data.table); library(foreign); library(reshape2); library(haven); library(assertable)

## Setup filepaths
root <- "ROOT_FILEPATH"
user <- "USERNAME"

location <- commandArgs(trailingOnly = T)[1]
new_upload_version <- commandArgs(trailingOnly = T)[2]
start_year <- as.integer(commandArgs(trailingOnly = T)[3])
gbd_year <- as.integer(commandArgs(trailingOnly = T)[4])

years <- c(start_year:gbd_year)

master_dir <- paste0("/FILEPATH/hiv_adjust/",new_upload_version)
inputs_dir <- paste0(master_dir, "/inputs")
in_dir_hiv <- paste0(master_dir,"/hiv_output")
results_dir <- paste0(master_dir,"/results")

## Grab functions to aggregate everything
library(mortdb)
library(mortcore)

## Specify subnationals that you need to aggregate
locations <- data.table(fread(paste0(inputs_dir, "/locations_all.csv")))
loc_name <- unique(locations[location_id == location, ihme_loc_id])
lowest <- data.table(fread(paste0(inputs_dir, "/locations_lowest.csv")))

## If it's a national (not SDI), find children through getting lowest locations
if(location %in% unique(locations$parent_id)) {
  file_countries <- lowest[grepl(paste0(",", location, ","), path_to_top_parent), ihme_loc_id]
  reckon_data <- import_files(filenames = paste0(in_dir_hiv,"/reckon_reporting_", file_countries, ".csv"),
               multicore = T,
               mc.cores = 5)
} else {
  ## Otherwise, just import the country itself
  file_countries <- lowest[location_id == location, ihme_loc_id]
  reckon_data <- fread(paste0(in_dir_hiv,"/reckon_reporting_", file_countries, ".csv"))
}


## Collapse to total deaths across all locations and to both sexes (already in number-space, so just need to collapse)
collapse_vars <- c("value")
id_vars <- c("location_id", "sex_id","year_id","age_group_id","measure_type")

reckon_data <- agg_results(reckon_data,
                           value_vars = "value",
                           id_vars = c(id_vars, "sim"),
                           age_aggs = "",
                           loc_scalars = F,
                           agg_hierarchy = T,
                           end_agg_level = 3,
                           agg_sdi = F,
                           tree_only = loc_name,
                           agg_sex = T,
                           gbd_year=gbd_year)


## Collapse to mean, lower, and upper
mean_vals <- data.table(reckon_data)[,lapply(.SD,mean),.SDcols=collapse_vars,
                                       by=id_vars]
setnames(mean_vals,c("value"),c("mean"))

lower_vals <- data.table(reckon_data)[,lapply(.SD, quantile, probs=.025, na.rm=TRUE),.SDcols=collapse_vars,
                                     by=id_vars]
setnames(lower_vals,c("value"),c("lower"))

upper_vals <- data.table(reckon_data)[,lapply(.SD, quantile, probs=.975, na.rm=TRUE),.SDcols=collapse_vars,
                                     by=id_vars]
setnames(upper_vals,c("value"),c("upper"))


## Combine and output data
output <- merge(mean_vals,lower_vals,by=id_vars)
output <- merge(output,upper_vals,by=id_vars)

assert_values(output[year_id >= 1954,], colnames(output), "not_na")
id_vars <- list(location_id=unique(output$location_id), sex_id = c(1:3), year_id = years, age_group_id = c(1, 23, 24, 40), measure_type = c("env_post", "env_pre", "hiv_post", "hiv_pre_env", "hiv_pre_oth"))
assert_ids(output, id_vars = id_vars)

output[, run_id:=new_upload_version]
write_csv(output, paste0(results_dir,"/results_",location,".csv"))



