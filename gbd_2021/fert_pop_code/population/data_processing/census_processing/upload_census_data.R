####################################################################################################
## Author:       Paulami Naik
##
## Description: format processed census data for upload to the database
####################################################################################################

rm(list=ls())
root <- ifelse(Sys.info()[1]=="FILEPATH")

library(assertable)
library(data.table)
library(mortdb)


pop_dir <- "FILEPATH"
data_dir <- "FILEPATH"
db_dir <- "FILEPATH"
shared_functions_dir <- "FILEPATH"


#get location ids
source(paste0(shared_functions_dir, "get_ids.R"))
locations = get_ids("location")
source(paste0(shared_functions_dir, "get_location_metadata.R"))
location_hierarchy = get_location_metadata(location_set_id=21, gbd_round_id=5)
location_hierarchy <- location_hierarchy[,.(ihme_loc_id,location_id, level, parent_id, region_name, location_type, local_id)]

# get age_group_ids
age_groups = get_ids("age_group")
age_groups[,age_group_name:=trimws(age_group_name)]


#input data
data <- fread(paste0(data_dir, version,"/data/compiled.csv"))

#only keep the data steps that we upload  
data <- data[data_step %in% c("raw","distributed_unknown","unheaped","baseline")]

#drop data where the mean sums to 0 
summed_data <- data[, list(sum = sum(pop)), by = c("ihme_loc_id","year_id", "source", "nid","underlying_nid", "record_type", "data_step")]
data <- merge(data, summed_data, by = c("ihme_loc_id","year_id", "source", "nid","underlying_nid", "record_type", "data_step"), all.x = T  )
data <- data[sum > 2,]

#add data_stage_id.-----------------------------------------------------------------------------------------------------------------------------------------
data[data_step == "raw", data_stage_id := 3]
data[data_step == "raw" & ihme_loc_id == "ZAF" & year_id == 2011, data_stage_id := 2]
data[data_step == "distributed_unknown", data_stage_id := 4]
data[data_step == "unheaped", data_stage_id := 5]
data[data_step == "baseline", data_stage_id := 6]



data <- rbindlist(list(data, data_underlying_nids_added), use.names = T, fill = T)




# Checks for data ---------------------------------------------------------


# check for duplicates marked best
check_duplicates <- data[status == "best"]
check_duplicates[, data_points := .N, by = c("ihme_loc_id", "year_id","data_stage_id","age_group_id", "sex_id","outlier_type_id")]
if (nrow(check_duplicates[data_points > 1]) > 0) {
  print(check_duplicates[data_points > 1])
  stop("duplicates marked best")
}




census_data <- data[, list(       location_id = as.integer(location_id),
                                  year_id = as.integer(year_id),
                                  sex_id = as.integer(as.character(sex_id)),
                                  age_group_id = as.integer(age_group_id),
                                  nid = as.integer(nid),
                                  underlying_nid = as.integer(underlying_nid),
                                  census_midpoint_date,
                                  record_type_id = as.integer(record_type_id),
                                  data_stage_id = as.integer(data_stage_id),
                                  outlier_type_id = as.integer(outlier_type_id),
                                  sub_agg = as.integer(sub_agg),
                                  source = substr(source,1,49),
                                  mean = as.numeric(mean),
                                  notes = substr(notes,1,149))]



# assert
assert_values(census_data, colnames=c("location_id", "year_id", "sex_id", "age_group_id","data_stage_id", "outlier_type_id", "record_type_id", "mean", "source"), test="not_na")
assert_values(census_data, colnames="sex_id", test="in", test_val = c(4, 1, 2))
assert_values(census_data, colnames="data_stage_id", test="in", test_val = c(1:6))
assert_values(census_data, colnames="outlier_type_id", test="in", test_val = c(1:5))
assert_values(census_data, colnames="record_type_id", test="in", test_val = c(1:6))
assert_values(census_data, colnames="mean", test="gte", test_val = 0)
write.csv(census_data, file = paste0(data_dir, version, "/upload_census_data.csv"), row.names = F)

census_data_version <-gen_new_version(model_name = "census",
                                     model_type = "data",
                                     comment = "gbd2017",
                                     gbd_year = 2017)


write.csv(census_data,paste0(root, "FILEPATH", census_data_version, ".csv"), row.names = F)
upload_results(filepath = paste0(root, "FILEPATH", census_data_version, ".csv"),
               model_name = "census",
               model_type = "data",
               run_id = census_data_version,
               check_data_drops = FALSE)

#Change the model status to "best"
update_status(model_name = "census",
              model_type = "data",
              run_id = census_data_version,
              new_status = "best",
              new_comment = "corrected 0 pops and gbd year")




