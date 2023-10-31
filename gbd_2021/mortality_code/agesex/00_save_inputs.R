# Save age sex inputs

rm(list=ls())
library(data.table); library(assertable); library(DBI); library(readr)
library(plyr); library(argparse)
library(mortdb, lib = "FILEPATH")

parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='Age sex estimate version id for this run')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='GBD Year')

args <- parser$parse_args()
version_id <- args$version_id
gbd_year <- args$gbd_year

birth_parent_version <- get_proc_lineage("age sex", "estimate", run_id = version_id)
birth_parent_version <- birth_parent_version[parent_process_name =="birth estimate" & exclude==0,
                                                                parent_run_id]

out_dir <- paste0("FILEPATH")

# HIV location information
hiv_location_information <- get_locations(level = "estimate", gbd_year = gbd_year, hiv_metadata = T)
write_csv(hiv_location_information, paste0("FILEPATH"))

# Locations for Stata scripts, including indicator variable for standard locations
locs_for_stata = get_locations(level = "estimate", gbd_year = gbd_year)
national_parents <- locs_for_stata[level==4,parent_id]
standard_locs <- unique(get_locations(gbd_type="standard_modeling",level="all")$location_id)
locs_for_stata[, standard:= as.numeric(location_id %in% c(standard_locs, national_parents, 44533))]
write_csv(locs_for_stata, paste0("FILEPATH"))

# Country plus locations for Stata scripts
cplus_locs = get_locations(gbd_year = gbd_year)
write_csv(cplus_locs, paste0("FILEPATH"))

## loading space time location hierarchy
st_locs <- get_spacetime_loc_hierarchy(prk_own_region = F, old_ap=F, gbd_year = gbd_year)
write_csv(st_locs, paste0("FILEPATH"))

# Live births
live_births_data <- get_mort_outputs(model_name = 'birth',
                                     model_type = 'estimate',
                                     run_id = birth_parent_version,
                                     age_group_id = 169,
                                     demographic_metadata = T)
live_births_data[,c("upload_birth_estimate_id",
                    "age_group_id") := NULL]

# Filter down to Telangana and Andhra Pradesh
temp_ap <- live_births_data[location_id %in% c(4841, 4871)]
 
index_cols <- c('run_id', 'location_id', 
                 'year_id', 'sex_id')
value_cols <- c("mean", "lower", "upper")
 
temp_ap[, location_id := 44849]
temp_ap[, location_name := "Old Andhra Pradesh"]
 
temp_ap[, (value_cols) := lapply(.SD, sum), .SDcols = value_cols, by = index_cols]
temp_ap <- unique(temp_ap, by = index_cols)
live_births_data <- rbind(live_births_data, temp_ap, fill=T)

# formatting
live_births_data <- live_births_data[order(location_id, year_id, sex_id),]
live_births_data[sex_id == 1, sex := "male"]
live_births_data[sex_id == 2, sex := "female"]
live_births_data[sex_id == 3, sex := "both"]
setnames(live_births_data, c("year_id", "mean"), c("year", "births"))
live_births_data <- live_births_data[,c("run_id","year","location_id",
                                        "ihme_loc_id","location_name",
                                        "sex","sex_id","births",
                                        "lower","upper")]

# save
write_csv(live_births_data, paste0("FILEPATH"))

# copy age-sex data inputs
data_version <- get_proc_lineage(
  "age sex",
  "estimate",
  run_id = version_id  
)
data_version <- data_version[parent_process_name == "age sex data", parent_run_id]

fs::file_copy(
  fs::path("FILEPATH"),
  fs::path("FILEPATH"),
  overwrite = TRUE
)

