
### ---------------------------------------------------------------------------------------- ######
rm(list=ls())

library(data.table)
library(readxl)
library(openxlsx)
library(dplyr)
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/save_crosswalk_version.R")

# 0. Settings ---------------------------------------------------------------------------
bundle_id <- ADDRESS
gbd_year <- ADDRESS
release_id <- ADDRESS
################################################################################
### update data date
map_data_date <- "ADDRESS" 
map_version <- "ADDRESS"
date <- gsub("-", "_", Sys.Date())
run_folder_date <- "ADDRESS" 

################################################################################
bundle_data_name <- "ADDRESS"
xwalk_description <- "ADDRESS"

path <- paste0(FILEPATH)
parent_dir <- paste0(FILEPATH)

path_to_bundle <- paste0(FILEPATH)
interm_path <- paste0(FILEPATH)
  
output_dir <- paste0(FILEPATH)
draw_path <- paste0(FILEPATH)
  
dir.create(output_dir, recursive = T, showWarnings = F) 
dir.create(interm_path, recursive = T, showWarnings = F) 
dir.create(draw_path, recursive = T, showWarnings = F) 

# 1. need to run checks ---------------------------------------------------------------------------
path_to_prev <- paste0(path_to_bundle, bundle_data_name)
  
bundle <- fread(path_to_prev)
  
write.xlsx(bundle,
           file = paste0(interm_path,'bundle_malaria_data.xlsx'),
           row.names = FALSE,
           na = "")
write.csv(bundle,
           file = paste0(interm_path,'bundle_malaria_data.csv'),
           row.names = FALSE,
           na = "")
write.xlsx(bundle, file = paste0("FILEPATH/bundle_", date,".xlsx"),
           row.names = FALSE,
           sheetName = "ADDRESS",
           na = "")


    message("Correcting incidence draws")
    path_to_inc <- paste0(path_to_bundle, map_data_date)
    files <- list.files(path_to_inc, full.names = TRUE)
    bundle_inc <- NULL
    for (f in files){
      temp_df <- fread(f)
      bundle_inc <- rbind(bundle_inc, temp_df)
    }
    
        
    for (i in unique(bundle_inc$location_id)){
      print(paste(which(unique(bundle_inc$location_id) == i), "of", length(unique(bundle_inc$location_id))))
      write.csv(format(bundle_inc[bundle_inc$location_id == i,], nsmall = 2), 
                file = file.path(draw_path,paste(i,".csv", sep = "")),
                row.names = FALSE)
    }
}

rm(bundle, bundle_inc, f, files, temp_df, params, eth)
# 1.5 Upload bundle data ---------------------------------------------------------------------------
temp_bundle <- get_bundle_data(bundle_id)
write.xlsx(temp_bundle, file = paste0("FILEPATH/bundle_", date,".xlsx"),
           row.names = FALSE,
           sheetName = "ADDRESS",
           na = "")

rm(temp_bundle)

################################################################################
### save bundle version below and update so the right version is pulled for xwalking
################################################################################
result <- save_bundle_version(bundle_id)
result 
bundle_version_id <- ADDRESS  

################################################################################
## download the bundle and then format it for crosswalking
bundle_version_df <- get_bundle_version(bundle_version_id, fetch = "all")

# 2. Format data for crosswalking ---------------------------------------------------------------------------
## Call/create age metadata table
all_fine_ages <- as.data.table(get_age_metadata(release_id = release_id))
ages <- subset(all_fine_ages, age_group_id>2)

ages[age_group_id == 388, age_group_years_end := 0.50136986]
ages[age_group_id == 389, age_group_years_start := 0.50136986]

# create a new column for age_group_year needed where for any age group over 4 take one off the age end
ages[age_group_id >4& age_group_id <236, age_group_years_end := age_group_years_end-1]
ages[age_group_years_end==124, age_group_years_end := 99]

## early neonates protected by mother so drop these, they can't get malaria
ages <- subset(ages, age_group_id>2)
setnames(ages, c("age_group_years_end", "age_group_years_start"), c("age_end", "age_start"))

## Input data is bundle 
data <- bundle_version_df

## subset input data to incidence data only; then modify age-start and age-end to merge with age metadata 
inc <- subset(data, measure=="incidence") 

nrow(inc)
inc <- merge(inc, ages, by = c("age_start", "age_end"))
nrow(inc)

inc <- subset(inc, mean >0.0000000000001)
inc_emr <- inc[, c("location_id", "sex", "year_end", "year_start", "age_group_id", "age_start", "age_end")]
inc_emr_vector <- unique(inc$location_id)

## subset input data to emr data only; the main task is to trim EMR data in locations where incidence data are not present  
emr <- subset(data, measure=="mtexcess" ) 

emr <- subset(emr, age_end<104)

emr$age_end[emr$age_end==100] <-99.999
emr[age_end >0.999, age_end := age_end-0.999]
emr_subset <- emr[emr$location_id %in% inc_emr_vector, ] ##subsetting to locations with incidence data

emr_old <-subset(emr_subset, age_start >19.00)
nrow(emr_old)
emr_old <- merge(emr_old, ages, by = c("age_start", "age_end"))
nrow(emr_old)

emr_young <- subset(emr_subset, age_start <20)

nrow(emr_young)
# will lose the rows with the early neonatal data but this is ok
# early neonates are protected by mother, they can't get malaria
emr_young <- merge(emr_young, ages, by = c( "age_start", "age_end"))
nrow(emr_young)

complete_processed<- rbind(inc, emr_young,emr_old, fill=TRUE)

complete_processed$crosswalk_parent_seq <- complete_processed$seq

write.csv(complete_processed, paste0(output_dir, "xwalk_version_", date, ".csv"), row.names = F)
write.xlsx(complete_processed, paste0(output_dir, "xwalk_version_", date, ".xlsx"), sheetName = "ADDRESS", col.names=TRUE)

complete_processed_only_mtxcess <- subset(complete_processed, measure != "incidence")
incidence_data <- subset(complete_processed, sex == "Both")

incidence_data_men <- incidence_data
incidence_data_men$sex <- "Male"
incidence_data_men$seq <- "NULL"

incidence_data_female <- incidence_data
incidence_data_female$sex <- "Female"
incidence_data_female$seq <- "NULL"

## combine the three files
xwalk_sex_split <- rbind(complete_processed_only_mtxcess, incidence_data_men, incidence_data_female)

write.csv(xwalk_sex_split, paste0(output_dir, 'xwalk_', date, '.csv'), row.names= FALSE)
write.xlsx(xwalk_sex_split, paste0(output_dir, "xwalk_", date, ".xlsx"), sheetName = "ADDRESS", col.names=TRUE)

# 3. Upload crosswalking file ---------------------------------------------------------------------------
result <- save_crosswalk_version(
  bundle_version_id=bundle_version_id,
  data_filepath= paste0(output_dir, "xwalk_", date, ".xlsx"),
  description=xwalk_description)
result

