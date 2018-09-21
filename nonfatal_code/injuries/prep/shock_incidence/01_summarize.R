############################################################
############################################################
## Author: USERNAME
## Date: DATE
## Purpose: Summarize the ratios by calculating them over
## super regions
############################################################
############################################################

# clear workspace environment
rm(list = ls())

if(Sys.info()[1] == 'Windows') {
  username <- "FILEPATH"
  root <- "FILEPATH"
  code_dir <- paste0("FILEPATH")
} else {
  username <- "USERNAME"
  root <- "FILEPATH"
  code_dir <- "FILEPATH"
}

pacman::p_load(data.table, magrittr)

run_date <- "DATE"

# source the central functions
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")

# define directories
in_dir <- "FILEPATH"
out_dir <- "FILEPATH"

# get shared functions
source(paste0(root, "FILEPATH.R"))
source(paste0(root, "FILEPATH.R"))
source(paste0(root, "FILEPATH.R"))

# get demographic information to pull from
dems <- get_demographics(gbd_team = "epi")
location_ids <- dems$location_ids
sex_ids <- dems$sex_ids

# get location information
metaloc <- get_location_metadata(location_set_id = 2)
supers <- metaloc[!is.na(super_region_id), super_region_id] %>% unique

#############################################################
# IMPORTANT: SETTINGS OF THIS RUN, LINE UP WITH MASTER SCRIPT
#############################################################

testing = FALSE
local = FALSE
paste0("testing is ", testing, " and local is ", local)

if(!local){
  
  # get job array task ID and find corresponding parameter values
  task_id <- Sys.getenv("SGE_TASK_ID") %>% as.numeric
  cat("This task_id is ", task_id)
  
  if(!testing){
    
    # set parameters
    metaloc <- get_location_metadata(location_set_id = 2)
    supers <- metaloc[!is.na(super_region_id), super_region_id] %>% unique %>% as.numeric
    
    super_id <- supers[task_id]
    
  } else {
    
    # settings for if we are testing - only submitting two jobs from the master script
    super_id <- 64 
  }
  
} else {
  
  # for local work only - only running one job
  
  super_id <- 4
}

## Get options for the inputs

meta <- copy(metaloc)
locations <- meta[super_region_id == super_id & location_id %in% dems$location_ids, location_id] %>% unique %>% as.numeric

combos <- expand.grid(locations, dems$sex_ids)

files_want <- paste0(combos[,1], "_", combos[,2], "FILEPATH.csv")

files <-  list.files(in_dir)[list.files(in_dir) %in% files_want]

path <- paste0(in_dir, "/", files)

## read in all of the data at once
datalist <- lapply(setNames(path, make.names(paste0("ratios_", gsub("FILEPATH.csv", "", files)))), 
                fread)

data <- rbindlist(datalist, use.names = TRUE)

# perform collapsing operations on the data
draws <- paste0("ratio_", 0:999)

data <- data[, lapply(.SD, mean), .SDcols = draws, by = c("location_id", "inpatient", "age_group_id", "ecode")]

locs <- get_location_metadata(location_set_id = 2)
locs <- locs[, list(location_id, super_region_id, super_region_name)]

alldata <- merge(data, locs, by = "location_id")

alldata <- alldata[, lapply(.SD, mean), .SDcols = draws, by = c("super_region_name", "inpatient", "age_group_id", "ecode")]

filename <- paste0(super_id, "_compiled.csv")

# write draws file
cat("Writing the draws file to ", "FILEPATH")
write.csv(alldata, paste0(out_dir, "/FILEPATH/", filename), row.names = FALSE)

# perform summary operations and write to summary directory
alldata[, mean := apply(alldata[ , draws, with = F], 1, mean)]
alldata[, ll := apply(alldata[ , draws, with = F], 1, quantile, probs = c(0.025))]
alldata[, ul := apply(alldata[ , draws, with = F], 1, quantile, probs = c(0.975))]

# keep only summary variables
alldata = alldata[ ,c("super_region_name", "inpatient", "age_group_id", "ecode", "mean", "ll", "ul"), with = F]

# write data to out directory
cat("Writing the summary file to ", paste0(out_dir, "/FILEPATH/", filename))
write.csv(alldata, paste0(out_dir, "/FILEPATH/", filename), row.names = FALSE)






