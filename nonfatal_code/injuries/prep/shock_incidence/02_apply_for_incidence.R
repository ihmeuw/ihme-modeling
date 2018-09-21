############################################################
############################################################
## Author: USERNAME
## Date: DATE
## Purpose: Apply the incidence:mortality ratios
############################################################
############################################################

# clear workspace environment
rm(list = ls())

if(Sys.info()[1] == 'Windows') {
  username <- "USERNAME"
  root <- "J:/"
  code_dir <- paste0("FILEPATH")
} else {
  username <- "USERNAME"
  root <- "FILEPATH"
  code_dir <- "FILEPATH"
}

pacman::p_load(data.table, magrittr, readstata13)

run_date <- "DATE"

# source the central functions
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")

# define directories
shock_dir <- "FILEPATH"
ratio_dir <- "FILEPATH"

out_dir <- "FILEPATH"

# get shared functions
source("FILEPATH")

# get demographic information to pull from
dems <- get_demographics(gbd_team = "epi")
location_ids <- dems$location_ids
sex_ids <- dems$sex_ids

cod <- get_demographics(gbd_team = "cod")
cod_years <- cod$year_ids

# get location information
metaloc <- get_location_metadata(location_set_id = 35)
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
    
    # function to generate parameters from the task_id
    gen_parameters <- function(task_id){
      param_map <- expand.grid(location_ids, sex_ids)
      params <- param_map[task_id,]
      return(list(location_id = params[, 1], sex_id = params[, 2]))
    }
    
    # set parameters
    parameters <- gen_parameters(task_id)
    location_id <- parameters$location_id
    sex_id <- parameters$sex_id
    
  } else {
    
    # settings for if we are testing - only submitting two jobs from the master script
    location_id <- 101
    sex_id <- sex_ids[task_id]
  }
  
} else {
  
  # for local work only - only running one job
  
  location_id <- 44550
  sex_id <- 1
}

#######################################
## COMPUTATION
#######################################

#######################################
## For a given location and sex:
##  1. Grab the super-region that it's associated with
##  2. Grab the shock mortality for each of the 3 shock causes
##      for the specific location-sex pair
##  3. Check to make sure that there was a shock mortality file for that cause
##  4. If there WASN'T, then append on the "blank" files that will be created
#######################################

########### MORTALITY

shock_codes <- c("inj_war_execution", "inj_war_warterror", "inj_disaster")

mort_files <- paste0("shockmort_", location_id, "_", sex_id, ".csv")

# create a blank dataset to merge on if we don't have the shock data for this location, type, sex...
blank_params <- expand.grid(age_group_id = dems$age_group_ids, 
                            year_id = cod_years)

blank <- data.table(location_id = rep(location_id, n = nrow(blank_params)),
                    sex_id = rep(sex_id, n = nrow(blank_params)),
                    blank_params)

draws <- paste0("draw_", 0:999)
blank[, (draws) := 0]

# define function to pull in the data from each of the files
# or add the blank data table if the file doesn't exist

grabdata <- function(shock_code){
  
  file <- paste0(shock_dir, "/FILEPATH/", shock_code, "/", mort_files)
  
  # if the file exists, pull in the data

  if(file.exists(file)){
    
    data <- fread(file)

    } else {
    
    # if the file doesn't exist, need to add the blank files
    # and create a column for the ecode
    print(paste0("File ", file, " does not exist"))
    data <- copy(blank)
    data[, ecode := shock_code]
  }
  
  return(data)
}

# get all shock data
shock_data <- lapply(shock_codes, grabdata) %>% rbindlist

######### RATIOS

# pull the super-region id
loc <- location_id
super_region_id <- metaloc[location_id == loc, super_region_id] %>% unique %>% as.numeric

ratio <- paste0(ratio_dir, "/", super_region_id, "_compiled.csv")

ratios <- fread(ratio)

# determine which ecodes will be used to impute the shock incidence

relationships <- "FILEPATH" %>% fread
relationships <- melt(relationships, id.vars = "imputed_ecode")

# get disaster specifications for inj_disaster by year and loc_id
emdat <- read.dta13("FILEPATH.dta") %>% data.table

# define function to compute incidence for a shock code and a platform
water_causes <- c("Riverine flood", "Flash flood", "Water", "Tropical cyclone", "Tsunami",
                  "Coastal flood", "Flood", "Extra-tropical storm", "Other hydrological")

# get the years that had a water disaster
water_years <- emdat[cause %in% water_causes & location_id == loc, year] %>% unique %>% as.numeric

# define function to impute the incidence for each shock code and platform
impute <- function(shock_code, platform){
  
  # subset data
  data <- shock_data[ecode == shock_code]
  
  # get imputation codes
  impute_from <- relationships[imputed_ecode == shock_code, value] %>% unique
  
  ratio_data <- ratios[ecode %in% impute_from & inpatient == platform]
  
  ratio_draws <- paste0("ratio_", 0:999)
  
  if(shock_code == "inj_disaster" & length(water_years != 0)){
    
    # get the years where there were water and non-water
    # disaster types
    water <- data[year_id %in% water_years]
    nonwater <- data[!year_id %in% water_years]
    
    # get the ratios to apply to the years that have water
    water_ratios <- ratio_data[, lapply(.SD, mean),
                               .SDcols = ratio_draws, by = c("age_group_id")]
    
    # get the ratios to apply to the years that do not have water
    nonwater_ratios <- ratio_data[ecode != "inj_drowning", lapply(.SD, mean),
                                  .SDcols = ratio_draws, by = c("age_group_id")]
    
    # apply the water ratios to the water years, and the 
    # nonwater ratios to the nonwater years
    allwater <- merge(water, water_ratios, by = c("age_group_id"))
    allnonwater <- merge(nonwater, nonwater_ratios, by = c("age_group_id"))
    
    # bind the data together
    alldata <- rbindlist(list(allwater, allnonwater), use.names = TRUE)
    
  } else {
    
    # take away the drowning code for disaster if there aren't any years associated with water
    if(shock_code == "inj_disaster" & length(water_years == 0)) ratio_data <- ratios[ecode != "inj_drowning"]
    
    # get the ratio data and average over the ecodes
    ratio_data <- ratio_data[, lapply(.SD, mean), .SDcols = ratio_draws, by = c("age_group_id")]
    ratio_data <- ratio_data[, c("age_group_id", ratio_draws), with = F]
    
    alldata <- merge(data, ratio_data, by = c("age_group_id"))
    
  }
  
  # multiply draw by ratio draw to get incidence
  merged <- melt(alldata, id = 1:4, measure = patterns("^draw_", "^ratio_"), variable.factor = FALSE)
  merged[, incidence := value1 * value2]
  
  # rename variables and reshape
  merged <- merged[, list(age_group_id, location_id, year_id, sex_id, variable, incidence)]
  setnames(merged, "variable", "draw")
  merged[, draw := paste0("draw_", as.numeric(draw) - 1)]
  merged[incidence > 0.5, incidence := 0.5]
  
  merged <- dcast(merged, age_group_id + location_id + year_id + sex_id ~ draw, value.var = "incidence")
  merged[, ecode := shock_code]
  merged[, inpatient := platform]
  
  # return the final data which is the merged, recasted draw data table
  return(merged)
}

# set parameters for the function to loop over

platforms <- c(0, 1)
params <- expand.grid(shock_codes, platforms)
setnames(params, c("shock_code", "platform"))

# apply impute() over the parameter grid
result <- apply(params, 1, function(x) do.call(impute, as.list(x))) %>% rbindlist(use.names = TRUE)

#############################
#############################
## SAVE THE FINAL OUTPUT IN
## BOTH DRAWS AND SUMMARY
## FILES
#############################
#############################

for(platform in c("inp", "otp")){
  if(platform == "inp") platnum <- 1
  if(platform == "otp") platnum <- 0
  
  subset <- result[inpatient == platnum]
  
  filename <- paste0("FILEPATH", location_id, "_", platform, "_", sex_id, ".csv")
  filepath <- paste0(out_dir, "/FILEPATH/", filename)
  
  # write the draws file
  cat("Writing draws file to ", filepath, "\n")
  write.csv(subset, filepath, row.names = FALSE)
  
  # perform summary operations and write to summary directory
  subset[, mean := apply(subset[, draws, with = F], 1, mean)]
  subset[, ll := apply(subset[, draws, with = F], 1, quantile, probs = c(0.025))]
  subset[, ul := apply(subset[, draws, with = F], 1, quantile, probs = c(0.975))]
  
  # keep only the summary variables
  subset <- subset[, c("inpatient", "ecode", "age_group_id", "location_id", "sex_id", "year_id", "mean", "ll", "ul"),
                   with = F]
  
  # define summary filepath
  filepath <- paste0(out_dir, "/FILEPATH/", filename)
  
  # write the summary file
  cat("Writing summary file to ", filepath, "\n")
  write.csv(subset, filepath, row.names = FALSE)
}










