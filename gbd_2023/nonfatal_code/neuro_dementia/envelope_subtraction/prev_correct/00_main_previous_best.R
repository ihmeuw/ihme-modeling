#######################################################################################
### Author: USERNAME, updated by USERNAME to pull in 2019 best results
### Date: 04/05/2019, updated April 2020
### Project: GBD Nonfatal Estimation
### Purpose: Main Script for Prevalence from Previous Round Best Models
#######################################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH" 
  h <- "FILEPATH"
  l <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
  functions_dir <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
library(mortcore, lib = "FILEPATH")
date <- gsub("-", "_", Sys.Date())

# SET UP OBJECTS ----------------------------------------------------------

code_dir <- paste0("FILEPATH")
functions_dir <- paste0("FILEPATH")
save_dir <- "FILEPATH"

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_location_metadata.R"))

# GET LOCATIONS -----------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = ID, gbd_round_id=ID)
loc_dt <- loc_dt[is_estimate == 1 & most_detailed == 1]
params <- data.table(location = loc_dt[, location_id])
params[, task_num := 1:.N]
map_path <- paste0(save_dir, "task_map.csv")
write.csv(params, map_path, row.names = F)

# SUBMIT JOBS -------------------------------------------------------------

array_qsub(jobname = "get_prev", 
           shell = "FILEPATH",
           code = paste0(code_dir, "01_childmanip_2019.R"),
           pass = list(map_path),
           proj = "proj_yld",
           num_tasks = nrow(params),
           cores = 3, mem = 4, 
           wallclock = "00:15:00", 
           log = T, submit = T)





# Check for missing locs, print names of missing locations and return datatable containing missing location_ids/names
check_missing_locs<-function(indir,filepattern){
  setwd(indir)
  
  # which locations should you have? 
  loc_dt <- get_location_metadata(location_set_id = ID, gbd_round_id=ID)
  loc_dt <- loc_dt[(is_estimate == 1 & most_detailed == 1)]
  locs<-loc_dt$location_id
  
  # which locations do you have 
  filepattern<-gsub(pattern = "\\..*",replacement = "",x = filepattern) #remove file extension 
  index<-grep(x = strsplit(x=strsplit(x = filepattern,split = "\\{")[[1]],split="\\}"),pattern = "location")-1
  saved_locs<-strsplit(x = gsub(pattern="\\..*",replacement="",x=list.files()),split = "_")
  saved_locs<-sapply(saved_locs, `[[`, index)
  
  missing<-setdiff(locs,saved_locs)
  loc_dt<-get_location_metadata(ID)
  missing_loc_dt<-loc_dt[location_id%in%missing][,.(location_id,location_name)]
  if(length(missing)>=1){
    print(paste("Missing the following location IDs:",paste(missing_loc_dt[,location_id],collapse=",")))
    return(missing_loc_dt)
  }else{
    print("Not missing any locations")
    return(FALSE)
  }
}


missing <- check_missing_locs(indir = "FILEPATH", 
                              filepattern = "{location_id}.rds")

