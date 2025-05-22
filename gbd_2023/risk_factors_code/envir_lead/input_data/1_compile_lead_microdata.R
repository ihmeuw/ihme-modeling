############################################################################################################
## Project: RF: Lead Exposure
## Purpose: Compile lead microdata for distribution modeling
############################################################################################################

# clear memory
rm(list=ls())

####### Libraries ##########################
library(data.table)
library(magrittr)
library(haven)
library(ggplot2)

######### runtime configuration ######################
#This is so you can automatically determine if you are using a linux system or not
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

############# Import microdata #####################
files <- list.files(file.path(j_root,"FILEPATH/exp_microdata_gbd2020"), full.names = TRUE)

######### pull location metadata ########################
#source location metadata
source("FILEPATH/get_location_metadata.R")

#Get location metadata for gbd round and location set
locations <- get_location_metadata(gbd_round_id = 7, location_set_id = 22)

#Only keep the following columns
locs <- locations[,list(location_id,ihme_loc_id,region_id,super_region_id)]

########## Read file function ########################
read_file <- function(filepath) {
  if (filepath %like% ".dta") { #If the filepath has .dta do this
    df <- data.table(read_dta(filepath)) #use the data.table function
  } else if (filepath %like% ".csv") { #if the filepath has .csv do this
    df <- fread(filepath) #use the fread function
  }
  #now that you have read in the file do this:
  if (unique(df$ihme_loc_id == "IND")) df[,ihme_loc_id:= admin_1_id] #if the ihme_loc_id is IND (India), then change the ihme_loc_id to be admin_1_id
  df[, year_id := floor((year_start + year_end)/2)] #change the year_id column to be the output of this equation
  df <- df[!is.na(bll) & bll != 8888, .(nid, ihme_loc_id, year_id, age_year, sex_id, bll)] #if the bll is not NA and is not 8888, then only keep these columns

  return(df)#the output of this function should be the remaining data.table
}

####### Read in files #####################
#Read in all of the files by using the read_file function. Place the output of each as more rows in one large data.table called data
data <- rbindlist(lapply(files, read_file), fill = TRUE)

######### Merge locs and data ###################
data <- merge(locs, data, by = "ihme_loc_id")
setnames(data, "bll", "data") #change bll to data

########## Save ###################
write.csv(data[, .(nid, location_id, year_id, age_year, sex_id, data)],
          "FILEPATH/compiled_lead_microdata.csv", row.names = FALSE)

## END
