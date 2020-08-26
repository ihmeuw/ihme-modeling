############################################################################################################
## Project: RF: Lead Exposure
## Purpose: Compile lead microdata for distribution modeling
############################################################################################################

# clear memory
rm(list=ls())
library(data.table)
library(magrittr)
library(haven)
library(ggplot2)

files <- list.files("FILEPATH")

# pull location metadata
source("FILEPATH")
locations <- get_location_metadata(gbd_round_id = 6, location_set_id = 22)
locs <- locations[,list(location_id,ihme_loc_id,region_id,super_region_id)]

data <- data.table()
for (survey in files) {
  df <- read_dta("FILEPATH") %>% data.table
  if (unique(df$ihme_loc_id == "IND")) df[,ihme_loc_id:= admin_1_id]
  df[,year_id := floor((year_start + year_end)/2)]
  data <- rbind(data,df[!is.na(bll) & bll != 8888,list(nid,ihme_loc_id,year_id,age_year,sex_id,bll)])
}

setnames(data,"bll","data")
data <- merge(data,locs,by="ihme_loc_id",all.x=T)

write.csv(data, "FILEPATH")

## END