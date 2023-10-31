# Purpose: Run saves for final MEID
# Notes: Processes incidence & prevalence draws, apply grs, upload the saves

### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-"FILEPATH"
data_root <- "FILEPATH"

params_dir <- "FILEPATH"
draws_dir <- "FILEPATH"
interms_dir <- "FILEPATH"
logs_dir <- "FILEPATH"

# Source relevant libraries
library(dplyr)
library(readstata13)
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/save_results_epi.R")
source("FILEPATH/get_age_metadata.R")

###########################################################################
ifelse(!dir.exists("FILEPATH/ADDRESS_prevalence/") dir.create("FILEPATH/ADDRESS_prevalence/"), FALSE)

#draws processing for CL MEID "ADDRESS"

#pull incidence and prevalence draws and output into files for saves

#pull in geographic restrictions and drop non-endemic countries and drop restricted locations
#output draws to folder by location
draw.cols <- paste0("draw_", 0:999)

cl_geo<-read.csv("FILEPATH")
cl_geo<-cl_geo[cl_geo$most_detailed==1,]
cl_geo<-cl_geo[cl_geo$year_start==2019,]
cl_geo<-cl_geo[cl_geo$value_endemicity==1,]

#list of unique CL endemic locations
unique_cl_locations<-unique(cl_geo$location_id)

#####pull in incidence and prev and append
##copy 2018 results into 2019 thru 2022

for(i in unique_cl_locations){
  #pull in single country and spit out
  inc_file<-read.dta13(paste0("FILEPATH",i,".dta"))
  
  #drop extra vars
  inc_file<-inc_file[,1:1010]
  #drop run_id, totalPop,v1, population
 inc_file<- select(inc_file, -c(v1, totalPop, run_id, population))
  
  prev_file<-read.csv(paste0("FILEPATH/ADDRESS_prevalence/",i,".csv"))
  
  #append
  upload_file<-rbind(inc_file,prev_file)
  
  #fix years
  upload_file<-upload_file%>%
    mutate(year_id=ifelse(year_id %in% 2018,2019,year_id))
  
  #copy and append 
  
  yr20<-upload_file[upload_file$year_id==2019,]
  yr20$year_id<-2020
  
  yr21<-upload_file[upload_file$year_id==2019,]
  yr21$year_id<-2021
  
  yr22<-upload_file[upload_file$year_id==2019,]
  yr22$year_id<-2022
  
  
  upload_file2<-rbind(upload_file,yr20,yr21,yr22)
  
  years<-c(1990,1995,2000,2005,2010,2015,2019,2020,2021,2022)
  upload_file2<-
    upload_file2 %>% filter(year_id %in% years)
  
  #ensure prevalence < 6 months of age = zero
  
  draws2<-setDT(upload_file2)
  draws2[, id := .I]

  draws2[age_group_id<4, (draw.cols) := 0, by=id]
  draws2[age_group_id==388, (draw.cols) := 0, by=id]
  
  #draws2$model_id<-ADDRESS
  
  write.csv(draws2,(paste0("FILEPATH/ADDRESS/", i,".csv")))  
}


#output all non-endemic draws

all_locs<-get_location_metadata(gbd_round_id = "ADDRESS",location_set_id = 35, decomp_step="ADDRESS")
all_locs<-all_locs[all_locs$is_estimate==1,]
#all locations
location_list<-unique(all_locs$location_id)

ne_locs<-location_list[! location_list %in% unique_cl_locations]
upload_file<-read.csv(paste0("FILEPATH/ADDRESS_prevalence/","ADDRESS",".csv"))

#fix zero years
#copy and append 
yr19<-upload_file[upload_file$year_id==2018,]
yr19$year_id<-2019

yr20<-upload_file[upload_file$year_id==2018,]
yr20$year_id<-2020

yr21<-upload_file[upload_file$year_id==2018,]
yr21$year_id<-2021

yr22<-upload_file[upload_file$year_id==2018,]
yr22$year_id<-2022


upload_file2<-rbind(upload_file,yr19,yr20,yr21,yr22)

for(i in ne_locs){
  #pull in single country and spit out
  upload_file2$location_id<-i
  s1<-setDT(upload_file2)
  s1[, id := .I]
  s1[, (draw.cols) := 0, by=id]
  s2<-copy(s1)
  s2[, measure_id := 6]
  out <- rbind(s1, s2)
  write.csv(out,(paste0("FILEPATH/ADDRESS/", i,".csv")))  
}
