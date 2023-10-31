### NTDs Dengue
#Description: processing draws if used an all-ages ST-GPR model
#process dengue draws from all-age (sex specific) dengue incidence estimates produced by ST/GPR

#########################################################

library(dplyr)
rm(list = ls())
source("FILEPATH/get_location_metadata.R")

#pull in geographic restrictions and drop non-endemic countries and drop restricted locations
#output draws to folder by location
draw.cols <- paste0("draw_", 0:999)
#insert ST/gpr run id here:
st_gpr_run_id<-"ADDRESS"
#set estimation years:
years<-c(1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022)
##############################################################

##############################################################
#create directory to store draws
dir.create("FILEPATH"))

#load geographic restrictions
d_geo<-read.csv(sprintf("FILEPATH/gr_ntd_dengue.csv"), 
                stringsAsFactors = FALSE)

dataset<-d_geo[,1:52]

#convert to long
melt_data<-melt(dataset, id.vars = c('location_id',
                                     'parent_id',
                                     'level',
                                     'type',
                                     'location_name',
                                     'super_region_id',
                                     'region_id',
                                     'ihme_loc_id'))

melt_data$value<-as.character(melt_data$value)

status_list<-subset(melt_data, type=='status')

presence_list<-subset(status_list,value=='p' |value=='pp')

#list of unique dengue endemic locations
unique_d_locations<-unique(presence_list$location_id)


##process draws for dengue model

d_locs<-get_location_metadata(gbd_round_id = "ADDRESS",location_set_id = 35)
d_locs<-d_locs[d_locs$is_estimate==1,]
#all locations
location_list<-unique(d_locs$location_id)

#using results from STGPR 
for(i in unique_d_locations){
  #pull in single country and spit out
  upload_file<-read.csv(paste0("FILEPATH/",i,".csv"))
  upload_file$model_id<-"ADDRESS"
  upload_file$measure_id<-6
  #only keep estimation years
  upload_file<-upload_file%>%
    filter(year_id %in% c(years))
  
  write.csv(upload_file,(paste0("FILEPATH/", i,".csv")))  
}

########################################################
#output zero draws for non-endemic areas:
ne_locs<-location_list[! location_list %in% unique_d_locations]

#create shell upload file 
upload_file<-read.csv(paste0("FILEPATH/211.csv"))

for(i in ne_locs){
  #pull in single country and spit out
  upload_file$location_id<-i
  s1<-setDT(upload_file)
  s1[, id := .I]
  s1[, (draw.cols) := 0, by=id]
  write.csv(s1,(paste0("FILEPATH/", i,".csv")))  
}
