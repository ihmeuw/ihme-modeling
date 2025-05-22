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
release_id <- ADDRESS
#set estimation years:
years<-c(1990, 1995, 2000, 2005, 2010, 2015, 2020, 2022, 2023, 2024)
##############################################################

##############################################################
#create directory to store draws
dir.create("FILEPATH"))

#load geographic restrictions
d_geo<-read.csv(sprintf("FILEPATH/gr_ntd_dengue.csv"), 
                stringsAsFactors = FALSE)

# want to pull endemic national locations for imputation adj purposes
nat_locs <- c(179,163,90,11,16,6,135,95,142,86,67,214,180,130, 72,165,51,62,196,102)
d_geo<-d_geo[d_geo$value_endemicity==1,]

end_nat <- nat_locs[nat_locs %in% d_geo$location_id]
d_geo<-d_geo[d_geo$most_detailed==1,]

# list of unique dengue endemic locations
unique_d_locations<-unique(d_geo$location_id)
unique_d_locations <- c(unique_d_locations, end_nat)

# process draws for dengue model
d_locs<-get_location_metadata(release_id =release_id,location_set_id = 35)
d_locs<-d_locs[d_locs$is_estimate==1,]

# all locations
location_list<-unique(d_locs$location_id)

x <- 1
# pulling and saving out ST-GPR results for each location 
for(i in unique_d_locations){
  #pull in single country and spit out
  message(paste0("location_id: ", i, "; number: ", x, "/547"))
  message(x)
  upload_file<-read.csv(paste0("FILEPATH"))
  upload_file$modelable_entity_id<-1505
  upload_file$measure_id<-6
  # only keep estimation years
  upload_file<-upload_file%>%
    filter(year_id %in% c(years))
  
  fwrite(upload_file,paste0("FILEPATH"))  
  x <- x +1
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
