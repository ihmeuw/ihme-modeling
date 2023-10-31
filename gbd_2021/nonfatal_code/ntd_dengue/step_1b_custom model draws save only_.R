### NTDs Dengue
#Description: save custom model draws

library(dplyr)
library(data.table)
#import custom model incidence draws
source("FILEPATH/get_location_metadata.R")


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

d_locs<-get_location_metadata(gbd_round_id = "ADDRESS",
                              location_set_id = 35, decomp_step="ADDRESS")
d_locs<-d_locs[d_locs$is_estimate==1,]

#all locations
location_list<-unique(d_locs$location_id)
ne_locs<-location_list[! location_list %in% unique_d_locations]

draw.cols <- paste0("draw_", 0:999)
mod1<-read.csv("FILEPATH/ADDRESS.csv")

for(i in ne_locs){
  #pull in single country and spit out
  draws2<-setDT(mod1)
  draws2$location_id<-i
  draws2[, id := .I]
  draws2[, (draw.cols) := 0, by=id]
  write.csv(draws2,(paste0("FILEPATH/", i,".csv")))  
}


source("FILEPATH/save_results_epi.R")
save_results_epi(input_dir =paste0("FILEPATH/"),
                 input_file_pattern = "{location_id}.csv", 
                 model_id= "ADDRESS",
                 description = "stgpr only ll age",
                 measure_id = 6,
                 decomp_step = "ADDRESS",
                 gbd_round_id= "ADDRESS",
                 bundle_id= "ADDRESS",
                 crosswalk_version_id= "ADDRESS",
                 mark_best = FALSE
)   
