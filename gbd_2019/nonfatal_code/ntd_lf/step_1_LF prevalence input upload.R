

#process LF Focal 3 prevalence inputs into MEID 


user <- Sys.info()[["user"]] ## Get current user name
path <- paste0("FILEPATH", user, "FILEPATH")
library(dplyr)
library(tidyr)

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}		

#load shared functions 

source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))

#pull in geographic restrictions file 

lf_geo<-read.csv(sprintf("FILEPATH"), stringsAsFactors = FALSE)

dataset<-lf_geo[,1:52]

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

#list of unique trachoma endemic locations
unique_lf_locations<-unique(presence_list$location_id)

locs<-get_location_metadata(location_set_id = 35, gbd_round_id=ADDRESS)
locs<-locs[locs$is_estimate==1,]

#list of all locations:
location_list<-unique(locs$location_id)



#split india subnational parent from urban to rural

india_list<-read.csv("FILEPATH")

india_parent<-unique(india_list$parent_id)

india_locs<-merge(india_list,locs, by="parent_id")
unique_india<-unique(india_locs$location_id)

#loop through this list and output files for location ids that identify india subnats urban/rural


for(i in india_parent){
  upload_ind<-read.csv(paste0("FILEPATH", i,".csv"))
  upload_ind$parent_id<-upload_ind$location_id
  test1<-merge(upload_ind,india_locs, by="parent_id")
  
  #output to urban rural
  ind_ids<-unique(test1$location_id.y)
  
  for(j in ind_ids){
  
  test2<-test1[test1$location_id.y==j,]
  test2$location_id<-j
  #output location ids for india urban v. rural subnats to the main folder of draws produced by the geospatial model
  write.csv(test2,(paste0("FILEPATH", j,".csv")))
  }
}


#import shell dataset 
shell<-read.csv(paste0("FILEPATH"))
shell_2<-shell[shell$age_group_id==5,]
shell_2$age_group_id<-2

shell_3<-shell[shell$age_group_id==5,]
shell_3$age_group_id<-3

shell_4<-shell[shell$age_group_id==5,]
shell_4$age_group_id<-4

#append ages 

shell_2_4<-rbind(shell_2,shell_3,shell_4)
shell_2_4$location_id<-NULL

#set draws to zero for <1s
draw.cols <- paste0("draw_", 0:999)
s1<-setDT(shell_2_4)
s1[, id := .I]
s2<-s1[age_group_id<5, (draw.cols) := 0, by=id]


#----------get draws from focal 3 input files-----------------

for(i in unique_lf_locations){
  #pull location specific draws from geospatial model results
  upload_file<-read.csv(paste0("FILEPATH", i,".csv"))
  
  
  #add in lower ages
  s2$location_id<-i
  upload_file<-rbind(upload_file,s2, fill=T)
  #fix names variable
  names(upload_file)[names(upload_file) == "year"] <- "year_id"
  
    #add year 2019 (carry 2017 prediction forward)
  draws19<-upload_file[upload_file$year_id==2017,]
  draws19$year_id<-2019
  
  
  #append years together
  
  upload_file2<-rbind(upload_file, draws19)
  
  #add upper age group- assume same prevalence as age group 32
  
  draws_age235<-upload_file2[upload_file2$age_group_id==32,]
  draws_age235$age_group_id<-235
  
  #append
  
  
  
  upload_file3<-rbind(upload_file2,draws_age235)
  upload_file3$modelable_entity_id<-ADDRESS
  #add in age gropus 2 - 4, set prevalence to zero
  

  #output to format for GBD save results
  write.csv(upload_file3,(paste0("FILEPATH", i,".csv")))
}


#--------------output zero prevalence draws for non-endemic locations-------------------


#--------------GENERATE A LIST OF NON-ENDEMIC LOCATIONS------------#

ne_locs<-location_list[! location_list %in% unique_lf_locations]



#pull in all draws shell

for(i in ne_locs){
  upload_file3$location_id<-i
  upload_file3[, (draw.cols) := 0, by=id]
 
  write.csv(upload_file3,(paste0("FILEPATH", i,".csv")))
}


#save results

source("FILEPATH")
save_results_epi(input_dir =paste0("FILEPATH"),
                 input_file_pattern = "{location_id}.csv", 
                 modelable_entity_id = ADDRESS,
                 description = "LF resub GS 20191219",
                 measure_id = ADDRESS,
                 gbd_round_id=ADDRESS,
                 decomp_step = "iterative",
                 mark_best = TRUE
)       

