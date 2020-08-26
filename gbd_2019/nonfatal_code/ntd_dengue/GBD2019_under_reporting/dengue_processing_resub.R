

user <- Sys.info()[["user"]] ## Get current user name
path <- paste0("/share/homes/", user, "/rlibs/")
#install.packages("BayesianTools", lib=path)
#library(BayesianTools, lib.loc=path)
library(dplyr)

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}		

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
#pull in geographic restrictions and drop non-endemic countries and drop restricted locations


d_geo<-read.csv(sprintf("FILEPATH
"), stringsAsFactors = FALSE)

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
##output all draws from St/GPR ADDRESS - updated UR model with all data points

d_locs<-get_location_metadata(gbd_round_id = 6,location_set_id = 35)
d_locs<-d_locs[d_locs$is_estimate==1,]
#all locations
location_list<-unique(d_locs$location_id)

for(i in unique_d_locations){
  #pull in single country and spit out
  upload_file<-read.csv(paste0("FILEPATH",i,".csv"))
  upload_file$modelable_entity_id<-ADDRESS
  upload_file$measure_id<-6
  #only keep estimation years
  upload_file<-upload_file[upload_file$year_id==1990 | upload_file$year_id==1995 |upload_file$year_id==2000 | upload_file$year_id==2005 |upload_file$year_id==2010 |upload_file$year_id==2015 | upload_file$year_id==2017 |upload_file$year_id==2019,]
  
  #generate placeholder for age groups <5
  age4<-upload_file[upload_file$age_group_id==5,]
  age4$age_group_id<-4
  
  age3<-upload_file[upload_file$age_group_id==5,]
  age3$age_group_id<-3
  
  age2<-upload_file[upload_file$age_group_id==5,]
  age2$age_group_id<-2
  
  #append
  
  s1<-rbind(upload_file,age4,age3,age2)
  
  
  write.csv(s1,(paste0("FILEPATH", i,".csv")))  
}



##############################

########################WEST AFRICA PULL IN DRAWS FROM MODEL THAT W. African Data####################################

#except cape verde
africa_locs<-d_locs[d_locs$region_id==199,]
w_afr_locs<-unique(africa_locs$location_id)

for(i in w_afr_locs){
  upload_file<-read.csv(paste0("FILEPATH",i,".csv"))
  
  upload_file$modelable_entity_id<-ADDRESS
  upload_file$measure_id<-6
  #only keep estimation years
  upload_file<-upload_file[upload_file$year_id==1990 | upload_file$year_id==1995 |upload_file$year_id==2000 | upload_file$year_id==2005 |upload_file$year_id==2010 |upload_file$year_id==2015 | upload_file$year_id==2017 |upload_file$year_id==2019,]
  
  #generate placeholder for age groups <5
  age4<-upload_file[upload_file$age_group_id==5,]
  age4$age_group_id<-4
  
  age3<-upload_file[upload_file$age_group_id==5,]
  age3$age_group_id<-3
  
  age2<-upload_file[upload_file$age_group_id==5,]
  age2$age_group_id<-2
  
  #append
  
  s1<-rbind(upload_file,age4,age3,age2)
  
  
  write.csv(s1,(paste0("FILEPATH", i,".csv")))  
}
  
#output zero draws for non-endemic areas:


ne_locs<-location_list[! location_list %in% unique_d_locations]
upload_file<-read.csv("FILEPATH")

for(i in ne_locs){
  #pull in single country and spit out
  upload_file$location_id<-i
  s1<-setDT(upload_file)
  s1[, id := .I]
  s1[, (draw.cols) := 0, by=id]
  write.csv(s1,(paste0("FILEPATH", i,".csv")))  
}


source("FILEPATH")
save_results_epi(input_dir =paste0("FILEPATH"),
                 input_file_pattern = "{location_id}.csv", 
                 modelable_entity_id = ADDRESS,
                 description = "Iterative model resub ADDRESS2",
                 measure_id = 6,
                 decomp_step = "iterative",
                 gbd_round_id=6,
                 mark_best = TRUE
)
