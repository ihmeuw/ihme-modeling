

#CODE to clean lowvis and blindess data and to apply geographic restrictions

#-------------------------------------------3

user <- Sys.info()[["user"]] 
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

#load functions 

source(sprintf("%sFILEPATH",prefix))
source(sprintf("%sFILEPATH",prefix))
source(sprintf("%sFILEPATH",prefix))
source(sprintf("%sFILEPATH",prefix))
source(sprintf("%sFILEPATH",prefix))

#pull in geographic restrictions file 

tra_geo<-read.csv(sprintf("FILEPATH"), stringsAsFactors = FALSE)
dataset<-tra_geo[,1:50]

#convert to long
melt_data<-melt(dataset, id.vars = c('loc_id',
                                     'parent_id',
                                     'level',
                                     'type',
                                     'loc_nm_sh',
                                     'loc_name',
                                     'spr_reg_id',
                                     'region_id',
                                     'ihme_lc_id',
                                     'GAUL_CODE'))

melt_data$value<-as.character(melt_data$value)

status_list<-subset(melt_data, type=='status')

presence_list<-subset(status_list,value=='p' |value=='pp')

#list of unique trachoma endemic locations
unique_tra_locations<-unique(presence_list$loc_id)

locs<-get_location_metadata(location_set_id = 35)
locs<-locs[locs$is_estimate==1,]

location_list<-unique(locs$location_id)


#----------get draws -----------------


meDraws_blind<-get_draws(gbd_id_type=ADDRESS,gbd_id=ADDRESS, source="epi", location_id=unique_tra_locations, year_id=c(1990,1995,2000, 2005, 2010,2015, 2017, 2019), decomp_step="iterative")
meDraws_lowvis<-get_draws(gbd_id_type=ADDRESS,gbd_id=ADDRESS, source="epi", location_id=unique_tra_locations, year_id=c(1990,1995,2000, 2005, 2010, 2015,2017, 2019), decomp_step="iterative")

draw.cols <- paste0("draw_", 0:999)

for(i in unique_tra_locations){
    upload_file<-meDraws_blind[meDraws_blind$location_id==i,]
    upload_file$modelable_entity_id<-ADDRESS
    #set ages <15 years to zero
    s1<-setDT(upload_file)
    s1[, id := .I]
    s1[age_group_id<8, (draw.cols) := 0, by=id]
    
    
    write.csv(s1,(paste0("FILEPATH", i,"FILEPATH")))
}



for(i in unique_tra_locations){
  upload_file2<-meDraws_lowvis[meDraws_lowvis$location_id==i,]
  upload_file2$modelable_entity_id<-ADDRESS
  s2<-setDT(upload_file2)
  s2[, id := .I]
  s2[age_group_id<8, (draw.cols) := 0, by=id]
  write.csv(s2,(paste0("FILEPATH", i,"FILEPATH")))
}

#--------------GENERATE A LIST OF NON-ENDEMIC LOCATIONS------------#

ne_locs<-location_list[! location_list %in% unique_tra_locations]

#OUTPUT zero draws for non-endemic locations for both lowvis and blindness

for(i in ne_locs){
  s2$location_id<-i
  s2$modelable_entity_id<-ADDRESS
  s2[, (draw.cols) := 0, by=id]
  write.csv(s2,(paste0("FILEPATH", i,"FILEPATH")))
  s2$modelable_entity_id<-ADDRESS
  write.csv(s2,(paste0("FILEPATH", i,"FILEPATH")))
}


#save results

source("FILEPATH")
save_results_epi(input_dir =paste0("FILEPATH"),
                 input_file_pattern = "FILEPATH", 
                 modelable_entity_id = ADDRESS,
                 description = "Step 4 model",
                 measure_id = ADDRESS,
                 decomp_step = "step4",
                 mark_best = TRUE
)       


save_results_epi(input_dir =paste0("FILEPATH"),
                 input_file_pattern = "FILEPATH", 
                 modelable_entity_id = ADDRESS,
                 description = "Step 4 model",
                 measure_id = ADDRESS,
                 decomp_step = "step4",
                 mark_best = TRUE
)       
