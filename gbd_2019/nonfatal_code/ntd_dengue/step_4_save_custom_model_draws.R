library(dplyr)
library(data.table)
#import custom model incidence draws
source("FILEPATH")


d_geo<-read.csv(sprintf("FILEPATH"), stringsAsFactors = FALSE)

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

d_locs<-get_location_metadata(gbd_round_id = 6,location_set_id = 35)
d_locs<-d_locs[d_locs$is_estimate==1,]
#all locations
location_list<-unique(d_locs$location_id)
ne_locs<-location_list[! location_list %in% unique_d_locations]

#using results from custom negative binomial model
upload_file<-read.csv("FILEPATH")

#for(i in unique_d_locations){
  #pull in single country and spit out
 # upload_file<-custom[custom$location_id==i,]
 
  #write.csv(upload_file,(paste0("FILEPATH", i,".csv")))  
#}


#fix locations that stata won't output via the cluster

et<-read.csv("FILEPATH")
d35600<-read.csv("FILEPATH")
nga<-read.csv("FILEPATH")
d438<-read.csv("FILEPATH")
d535<-read.csv("FILEPATH")
#append
upload2<-rbind(et,d35600,nga,d438,d535)
ups<-unique(upload2$location_id)

for(i in ups){
#pull in single country and spit out
upload_file<-upload2[upload2$location_id==i,]
write.csv(upload_file,(paste0("FILEPATH")))  
}


#ups2
d4643<-read.csv("FILEPATH")
d150<-read.csv("FILEPATH")
d320<-read.csv("FILEPATH")

extra1<-read.csv("FILEPATH")
extra2<-read.csv("FILEPATH")
extra3<-read.csv("FILEPATH")
d482<-read.csv("FILEPATH")
#append
upload2<-rbind(d4643,d150,d320,extra1, extra2, extra3,d482)
ups2<-unique(upload2$location_id)

for(i in ups2){
  #pull in single country and spit out
  upload_file<-upload2[upload2$location_id==i,]
  write.csv(upload_file,(paste0("FILEPATH", i,".csv")))  
}

#output non -endemic draws

ne_locs<-location_list[! location_list %in% unique_d_locations]
draw.cols <- paste0("draw_", 0:999)

for(i in ne_locs){
  #pull in single country and spit out
  mod1<-read.csv((paste0("FILEPATH", 33,".csv")))
  draws2<-setDT(mod1)
  draws2$location_id<-i
  write.csv(draws2,(paste0("FILEPATH", i,".csv")))  
}


source("FILEPATH")
save_results_epi(input_dir =paste0("FILEPATH"),
                 input_file_pattern = "{location_id}.csv", 
                 modelable_entity_id = ADDRESS,
                 description = "custom model draws_random",
                 measure_id = 6,
                 decomp_step = "iterative",
                 gbd_round_id= 6,
                 mark_best = TRUE
)   
