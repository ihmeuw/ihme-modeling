#hybridize draws
library(dplyr)


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


#output draws to folder by location
draw.cols <- paste0("draw_", 0:999)



#list of unique dengue endemic locations
unique_d_locations<-unique(presence_list$location_id)

for(i in unique_d_locations){
  mod1<-read.csv((paste0("FILEPATH", i,".csv")))
  mod1a <- mod1[, c(paste0("draw_", 0:500), "location_id", "year_id", "sex_id", "age_group_id")]
  mod2<-read.csv((paste0("FILEPATH", i,".csv")))
  mod2a <- mod2[, c(paste0("draw_", 501:999), "location_id", "year_id", "sex_id", "age_group_id")]

 
mod3<-merge(mod1a,mod2a, by=c("location_id","year_id","sex_id","age_group_id"))
mod3$measure_id<-6
mod3$modelable_entity_id<-ADDRESS


draws2<-setDT(mod3)
draws2[, id := .I]
#set all draws for age 5 and under to 0
draws2[age_group_id<4, (draw.cols) := 0, by=id]

#output
write.csv(draws2,(paste0("FILEPATH", i,".csv")))
}

#pull in hybrid2 draws from India
india_locs<-read.csv("FILEPATH")
unique_ind<-unique(india_locs$location_id)

for(i in unique_ind){
  mod1<-read.csv((paste0("FILEPATH", i,".csv")))
  write.csv(mod1,(paste0("FILEPATH", i,".csv")))  
}



fl<-read.csv("FILEPATH")
draws2<-setDT(fl)
draws2[, id := .I]
#set all draws  prior to 2010 to zero
draws2[year_id<2010, (draw.cols) := 0, by=id]
#multiply incidence by 10% to reduce 
for(j in grep('draw_', names(draws2))){
  set(draws2, i= which(draws2[[j]]<1), j= j, value=draws2[[j]]*.1)
}

write.csv(draws2,"FILEPATH")


tx<-read.csv("FILEPATH")
draws2<-setDT(tx)
draws2[, id := .I]

#multiply incidence by 10% to reduce 
for(j in grep('draw_', names(draws2))){
  set(draws2, i= which(draws2[[j]]<1), j= j, value=draws2[[j]]*.1)
}

write.csv(draws2,"FILEPATH")


#for non-endemic locations

ne_locs<-location_list[! location_list %in% unique_d_locations]

for(i in ne_locs){
  #pull in single country and spit out
  mod1<-read.csv((paste0("FILEPATH", 33,".csv")))
  draws2<-setDT(mod1)
  draws2$location_id<-i
  draws2[, id := .I]
  #set all draws  to 0
  #draws2[, (draw.cols) := 0, by=id]
  
  write.csv(draws2,(paste0("FILEPATH", i,".csv")))  
}


source("/ihme/cc_resources/libraries/current/r/save_results_epi.R")
save_results_epi(input_dir =paste0("FILEPATH"),
                 input_file_pattern = "{location_id}.csv", 
                 modelable_entity_id = ADDRESS,
                 description = "Iterative model hybrid age fix_2",
                 measure_id = 6,
                 decomp_step = "iterative",
                 gbd_round_id=6,
                 mark_best = TRUE
)   
