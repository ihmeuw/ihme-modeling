#NTDs Dengue
#Description: hybridize draws from st/gpr and custom model

library(dplyr)
draw.cols <- paste0("draw_", 0:999)

#specify final st/gpr model run here:
st_gpr_run_id<-"ADDRESS"

source("FILEPATH/save_results_epi.R")

##############----------------------------######################

##read in GRs and zero locations - 
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


#output draws to folder by location


#list of unique dengue endemic locations
unique_d_locations<-unique(presence_list$location_id)

#note the final file path for custom draws must be specified here:
for(i in unique_d_locations){
  mod1<-read.csv((paste0("FILEPATH/", i,".csv")))
  mod1a <- mod1[, c(paste0("draw_", 0:500), "location_id", "year_id", "sex_id", "age_group_id")]
  mod2<-read.csv((paste0("/FILEPATH/", i,".csv")))
  mod2a <- mod2[, c(paste0("draw_", 501:999), "location_id", "year_id", "sex_id", "age_group_id")]

 
mod3<-merge(mod1a,mod2a, by=c("location_id","year_id","sex_id","age_group_id"))
mod3$measure_id<-6
mod3$model_id<-"ADDRESS"


draws2<-setDT(mod3)
draws2[, id := .I]

#output
write.csv(draws2,(paste0("FILEPATH/", i,".csv")))
}

#manual corrections for US and Florida 
#texas (loc=566) and florida (loc=532) 

#florida
fl<-read.csv("FILEPATH/532.csv")
draws2<-setDT(fl)
draws2[, id := .I]
#set all draws  prior to 2010 to zero as no dengue incidence reported
draws2[year_id<2010, (draw.cols) := 0, by=id]
write.csv(draws2,"FILEPATH/532.csv")

#texas
tx<-read.csv("FILEPATH/566.csv")
draws2<-setDT(tx)
draws2[, id := .I]
#multiply incidence by 10% to reduce by UR factor in this setting 
for(j in grep('draw_', names(draws2))){
  set(draws2, i= which(draws2[[j]]<1), j= j, value=draws2[[j]]*.1)
}
write.csv(draws2,"FILEPATH/566.csv")


#for non-endemic locations

ne_locs<-location_list[! location_list %in% unique_d_locations]

for(i in ne_locs){
  #pull in single country and write out to copy over with zeros for non-endemic settings (Kuwait = 145)
  mod1<-read.csv((paste0("FILEPATH/", 145,".csv")))
  draws2<-setDT(mod1)
  draws2$location_id<-i
  draws2[, id := .I]
  #set all draws  to 0
  draws2[, (draw.cols) := 0, by=id]
  
  write.csv(draws2,(paste0("FILEPATH/", i,".csv")))  
}


desc2<-paste0("hybrid of custom and stgpr ",st_gpr_run_id," models")
save_results_epi(input_dir =paste0("FILEPATH/"),
                 input_file_pattern = "{location_id}.csv", 
                 model_id= "ADDRESS",
                 description = desc2,
                 measure_id = 6,
                 decomp_step = "ADDRESS",
                 gbd_round_id="ADDRESS",
                 bundle_id="ADDRESS",
                 crosswalk_version_id="ADDRESS",
                 mark_best = TRUE
)   
