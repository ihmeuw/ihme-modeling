
central_root <- 'FILEPATH'
setwd(central_root)

source('FILEPATH')
source('FILEPATH')

#check locations for which data are included
dt<-read.csv("FILEPATH")
myvars <- c("location_id","dengue_prob_mean")
dt_locs <- dt[myvars]

source("FILEPATH")
locs<-get_location_metadata(location_set_id = 22,gbd_round_id=6)
locs<-locs[locs$is_estimate==1,]

#skip
dt_locs2<-merge(dt_locs,locs,by="location_id")
table(dt_locs2$location_name)
write.csv(dt_locs2,"FILEPATH")


dt3<-dt[dt$location_id!=14,]
dt4<-dt3[dt3$location_id!=369,]

write.csv(dt4,"FILEPATH")


run_id <- register_stgpr_model("FILEPATH", model_index_id = 1)
stgpr_sendoff(run_id, 'ADDRESS')

#model #2, no level 4 res, dropped covar on latitude otherwise same as #1
run_id <- register_stgpr_model("FILEPATH", model_index_id = 2)
stgpr_sendoff(run_id, 'ADDRESS')

#model #3, no level 4 res, only covar is csmr
run_id <- register_stgpr_model("FILEPATH", model_index_id = 3)
stgpr_sendoff(run_id, 'ADDRESS')


#model #4 change pop latitude covar
run_id <- register_stgpr_model("FILEPATH", model_index_id = 4)
stgpr_sendoff(run_id, 'ADDRESS')


#model #5 change added elevation covar
run_id <- register_stgpr_model("FILEPATH", model_index_id = 5)
stgpr_sendoff(run_id, 'ADDRESS')


#model #6 change added changed zeta to .1 and added nsv
run_id <- register_stgpr_model("FILEPATH", model_index_id = 6)
stgpr_sendoff(run_id, 'ADDRESS')

#model #7 
run_id <- register_stgpr_model("FILEPATH", model_index_id = 7)
stgpr_sendoff(run_id, 'ADDRESS')

#model #8 -outliered data, no pop density covar
run_id <- register_stgpr_model("FILEPATH", model_index_id = 8)
stgpr_sendoff(run_id, 'ADDRESS')



#model #9 -outliered data, chnaged zeta and lambda 
run_id <- register_stgpr_model("FILEPATH", model_index_id = 9)
stgpr_sendoff(run_id, 'ADDRESS')


#model #10 -no outliered data, changed zeta and lambda added elevation
run_id <- register_stgpr_model("FILEPATH", model_index_id = 10)
stgpr_sendoff(run_id, 'ADDRESS')
#120449

#model #11 -outliered all south asia data
run_id <- register_stgpr_model("FILEPATH", model_index_id = 11)
stgpr_sendoff(run_id, 'ADDRESS')

#model #12 -nauru test
run_id <- register_stgpr_model("FILEPATH", model_index_id = 12)
stgpr_sendoff(run_id, 'ADDRESS')


#model #13 
run_id <- register_stgpr_model("FILEPATH", model_index_id = 13)
stgpr_sendoff(run_id, 'ADDRESS')


#model #14
run_id <- register_stgpr_model("FILEPATH", model_index_id = 14)
stgpr_sendoff(run_id, 'ADDRESS')

#model #15
run_id <- register_stgpr_model("FILEPATH", model_index_id = 15)
stgpr_sendoff(run_id, 'ADDRESS')

#next split data into a s. asia only model - location ids are:

table(dt_locs2$region_name)


dt_sa<-merge(dt,locs,by="location_id")
dt_sa2<-dt_sa[dt_sa$region_name=="South Asia",]

write.csv(dt_sa2,"FILEPATH")



run_id <- register_stgpr_model("FILEPATH", model_index_id = 16)
stgpr_sendoff(run_id, 'ADDRESS')

#model #17-only update is the ur data chagne, no csmr
run_id <- register_stgpr_model("FILEPATH", model_index_id = 17)
stgpr_sendoff(run_id, 'ADDRESS')




#model #18-no csmr updated UR dataset
run_id <- register_stgpr_model("FILEPATH", model_index_id = 18)
stgpr_sendoff(run_id, 'ADDRESS')

#model #19-with csmr updated UR dataset, no pop dens
run_id <- register_stgpr_model("FILEPATH", model_index_id = 19)
stgpr_sendoff(run_id, 'ADDRESS')


#model #20-only csmr updated UR dataset
run_id <- register_stgpr_model("FILEPATH", model_index_id = 20)
stgpr_sendoff(run_id, 'ADDRESS')

dt<-read.csv("FILEPATH")
dt_sa<-merge(dt,locs,by="location_id")
dt_sa2<-dt_sa[dt_sa$region_name!="South Asia",]

#drop nauru and maldives b/c the value for prop dengue pop is 0
#dt_sa2<-dt_sa2[dt_sa2$location_id!=14,]
#dt_sa2<-dt_sa2[dt_sa2$location_id!=369,]

dt_sa2<-dt_sa2[dt_sa2$region_name!="East Asia",]
dt_sa2<-dt_sa2[!(dt_sa2$location_id==17 & dt_sa2$year_id==2017),]
dt_sa2<-dt_sa2[!(dt_sa2$location_id==189 & dt_sa2$year_id==2018),]
write.csv(dt_sa2,"FILEPATH")

#model 21
run_id <- register_stgpr_model("FILEPATH", model_index_id = 21)
stgpr_sendoff(run_id, 'ADDRESS')


dt_af2<-dt_sa[dt_sa$region_name!="Western Sub-Saharan Africa",]

write.csv(dt_af2,"FILEPATH")

#model 22-no w. africa data, testing if covariates can lift that part of the model
run_id <- register_stgpr_model("FILEPATH", model_index_id = 22)
stgpr_sendoff(run_id, 'ADDRESS')


#model 23-no sa and other outliers data, 
run_id <- register_stgpr_model("FILEPATH", model_index_id = 23)
stgpr_sendoff(run_id, 'ADDRESS')



#run saves to look at output

#save results

source("FILEPATH")
locs<-get_location_metadata(location_set_id = 35, gbd_round_id=6)
locs<-locs[locs$is_estimate==1,]

location_list<-unique(locs$location_id)

for(i in location_list){
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


source("FILEPATH")
save_results_epi(input_dir =paste0("FILEPATH"),
                 input_file_pattern = "{location_id}.csv", 
                 modelable_entity_id = ADDRESS,
                 description = "Iterative model resub 1/24PM",
                 measure_id = 6,
                 decomp_step = "iterative",
                 mark_best = TRUE
)   
