#code to account for ADL due to hydrocele and lympehdema
#pull in lymphedema draws 
lymph_draws<-read.csv("FILEPATH")

lymph_draws<-as.data.table(lymph_draws)
#lymphedema, 95% of patients have 4 episodes per year, 7 days each
for(j in grep('draw', names(lymph_draws))){
  set(lymph_draws, i= which(lymph_draws[[j]]<=1), j= j, value=lymph_draws[[j]]*(.95*4*(7/365)))
}


#pull in hydrocele draws
hydr_draws<-read.csv("FILEPATH")
#for hydrocele, 70% of patients have 2 episodes per year, 7 days each
for(j in grep('draw', names(hydr_draws))){
  set(hydr_draws, i= which(hydr_draws[[j]]<=1), j= j, value=hydr_draws[[j]]*(.7*2*(7/365)))
}


#----add these together-----
  
#create matrix for each
  #sort each by location_id year_id sex_id age_group_id
  
ADL<-bind_rows(lymph_draws, hydr_draws) %>%
  group_by(location_id, year_id, sex_id, age_group_id) %>%
  summarise_at(draw.cols,funs(sum(., na.rm = TRUE)))

#export files for all locations
  
total_locs<-get_location_metadata(gbd_round_id=6,location_set_id = 35)
total_locs<-total_locs[total_locs$is_estimate==1,]
loc_list<-unique(total_locs$location_id)

for(i in unique_lf_locations){
  upload_file<-ADL[ADL$location_id==i,]
  upload_file$modelable_entity_id<-ADDRESS
  upload_file$measure_id<-5
  #upload_file<-upload_file[age_group_id<6, (draw.cols) := 0, by=id]
   write.csv(upload_file,(paste0("FILEPATH", i,".csv")))
}


ne_locs<-location_list[! location_list %in% unique_lf_locations]

#OUTPUT zero draws for non-endemic locations for both lowvis and blindness

for(i in ne_locs){
  #pull in single country and zero out
  upload_file<-read.csv("FILEPATH")
  upload_file$location_id<-i
  upload_file$modelable_entity_id<-ADDRESS
  upload_file$measure_id<-ADDRESS
  #set all draws to zero
  s1<-setDT(upload_file)
  s1[, id := .I]
  s1[, (draw.cols) := 0, by=id]
  write.csv(s1,(paste0("FILEPATH", i,".csv")))  
}


#save results

source("FILEPATH")
save_results_epi(input_dir =paste0("FILEPATH"),
                 input_file_pattern = "{location_id}.csv", 
                 modelable_entity_id = ADDRESS,
                 description = "ADL 10_9 save",
                 measure_id = ADDRESS,
                 decomp_step = "iterative",
                 mark_best = TRUE
)       
