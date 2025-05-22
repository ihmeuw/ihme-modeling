#combine the mediated and unmediated total PAFs for bone lead

rm(list=ls())

library(readr)
source("FILEPATH/get_draws.R")
source("FILEPATH/get_cause_metadata.R")

cause_set<-2
release<-16
causes_med<-c(495, 496, 497, 498, 500, 501, 502, 998, 591, 592, 593)#for mediated PAFs
causes_cvd<-c(493) #for the total (mediated + unmediated) IHD pafs
years<-c(1990:2024)
n_draws<-250
med_v<-887424 #model version id for the mediated PAFs that were ran through the calculator
version<-26 #version from USERNAME's PAF calculator
total_filepath<-paste0("FILEPATH",release,"FILEPATH",version,"FILEPATH")
gbd_cycle<-"GBD2022" #gbd name for filepath
filename<-"GBD23_bone_lead_combo_pafs.csv"

# Total CVD PAFs ########################################
unmed_pafs_input<-rbindlist(lapply(list.files(total_filepath,pattern = ".csv",full.names = T),fread),fill=T)

#add in causes
unmed_pafs_causes<-data.table()

for(x in causes_cvd){
  print(x)
  temp<-copy(unmed_pafs_input)
  temp[,cause_id:=x]
  
  unmed_pafs_causes<-rbind(temp,unmed_pafs_causes)
}

#now make a copy for YLLs
unmed_pafs_ylls<-copy(unmed_pafs_causes)[,measure_id:=4]

unmed_pafs<-rbind(unmed_pafs_causes,unmed_pafs_ylls)

# Mediated PAFs #####################################
#only grab the years and causes that we need
year_cause <- expand.grid(list1 = seq_along(years), list2 = seq_along(causes_med))

year_cause_list<-apply(year_cause, 1, function(idx) list(years[[idx[1]]], causes_med[[idx[2]]]))

med_pafs<-rbindlist(lapply(year_cause_list,function(args) {
  y<-args[[1]]
  cause<-args[[2]]

  print(y)
  print(cause)

  get_draws(gbd_id_type = c("rei_id","cause_id"), gbd_id = c(243,cause), source = "paf", release_id = release, downsample = T, n_draws = n_draws, year_id=y,
            num_workers = 9,version_id = med_v)
}),fill=T)

#only keep the columns that we need
med_pafs<-med_pafs[,-c("rei_id","modelable_entity_id","model_version_id","metric_id")]

# rbind ####################
pafs<-rbind(med_pafs,unmed_pafs)


#save to csv
write_excel_csv(pafs,paste0("FILEPATH/",gbd_cycle,"/FIELAPTH/",filename))

