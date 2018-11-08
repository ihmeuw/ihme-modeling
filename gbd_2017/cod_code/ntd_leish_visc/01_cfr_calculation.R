#vl death processing

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}
source(sprintf("FILEPATH/save_results_epi.R",prefix))
source(sprintf("FILEPATH/get_cod_data.R",prefix))
source(sprintf("FILEPATH/get_population.R",prefix))
source(sprintf("FILEPATH/get_ids.R",prefix))

#load in vl death data
vl_deaths<-get_cod_data(cause_id=348)
vl_master<-vl_deaths


#subset to non-zeroes
vl_deaths<-subset(vl_deaths, vl_deaths$age_group_id!=22)
vl_deaths<-subset(vl_deaths, vl_deaths$deaths>0)

#import vl georestrictions and drop restricted locations

leish_geo<-read.csv(sprintf("FILEPATH/geo_restrict_vl.csv",prefix), stringsAsFactors = FALSE)
dataset<-leish_geo[,1:48]


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

unique_vl_locations<-unique(presence_list$loc_id)

test<-which(vl_deaths$location_id %in% unique_vl_locations)

vl_deaths<-vl_deaths[test,]

# #trim to age_group_id, data_type, location_id, location_name, year, sex, raw_count, deaths
# vl_deaths_processing<-data.frame(location_id=vl_deaths$location_id,
#                                  location_name=vl_deaths$location_name,
#                                  year=vl_deaths$year,
#                                  age_group_id=vl_deaths$age_group_id,
#                                  sex=vl_deaths$sex,
#                                  COD_DB_deaths=vl_deaths$deaths,
#                                  rate=vl_deaths$rate,
#                                  study_deaths=vl_deaths$study_deaths,
#                                  sample_size=vl_deaths$sample_size)
vl_deaths_processing<-vl_deaths

vl_deaths_subset<-subset(vl_deaths_processing, vl_deaths_processing$year %in% c(1990:2017) )

#drop Brazil
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=135)
#drop Ethiopia
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=179)
#drop India
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=163)
#drop China
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=6)
#drop Mexico
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=130)
#drop Ukraine
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=63)
#drop Kenya
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=180)

#drop select India subnationals
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4843)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4844)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4851)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4853)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4854)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4855)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4856)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4857)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4859)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4860)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4865)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4868)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4869)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4870)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4873)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4874)
vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$location_id!=4875)



#eliminate cf_raw==0
#vl_deaths_subset<-subset(vl_deaths_subset, vl_deaths_subset$cf_raw!=0)
start_time<-Sys.time()
#pull demographics and calculate CFR
for (i in 1:nrow(vl_deaths_subset)){

  #load in file
  incidence_file<-read.csv(paste0(prefix,'FILEPATH',
                         vl_deaths_subset$location_id[i],'_',vl_deaths_subset$year[i],'.csv'))
  incidence_file<-subset(incidence_file, incidence_file$measure_id==6)

  vl_deaths_subset$incidence_rate[i]<-mean(t(incidence_file[incidence_file$age_group_id==vl_deaths_subset$age_group_id[i] & incidence_file$sex_id==vl_deaths_subset$sex[i],8:1007]))
  # population_number<-get_population(age_group_id=vl_deaths_subset$age_group_id[i],
  #                                   sex_id=vl_deaths_subset$sex[i],
  #                                   year_id=vl_deaths_subset$year[i],
  #                                   location_id=vl_deaths_subset$location_id[i])
  # subset_population<-subset(population_number, population_number$age_group_id==vl_deaths_subset$age_group_id[i] & population_number$sex_id==vl_deaths_subset$sex[i])
  specific_population<-vl_deaths_subset$pop[i]
  vl_deaths_subset$case_estimate[i]<-specific_population*vl_deaths_subset$incidence_rate[i]
  vl_deaths_subset$cfr[i]<-vl_deaths_subset$deaths[i]/vl_deaths_subset$case_estimate[i]
  #print(paste0("Processed ",i, " of ", nrow(vl_deaths_subset)))
  if(i %in% seq(1,95001,1000)){print(paste0(i, " at ", Sys.time()))}
}
end_time<-Sys.time()
#for now, drop all CFR greater than one
vl_deaths_output<-subset(vl_deaths_subset, vl_deaths_subset$cfr<1)

#curate for ST-GPR
st_gpr_output<-data.frame(me_name=rep("ntd_vl_cfr",nrow(vl_deaths_output)),
                          location_id=vl_deaths_output$location_id,
                          year_id=vl_deaths_output$year,
                          age_group_id=vl_deaths_output$age_group_id,
                          sex_id=vl_deaths_output$sex,
                          data=vl_deaths_output$cfr,
                          variance=rep(0.05,nrow(vl_deaths_output)),
                          sample_size=vl_deaths_output$sample_size,
                          nid=vl_deaths_output$nid)


#save outputs
write.csv(st_gpr_output, file=paste0(prefix, "FILEPATH/cfr_dataset_14June.csv"))
st_gpr_output<-read.csv(file=paste0(prefix, "FILEPATH/cfr_dataset_14June.csv"))
write.csv(vl_deaths_output, file=paste0(prefix, "FILEPATH/cfr_dataset_14June_fullmetadata.csv"))
