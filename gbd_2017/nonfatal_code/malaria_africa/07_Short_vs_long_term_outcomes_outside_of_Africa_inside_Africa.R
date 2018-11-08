
library(readstata13)

library(doMC)


reclassify = function(data, inCategories, outCategories)
{
  outCategories[ match(data, inCategories)]
}



# Deaths




setwd('FILEPATH')

# EMR from Helena

emr<-read.csv('FILEPATH')


load('FILEPATH')

cfr_betas<-read.csv('FILEPATH')

pop_gbd<-read.csv('FILEPATH')



load('FILEPATH')

new_envelope<-envelope_17_May_2017

#API_all_pf_pv 

load('FILEPATH')



# long term disability ratio #

disability_ratio<-read.dta13('FILEPATH')

disability_ratio_db<-data.frame(ratio=as.numeric(disability_ratio))



# age_id 

age_id<-read.csv('FILEPATH')



load('FILEPATH')

registerDoMC(8)

GBD_2016_results_codcorrect_pv_pf<-GBD_2016_results_codcorrect[GBD_2016_results_codcorrect$location_id %in% unique(API_all_pf_pv$location_id),]

SSA_location_id<- location_metadata$location_id[which(location_metadata$super_region_name=="Sub-Saharan Africa" )]

SSA_location_id<-SSA_location_id[which(SSA_location_id %in% API_all_pf_pv$location_id)]

percentile_10<-SSA_location_id

location_id<-SSA_location_id

all_location_list<-list()


for (i_loc  in 1: length(location_id)){
  

  
  print(i_loc)
  
  country_incidence<-API_all_pf_pv[API_all_pf_pv$location_id==location_id[i_loc],]
  
  country_deaths<-GBD_2016_results_codcorrect_pv_pf[GBD_2016_results_codcorrect_pv_pf$location_id==location_id[i_loc],]
  
  country_deaths$lyas<-paste(country_deaths$location_id,country_deaths$year_id,country_deaths$age_group_id,country_deaths$sex_id,sep='_')
  
  country_env<-as.data.frame(new_envelope[ new_envelope$location_id==location_id[i_loc] & new_envelope$year_id >1979, ])
  
  country_env$lyas<-paste(country_env$location_id,country_env$year_id,country_env$age_group_id,country_env$sex_id,sep='_')
  
  country_pop<-pop_gbd [pop_gbd$location_id==location_id[i_loc] ,]
  
  country_pop$lyas<-paste(country_pop$location_id,country_pop$year_id,country_pop$age_group_id,country_pop$sex_id,sep='_')
  
  split_db<-merge(country_deaths[,c(2,8,11,3,4)],country_env[,c(5,9)],by='lyas')
  
  split_db<-merge(split_db, country_pop[,c(5,7)], by='lyas')
  
  names(split_db)<-c('lyas','year_id','deaths','sex_id','age_group_id','envelop','population')
  
  split_db<-split_db[order(split_db$age_group_id,split_db$sex_id),]
  
  split_db$age_bin<-reclassify(split_db$age_group_id, sort(unique(split_db$age_group_id)),c(rep('infants',4),rep('children',2),rep('adults',17)))
  
  split_db$beta_age<-reclassify(split_db$age_bin,unique(split_db$age_bin),c(0,-0.167772, -0.808675))
  
  split_db$beta_log_deaths<-log(split_db$envelop/split_db$population) *0.622771
  
  split_db$pseudo_cfr<-exp( split_db$beta_log_deaths+split_db$beta_age)/(1+exp(split_db$beta_log_deaths+split_db$beta_age))
  
  split_db$pseudo_cases<-split_db$deaths/ split_db$pseudo_cfr
  
  split_db$ay<-paste(split_db$year_id,split_db$age_bin,sep='_')
  
  country_incidence$ay<-paste(country_incidence$year_id,country_incidence$age_bin,sep='_')
  

  
  MAP_draws<-country_incidence[match(split_db$ay, country_incidence$ay),1:1000]
  
  pseduo_cases<-tapply(split_db$pseudo_cases,split_db$ay,sum)
  
  split_db$tot_pseudo_cases<-pseduo_cases[match(split_db$ay,names(pseduo_cases))]
  
  split_db$fraction<- split_db$pseudo_cases/ split_db$tot_pseudo_cases
  
  pop_ay<-tapply(split_db$population,split_db$ay,sum)
  
  split_db$pop_tot<-pop_ay[match(split_db$ay,names(pop_ay))]
  

  
  MAP_draws_cases<-MAP_draws*matrix(split_db$pop_tot,nrow = 1702,ncol = 1000)
  
  
  
  MAP_draws_age_cases<-MAP_draws_cases*matrix(split_db$fraction,nrow = 1702,ncol = 1000)
  

  
  MAP_draws_age_rate<-MAP_draws_age_cases/matrix(split_db$population,nrow = 1702,ncol = 1000)
  

  

  
  MAP_draws_age_rate[is.na( MAP_draws_age_rate)]<-0
  
  MAP_draws_age_rate[ MAP_draws_age_rate<0]<-0
  
  MAP_draws_age_rate[ MAP_draws_age_rate==Inf]<-0
  
 
  
  duration =matrix(disability_ratio_db[,1],ncol=1000,nrow=1702)
  
  
  MAP_draws_age_rate_short<-MAP_draws_age_rate *duration  
  
  mean_value<-apply(MAP_draws_age_rate_short,1,quantile,0.975)
  
  upper_value<-apply(MAP_draws_age_rate_short,1,quantile,0.5)
  
  lower_value<-apply(MAP_draws_age_rate_short,1,quantile,0.025)
  
  country_all_age_rate_ready<-as.data.frame(cbind(  mean_value, lower_value, upper_value, split_db$age_group_id,split_db$sex_id,split_db$year_id))
  
  country_all_age_rate_ready$location_id<-location_id[i_loc]
  
  names(country_all_age_rate_ready)<-c('mean','lower','upper','age_group_id','sex_id','year_id','location_id')
  
  
  all_location_list[[length(all_location_list)+1]]<-country_all_age_rate_ready[country_all_age_rate_ready$age_group_id<9,]
  

  
  
}
  


#######################
#### Age pattern ####

#####################

age_pattern_all_list<-list()


location_id<-percentile_10

for (i_loc in 1: length(location_id)) {

  print(i_loc)
  
  country_incidence<-API_all_pf_pv[API_all_pf_pv$location_id==location_id[i_loc],]
  
  country_deaths<-GBD_2016_results_codcorrect_pv_pf[GBD_2016_results_codcorrect_pv_pf$location_id==location_id[i_loc],]
  
  country_deaths$lyas<-paste(country_deaths$location_id,country_deaths$year_id,country_deaths$age_group_id,country_deaths$sex_id,sep='_')
  
  country_env<-new_envelope[ new_envelope$location_id==location_id[i_loc] & new_envelope$year_id >1979, ]
  
  country_env$lyas<-paste(country_env$location_id,country_env$year_id,country_env$age_group_id,country_env$sex_id,sep='_')
  
  country_pop<-pop_gbd [pop_gbd$location_id==location_id[i_loc] ,]
  
  country_pop$lyas<-paste(country_pop$location_id,country_pop$year_id,country_pop$age_group_id,country_pop$sex_id,sep='_')
  
  split_db<-merge(country_deaths[,c(2,8,11,3,4)],country_env[,c(5,9)],by='lyas')
  
  split_db<-merge(split_db, country_pop[,c(5,7)], by='lyas')
  
  names(split_db)<-c('lyas','year_id','deaths','sex_id','age_group_id','envelop','population')
  
  split_db<-split_db[order(split_db$age_group_id,split_db$sex_id),]
  
  split_db$age_bin<-reclassify(split_db$age_group_id, sort(unique(split_db$age_group_id)),c(rep('infants',4),rep('children',2),rep('adults',17)))
  
  split_db$beta_age<-reclassify(split_db$age_bin,unique(split_db$age_bin),c(0,-0.167772, -0.808675))
  
  split_db$beta_log_deaths<-log(split_db$envelop/split_db$population) *0.622771
  
  split_db$pseudo_cfr<-exp( split_db$beta_log_deaths+split_db$beta_age)/(1+exp(split_db$beta_log_deaths+split_db$beta_age))
  
  split_db$pseudo_cases<-split_db$deaths/ split_db$pseudo_cfr
  
  split_db$ay<-paste(split_db$year_id,split_db$age_bin,sep='_')
  
  country_incidence$ay<-paste(country_incidence$year_id,country_incidence$age_bin,sep='_')
  

  
  pseduo_cases<-tapply(split_db$pseudo_cases,split_db$ay,sum)
  
  split_db$tot_pseudo_cases<-pseduo_cases[match(split_db$ay,names(pseduo_cases))]
  
  split_db$fraction<- split_db$pseudo_cases/ split_db$tot_pseudo_cases
  
 
  age_pattern_all_list[[length(age_pattern_all_list)+1]]<-split_db$fraction


}



country_all_db<-do.call(cbind,age_pattern_all_list)








load('FILEPATH')


all_location_id<- epi_demographic$location_id[-c(which(epi_demographic$location_ids %in% SSA_location_id ))]

location_id<-all_location_id#unique(GBD_2016_results_codcorrect_pv_pf$location_id)

location_id<-location_id[which(location_id %in% unique(API_all_pf_pv$location_id) )]

location_id<-location_id[which(location_id %in% new_envelope$location_id)]

location_done<-c(SSA_location_id,location_id)


for (i_loc  in 1: length(location_id)) {
  
  
  print(i_loc)
  
  country_incidence<-API_all_pf_pv[API_all_pf_pv$location_id==location_id[i_loc],]
  
  a<-country_incidence[1:20,1:500]
  
  length(which(rowSums(a-a[,1])==0))>0
  
  if(length(which(rowSums(a-a[,1])==0))>0){
    
    mat_fraction<-country_all_db[,sample(c(1:7),1000,replace = T)]
    
  }else{
    
    
    
    mat_fraction<-matrix(rowMeans(country_all_db,na.rm=T),nrow=1702,ncol=1000)
    
    
  }
  
  country_deaths<-GBD_2016_results_codcorrect_pv_pf[GBD_2016_results_codcorrect_pv_pf$location_id==location_id[i_loc],]
  
  country_deaths$lyas<-paste(country_deaths$location_id,country_deaths$year_id,country_deaths$age_group_id,country_deaths$sex_id,sep='_')
  
  country_env<-new_envelope[ new_envelope$location_id==location_id[i_loc] & new_envelope$year_id >1979, ]
  
  country_env$lyas<-paste(country_env$location_id,country_env$year_id,country_env$age_group_id,country_env$sex_id,sep='_')
  
  country_pop<-pop_gbd [pop_gbd$location_id==location_id[i_loc] ,]
  
  country_pop$lyas<-paste(country_pop$location_id,country_pop$year_id,country_pop$age_group_id,country_pop$sex_id,sep='_')
  
  split_db<-merge(country_deaths[,c(2,8,11,3,4)],country_env[,c(5,9)],by='lyas')
  
  split_db<-merge(split_db, country_pop[,c(5,7)], by='lyas')
  
  names(split_db)<-c('lyas','year_id','deaths','sex_id','age_group_id','envelop','population')
  
  split_db<-split_db[order(split_db$age_group_id,split_db$sex_id),]
  
  split_db$age_bin<-reclassify(split_db$age_group_id, sort(unique(split_db$age_group_id)),c(rep('infants',4),rep('children',2),rep('adults',17)))
  
  split_db$beta_age<-reclassify(split_db$age_bin,unique(split_db$age_bin),c(0,-0.167772, -0.808675))
  
  split_db$beta_log_deaths<-log(split_db$envelop/split_db$population) *0.622771
  
  split_db$pseudo_cfr<-exp( split_db$beta_log_deaths+split_db$beta_age)/(1+exp(split_db$beta_log_deaths+split_db$beta_age))
  
  split_db$pseudo_cases<-split_db$deaths/ split_db$pseudo_cfr
  
  split_db$ay<-paste(split_db$year_id,split_db$age_bin,sep='_')
  
  country_incidence$ay<-paste(country_incidence$year_id,country_incidence$age_bin,sep='_')
  

  
  MAP_draws<-country_incidence[match(split_db$ay, country_incidence$ay),1:1000]
  
  pseduo_cases<-tapply(split_db$pseudo_cases,split_db$ay,sum)
  
  split_db$tot_pseudo_cases<-pseduo_cases[match(split_db$ay,names(pseduo_cases))]
  
  split_db$fraction<- split_db$pseudo_cases/ split_db$tot_pseudo_cases
  
  pop_ay<-tapply(split_db$population,split_db$ay,sum)
  
  split_db$pop_tot<-pop_ay[match(split_db$ay,names(pop_ay))]
  
  
  
  MAP_draws_cases<-MAP_draws*matrix(split_db$pop_tot,nrow = 1702,ncol = 1000)
  

  
  MAP_draws_age_cases<-MAP_draws_cases*mat_fraction
  
 
  
  MAP_draws_age_rate<-MAP_draws_age_cases/matrix(split_db$population,nrow = 1702,ncol = 1000)
  
  
  
  MAP_draws_age_rate[is.na( MAP_draws_age_rate)]<-0
  
  MAP_draws_age_rate[ MAP_draws_age_rate<0]<-0
  
  MAP_draws_age_rate[ MAP_draws_age_rate==Inf]<-0
  
  
  duration =matrix(disability_ratio_db[,1],ncol=1000,nrow=1702)
  
  
  MAP_draws_age_rate_short<-MAP_draws_age_rate *duration  #*matrix(duration,nrow = 1702,ncol = 1000)
  
  mean_value<-apply(MAP_draws_age_rate_short,1,quantile,0.975)
  
  upper_value<-apply(MAP_draws_age_rate_short,1,quantile,0.5)
  
  lower_value<-apply(MAP_draws_age_rate_short,1,quantile,0.025)
  
  country_all_age_rate_ready<-as.data.frame(cbind(  mean_value, lower_value, upper_value, split_db$age_group_id,split_db$sex_id,split_db$year_id))
  
  country_all_age_rate_ready$location_id<-location_id[i_loc]
  
  names(country_all_age_rate_ready)<-c('mean','lower','upper','age_group_id','sex_id','year_id','location_id')
  
  
  all_location_list[[length(all_location_list)+1]]<-country_all_age_rate_ready[country_all_age_rate_ready$age_group_id<9,]
  

  
}
  



##### NO malaria #####


add_no_malaria<-0

if(add_no_malaria==1){

country_all_age_rate_ready<-all_location_list[[1]]



no_malaria_location<-epi_demographic$location_ids[ -c(which(epi_demographic$location_ids %in% location_done))]

country_all_age_rate_ready[,1:3]<-0

location_id<-no_malaria_location

for (i_loc in 1: length(location_id))  {
  
  print(i_loc)
  
  country_all_age_rate_ready$location_id<-location_id[i_loc]
  
  all_location_list[[length(all_location_list)+1]]<-country_all_age_rate_ready[country_all_age_rate_ready$age_group_id<9,]
  

  
}


}


all_location_db<-do.call(rbind,all_location_list)

#### extra columns ###

all_location_db2<-all_location_db

all_location_db<-all_location_db[,which(names(all_location_db) %in% names(emr))]


needed_columns<-names(emr)[which(is.na(match(names(emr),names(all_location_db) )))]

first_row<-emr[1,needed_columns]

empty_column<-names(first_row)[which(is.na(first_row))]

names(first_row)[-c(which(is.na(first_row)))]

all_location_db$bundle_id<-51 
all_location_db$nid<-150244                    
all_location_db$input_type<-NA            
all_location_db$source_type<-"Mixed or estimation"  
all_location_db$smaller_site_unit<-0    
all_location_db$sex_issue<-0              
all_location_db$year_issue<-0            
all_location_db$age_issue<-0               
all_location_db$age_demographer<-0       
all_location_db$measure<-"incidence"             
all_location_db$unit_type<-'Person'              
all_location_db$unit_value_as_published<-1
all_location_db$measure_issue<-0          
all_location_db$measure_adjustment<-0     
all_location_db$uncertainty_type<-'Confidence interval'      
all_location_db$uncertainty_type_value <-95
all_location_db$urbanicity_type<- "Mixed/both"       
all_location_db$recall_type<-'Not Set'            
all_location_db$extractor<-'dbisanz'             
all_location_db$is_outlier<-0            
all_location_db$representative_name<-'Nationally and subnationally representative'

all_location_db$age_start<-age_id$age_start[match(all_location_db2$age_group_id,age_id$age_group_id)]

all_location_db$age_end<-age_id$age_end[match(all_location_db2$age_group_id,age_id$age_group_id)]

all_location_db$sex<-reclassify(all_location_db2$sex_id,c(1,2),c('Male','Female'))

all_location_db$year_start<-all_location_db2$year_id

all_location_db$year_end<-all_location_db2$year_id


empty_mat<-as.data.frame(matrix(NA, nrow = dim(all_location_db)[1], ncol = length(empty_column)))

names(empty_mat)<-empty_column


all_location_db_epi<-cbind(all_location_db,empty_mat)

all_location_db_epi<-all_location_db_epi[,names(emr)]

emr_only_malaria<-emr[which(emr$location_id %in% unique(all_location_db_epi$location_id)), ]

all_location_db_epi_emr<-rbind(all_location_db_epi,emr)

all_location_db_epi_emr$bundle_id<-1446 
all_location_db_epi_emr$nid<-150244 


# final check#


upper_to_change<-which(all_location_db_epi_emr$upper < all_location_db_epi_emr$mean)

all_location_db_epi_emr$upper[upper_to_change]<-all_location_db_epi_emr$mean[upper_to_change]


# 

all_location_db_epi_emr$upper[which(all_location_db_epi_emr$upper ==0)]<-0.000000000011

all_location_db_epi_emr$mean[which(all_location_db_epi_emr$mean ==0)]<-0.00000000001

all_location_db_epi_emr$lower[which(all_location_db_epi_emr$lower ==0)]<-0.000000000009



equal_values<-which(all_location_db_epi_emr$upper==all_location_db_epi_emr$mean)

all_location_db_epi_emr$upper[equal_values]<-all_location_db_epi_emr$upper[equal_values]+0.0005


all_location_db_epi_emr_year_adjusted<-all_location_db_epi_emr[which(all_location_db_epi_emr$year_start %in% c(1995,2000,2005,2010,2015,2016)),]


###############


write.csv(all_location_db_epi_emr_year_adjusted,file='FILEPATH',row.names=F,na='')







###################################################################################
###################################################################################
###################################################################################


##### Create draws 1000 ####

reclassify = function(data, inCategories, outCategories)
{
  outCategories[ match(data, inCategories)]
}


ihme_code<-read.csv('FILEPATH')


# non-carto #

master.country.list<-as.vector(read.csv('FILEPATH')[,2])

ewan_location_id<-ihme_code$loc_id[match(master.country.list,ihme_code$ihme_lc_id)]

ewan_iso3<-as.character(ihme_code$ihme_lc_id[match(master.country.list,ihme_code$ihme_lc_id)])

# infants



load('FILEPATH')


API_timeseries_realisations_array_infants<-API_timeseries_realisations_array_infants[order(API_timeseries_realisations_array_infants$location_id),]

API_infants_list<-list()


for (i in unique(API_timeseries_realisations_array_infants$location_id)){
  
  country_incidence<-API_timeseries_realisations_array_infants[which(API_timeseries_realisations_array_infants$location_id==i),]
  
  year_draws<-t(country_incidence[,2:37])/1000
  
  year_draws<-rbind(year_draws,year_draws[36,])
  
  year_draws_db<-as.data.frame(t(apply(year_draws,1,as.numeric)))
  
  year_draws_db$year_id<-1980:2016
  
  year_draws_db$location_id<-i
  
  year_draws_db$age_bin<-'infants'
  
  API_infants_list[[length(API_infants_list)+1]]<-year_draws_db
  
}
  
API_infants_db<-do.call(rbind,API_infants_list)
  

# children


load('FILEPATH')

API_timeseries_realisations_array_children<-API_timeseries_realisations_array_children[order(API_timeseries_realisations_array_children$location_id),]

API_children_list<-list()


for (i in unique(API_timeseries_realisations_array_children$location_id)){
  
  country_incidence<-API_timeseries_realisations_array_children[which(API_timeseries_realisations_array_children$location_id==i),]
  
  year_draws<-t(country_incidence[,2:37])/1000
  
  year_draws<-rbind(year_draws,year_draws[36,])
  
  year_draws_db<-as.data.frame(t(apply(year_draws,1,as.numeric)))
  
  year_draws_db$year_id<-1980:2016
  
  year_draws_db$location_id<-i
  
  year_draws_db$age_bin<-'children'
  
  API_children_list[[length(API_children_list)+1]]<-year_draws_db
  
}

API_children_db<-do.call(rbind,API_children_list)



# adults


load('FILEPATH')

API_timeseries_realisations_array_adults<-API_timeseries_realisations_array_adults[order(API_timeseries_realisations_array_adults$location_id),]

API_adults_list<-list()


for (i in unique(API_timeseries_realisations_array_adults$location_id)){
  
  country_incidence<-API_timeseries_realisations_array_adults[which(API_timeseries_realisations_array_adults$location_id==i),]
  
  year_draws<-t(country_incidence[,2:37])/1000
  
  year_draws<-rbind(year_draws,year_draws[36,])
  
  year_draws_db<-as.data.frame(t(apply(year_draws,1,as.numeric)))
  
  year_draws_db$year_id<-1980:2016
  
  year_draws_db$location_id<-i
  
  year_draws_db$age_bin<-'adults'
  
  API_adults_list[[length(API_adults_list)+1]]<-year_draws_db
  
}

API_adults_db<-do.call(rbind,API_adults_list)


API_non_carto_db<-rbind(API_infants_db,API_children_db,API_adults_db)

names(API_non_carto_db)[1:1000]<-paste('draws',0:999,sep='_')

#############################################################################################################
#############################################################################################################
#############################################################################################################
#############################################################################################################


# Tim subnational #


# Arabia

incidence_arabia<-read.csv('FILEPATH')[,1:5]

# Brazil

incidence_brazil<-read.csv('FILEPATH')[,1:5]

# China

incidence_china<-read.csv('FILEPATH')[,1:5]

# India

incidence_india<-read.csv('FILEPATH')[,1:5]

incidence_india$Rate[which(incidence_india$Rate<0)]<-0

# Indonesia

incidence_indonesia<-read.csv('FILEPATH')[,1:5]

# Mexico

incidence_mexico<-read.csv('FILEPATH')[,1:5]

# South Africa

incidence_southafrica<-read.csv('FILEPATH')[,1:5]


API_Tim_subnational<-rbind(incidence_arabia,
      incidence_brazil,
      incidence_china,
      incidence_india,
      incidence_indonesia,
      incidence_mexico,
      incidence_southafrica
      )

API_Tim_subnational$Age_Bin<-reclassify(API_Tim_subnational$Age_Bin,unique(API_Tim_subnational$Age_Bin),c('infants','children','adults'))



#############################################################################################################
#############################################################################################################
#############################################################################################################
#############################################################################################################



# carto #


incidence_carto<-read.csv('FILEPATH')[,1:5]

incidence_carto_clean<-incidence_carto[-c(which(incidence_carto$Rate<0)),]

incidence_carto_clean<-incidence_carto_clean[-c(which(incidence_carto_clean$ID<0)),]

incidence_carto_subnational<-rbind(incidence_carto_clean,API_Tim_subnational)

country_non_carto<-unique(incidence_carto_subnational$ID)[which(unique(incidence_carto_subnational$ID) %in% ewan_location_id)]

incidence_carto_subnational<-droplevels(incidence_carto_subnational[-c(which(incidence_carto_subnational$ID %in% country_non_carto)),])



API_carto_subnational<-list()

for (i_loc in unique(incidence_carto_subnational$ID)){
  
  print(i_loc)
  
  country_incidence<-incidence_carto_subnational[incidence_carto_subnational$ID==i_loc,]
  
  array_draws<-tapply(country_incidence$Rate,list(country_incidence$Year,country_incidence$Realization,country_incidence$Age_Bin),mean)
  
  
  infants_draws<-array_draws[1:36,,3]
  
  for(i in 1:9){
    
    infants_draws<-  cbind(infants_draws,infants_draws[,1:100])
    
  }
  
  infants_draws_db<-as.data.frame(rbind(infants_draws,infants_draws[36,]))
  
  infants_draws_db$year_id<-1980:2016
  
  infants_draws_db$location_id<-i_loc
  
  infants_draws_db$age_bin<-'infants'
  
  
  children_draws<-array_draws[1:36,,2]  
  
  for(i in 1:9){
    
    children_draws<-  cbind(children_draws,children_draws[,1:100])
    
  }
  
  children_draws_db<-as.data.frame(rbind(children_draws,children_draws[36,]))
    
  children_draws_db$year_id<-1980:2016
  
  children_draws_db$location_id<-i_loc
  
  children_draws_db$age_bin<-'children'
  
  
  adults_draws<-array_draws[1:36,,1]
  
  for(i in 1:9){
    
    adults_draws<-  cbind(adults_draws,adults_draws[,1:100])
    
  }
  
  adults_draws_db<-as.data.frame(rbind(adults_draws,adults_draws[36,]))
  
  adults_draws_db$year_id<-1980:2016
  
  adults_draws_db$location_id<-i_loc
  
  adults_draws_db$age_bin<-'adults'
  
  
  all_draws_db<-rbind(infants_draws_db,children_draws_db,adults_draws_db)
  
  API_carto_subnational[[length(API_carto_subnational)+1]]<- all_draws_db
  
}

API_carto_subnational_db<-do.call(rbind,API_carto_subnational)

names(API_carto_subnational_db)[1:1000]<-paste('draws',0:999,sep='_')


API_all_location_year_age_bin<-rbind(API_non_carto_db,API_carto_subnational_db)

API_all_location_year_age_bin2<-API_all_location_year_age_bin


# ### Adjust PV ###
# 
 incidence_pv_pf<-read.csv('FILEPATH')



country_100_pv_iso3<-c('ARG','ARM','AZE','CRI','SLV','GEO','IRQ','KGZ','PRK','PRY','KOR','TUR','TKM','UZB')#'MEX'

country_100_pv_location_id<-ihme_code$loc_id[match(country_100_pv_iso3,ihme_code$ihme_lc_id)]

incidence_pv_pf_100_pv<-incidence_pv_pf[incidence_pv_pf$location_id %in% country_100_pv_location_id,]

API_100_pv<-API_all_location_year_age_bin[API_all_location_year_age_bin$location_id %in% country_100_pv_location_id,]

country_pv_lits<-list()

for (i in country_100_pv_location_id){

country_db<-API_100_pv[API_100_pv$location_id==i,]  

country_db<-country_db[order(country_db$age_bin,country_db$year_id),]
  
country_incidence<-incidence_pv_pf_100_pv[incidence_pv_pf_100_pv$location_id==i,]

country_incidence<-country_incidence[order(country_incidence$age_group_id,country_incidence$year_id),]

pv_rate<-as.numeric(country_incidence$cases/country_incidence$pop_tot)



pv_rate[which(pv_rate>-100)]<-0

pv_rate[which(pv_rate==Inf)]<-0

pv_rate[which(is.na(pv_rate))]<-0

rep_pv_deaths_mat<-matrix(pv_rate,ncol=1000,nrow =length(pv_rate))

country_db[,grep('draws',names(country_db))]<-rep_pv_deaths_mat

country_pv_lits[[length(country_pv_lits)+1]]<-country_db

}


API_100_pv2<-do.call(rbind,country_pv_lits)

API_all_location_year_age_bin_no_100_pv<-API_all_location_year_age_bin[-c(which(API_all_location_year_age_bin$location_id %in% country_100_pv_location_id)),]

API_all_pf_pv<-rbind(API_all_location_year_age_bin_no_100_pv,API_100_pv2)

api_draws<-API_all_pf_pv[,grep('draws',names(API_100_pv))]



save(API_all_pf_pv,file='FILEPATH')

