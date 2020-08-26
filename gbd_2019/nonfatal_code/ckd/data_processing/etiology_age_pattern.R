# ---HEADER--------------------------------------------------------------------------------------------------
# Project: Non-fatal CKD Etiology Splits 
# Purpose: Impose age structure from AUSNZ Registry onto all other datapoints from registries 
#------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
# clear workspace
rm(list=ls())

# set runtime configuration 
# load packages 
require(data.table)
require(ggplot2)

# Source functions 
source(paste0('FILEPATH/function_lib.R'))
# source(paste0(h_root,"ckd/ckd_data_processing/format_epi_upload.R"))
source_shared_functions(c("upload_epi_data","get_epi_data"))

# set filepaths 
indir<-paste0('FILEPATH')
outdir<-paste0('FILEPATH')
#-------------------------------------------------------------------------------------------------------------

# ---DATA PROCESSING------------------------------------------------------------------------------------------
 
etiologies<-c("DM","DM1","DM2","HTN","GN","OTH")
for (etiology in etiologies){
  i<-which(etiologies==etiology)
  bundle_ids<-c(551,3110,3113,552,553,554)

  #import data
  assign(etiology,get_epi_data(bundle_id = bundle_ids[i]))
  
  #remove age-specific registry data 
  assign(etiology,get(etiology)[age_start==0|age_start==20])
  assign(etiology,get(etiology)[age_end==99])
  
  #divide into rows marked 20-99 and rows marked 0-99 
  assign(paste0(etiology,'_0_99'),get(etiology)[age_start==0])
  assign(paste0(etiology,'_20_99'),get(etiology)[age_start==20])
  
  #expand data in 0-99 groups for each age group in AUS/NZ age-pattern data from 0-100
  age_starts_0_99<-c(0,5,15,25,35,45,55,65,75,85)
  age_ends_0_99<-c(4,14,24,34,44,54,64,74,84,100)
  etiology_list_0_99<-list()
  for (i in 1:10){
    assign(paste0(etiology,'_0_99_',i),copy(get(paste0(etiology,'_0_99'))))
    assign(paste0(etiology,'_0_99_',i),get(paste0(etiology,'_0_99_',i))[,age_start:=age_starts_0_99[i]])
    assign(paste0(etiology,'_0_99_',i),get(paste0(etiology,'_0_99_',i))[,age_end:=age_ends_0_99[i]])
    etiology_list_0_99[[i]]<-get(paste0(etiology,'_0_99_',i))
  }
  
  #duplicate data in 0-99 groups 10 times for each age group in AUS/NZ age-pattern data from 0-100
  age_starts_20_99<-c(15,25,35,45,55,65,75,85)
  age_ends_20_99<-c(24,34,44,54,64,74,84,100)
  etiology_list_20_99<-list()
  for (i in 1:8){
    assign(paste0(etiology,'_20_99_',i),copy(get(paste0(etiology,'_20_99'))))
    assign(paste0(etiology,'_20_99_',i),get(paste0(etiology,'_20_99_',i))[,age_start:=age_starts_20_99[i]])
    assign(paste0(etiology,'_20_99_',i),get(paste0(etiology,'_20_99_',i))[,age_end:=age_ends_20_99[i]])
    etiology_list_20_99[[i]]<-get(paste0(etiology,'_20_99_',i))
  }
  
  assign(paste0(etiology,'_0_99'),rbindlist(etiology_list_0_99))
  
  assign(paste0(etiology,'_20_99'),rbindlist(etiology_list_20_99))
}

all_data_0_99<-rbind(DM_0_99,HTN_0_99,GN_0_99,OTH_0_99)
all_data_20_99<-rbind(DM_20_99,HTN_20_99,GN_20_99,OTH_20_99)
#-------------------------------------------------------------------------------------------------------------

# ---MERGE AGE TABLE------------------------------------------------------------------------------------------
#import age-specific proportion maps 
age_structure_0_99<-fread(paste0("FILEPATH/aus_nz_age_pattern_0_99.csv"))
age_structure_20_99<-fread(paste0("FILEPATH/aus_nz_age_pattern_20_99.csv"))

#drop numerator and denominator cols from age_structure data tables
age_structure_0_99[,c("numerator","denominator"):=NULL]
age_structure_20_99[,c("numerator","denominator"):=NULL]

by_vars<-c('age_start','age_end','bundle_id')
all_data_0_99<-merge(all_data_0_99,age_structure_0_99[,-c("year_start","year_end"),with=F],by=by_vars)
all_data_20_99<-merge(all_data_20_99,age_structure_20_99[,-c("year_start","year_end"),with=F],by=by_vars)
#-------------------------------------------------------------------------------------------------------------

# ---ADJUST MEAN, CASES, & SAMPLE SIZE-------------------------------------------------------------------------
all_data_0_99[,mean:=mean*ratio]
all_data_20_99[,mean:=mean*ratio]

all_data_0_99[,cases:=cases*ratio]
all_data_20_99[,cases:=cases*ratio]

all_data_0_99[,sample_size:=cases/mean]
all_data_20_99[,sample_size:=cases/mean]

#remove ratio and etiology cols
all_data_0_99[,c("ratio","Etiology"):=NULL,with=F]
all_data_20_99[,c("ratio","Etiology"):=NULL,with=F]

#clear upper, lower, standard error, and uncertainty type value
na_cols<-c('seq','effective_sample_size','upper','lower','standard_error','uncertainty_type_value','uncertainty_type')
all_data_0_99[,(na_cols):=NA,with=F]
all_data_20_99[,(na_cols):=NA,with=F]

#add response rate col, needed for upload
all_data_0_99[,response_rate:=NA]
all_data_20_99[,response_rate:=NA]

#append the age data sets
all_data<-rbind(all_data_20_99,all_data_0_99)
#-------------------------------------------------------------------------------------------------------------

# ---RESCALE TO 1---------------------------------------------------------------------------------------------
# Calculate etiology total for each source by each age, sex, year, location
by_vars<-c('nid','location_id','sex','year_start','group_review','year_end','measure','location_id','age_start','age_end')
all_data[,'denom':=lapply(.SD,sum),.SDcols='mean',by=by_vars,with=F]
all_data[,scaled_mean:=mean/denom]
all_data[,test:=lapply(.SD,sum),.SDcols='scaled_mean',by=by_vars]

#Finish formatting 
all_data[,extractor:='USERNAME']
all_data[,note_sr1:='data manipulated by USERNAME  see detailed data manipulation documentation & code in ckd folder on J drive']

all_data[,mean:=scaled_mean]
all_data[,cases:=sample_size*mean]

# Drop data where cases is NA  
all_data<-all_data[is.na(cases)==F]

all_data[,c('denom','scaled_mean','test'):=NULL,with=F]
all_data[,input_type:=NA]

gg<-ggplot(data=all_data[location_id==55&sex=="Both"], aes(x=age_start,y=mean,group=bundle_id,color = factor(bundle_id)))+
  geom_point() + 
  scale_shape(solid = T)+
  geom_smooth(aes(color = factor(bundle_id)))
print(gg)
dev.off()

# ---EXPORT FOR UPLOAD----------------------------------------------------------------------------------------
write.csv(all_data[bundle_id==551],paste0('new_DM_esrd_proportions_split.csv'),row.names = F,na='')
write.csv(all_data[bundle_id==552],paste0('new_GN_esrd_proportions_split.csv'),row.names = F,na='')
write.csv(all_data[bundle_id==553],paste0('new_HTN_esrd_proportions_split.csv'),row.names = F,na='')
write.csv(all_data[bundle_id==554],paste0('new_OTH_esrd_proportions_split.csv'),row.names = F,na='')
#-------------------------------------------------------------------------------------------------------------


