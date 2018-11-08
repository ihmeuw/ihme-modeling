# Purpose: Compare MarketScan and Hospital data to determine appropriate min, max, and mean for DisMod cv

# SETUP---------------------------------------------------------------------------------------------------

# clear 
rm(list=ls())

# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
} else { 
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}

# load packages 
require(pacman)
p_load(data.table,RMySQL,openxlsx,magrittr,readxl,ggplot2)

# source functions
source(paste0(h_root,"FILEPATH/helper_functions.R"))
shared_functions(c("get_envelope","get_population","get_epi_data","upload_epi_data"))

# settings
bundle <- 130
acause <- "resp_interstitial"
version_inp <- "v8_2018_05_15"
version_claims <- "v8_fixed_ages_2018_05_10"

# set directories
doc_dir<-paste0('FILEPATH')

# PREP DATA ---------------------------------------------------------------

# inpatient
inp <- read.xlsx(paste0('FILEPATH')) %>% as.data.table
inp <- inp[,c("location_id","location_name","sex","age_start","age_end","year_start","year_end","nid","inpt_primarydx_admit_rate","cases","sample_size")]
setnames(inp,"inpt_primarydx_admit_rate","mean")

# claims
claims <- read_excel(paste0('FILEPATH')) %>% as.data.table
claims <- claims[location_id!="Taiwan",c("location_id","location_name","sex","age_start","age_end","year_start","year_end","nid","mean","cases","sample_size")]
claims <- claims[year_start!=2000,]

# set terminal age group age_end to 99
inp[age_start==95,age_end:=99]
claims[age_start==95,age_end:=99]
  
# only keep locs in both data sets
keep_locs<-intersect(unique(claims[,location_id]),unique(inp[,location_id]))
claims<-claims[location_id%in%keep_locs]
inp<-inp[location_id%in%keep_locs]
  
# PROCESS MARKETSCAN ------------------------------------------------------

#sum cases and sample size by age over location to get prevalence for US  
claims[,c("cases_total","sample_size_total"):=lapply(.SD,sum),.SDcols=c("cases","sample_size"),by=c("age_start","age_end","year_start","year_end")]
# drop loc_id, loc name, cases, and sample_size now that we've collapsed across locations 
claims[,c("location_id","location_name","cases","sample_size","mean"):=NULL]  
# calculate new mean
claims[,mean:=cases_total/sample_size_total]
# calculate new standard error 
claims[,standard_error:=sqrt(mean*(1-mean)/sample_size_total)]
# subset to unique rows by year and sex 
claims<-unique(claims)
# drop cases and sample size 
claims[,c("cases_total","sample_size_total"):=NULL]
# rename cols for comparison 
setnames(claims,"mean","ms_mean")
setnames(claims,"standard_error","ms_standard_error")

# only keep 2010, but plot to compare years to make sure they are largely the same
gg_claims_by_year <- ggplot(data=claims,aes(x=age_start,y=ms_mean,color=as.factor(year_start)))+facet_wrap("sex")+geom_point()
gg_claims_by_year
claims <- claims[year_start==2010,]
claims[,c("year_start","year_end"):=NULL]

# PROCESS HOSPITAL --------------------------------------------------------

# pull age table to merge on age_group_id
age_table <- read.csv(paste0('FILEPATH'))
inp <- merge(inp,age_table,by=c("age_start","age_end"),all.x=T)

# pull mid-year populations # 
locs<-unique(inp[,location_id])
inp[,year_id:=year_start+(year_end-year_start)]
years<-unique(inp[,year_id])
ages<-unique(inp[,age_group_id])
pop<-get_population(age_group_id = ages, location_id = locs, year_id = years, sex_id = c(1,2))
pop[,sex:=ifelse(sex_id==1,"Male","Female")]
  
# merge pop onto hospital data
inp<-merge(inp,pop[,.(age_group_id,location_id,year_id,sex,population)],by=c("year_id","sex","age_group_id","location_id"))
# calculate cases from mean and population  
inp[,cases:=mean*population]
# sum cases and population across all locations 
inp[,c("cases_total","sample_size_total"):=lapply(.SD,sum),.SDcols=c("cases","population"),by=c("age_start","age_end","sex")]
# drop extra cols
inp[,c("year_id","age_group_id","location_id","location_name","mean","cases","sample_size","population"):=NULL]
# calculate mean and standard error from cases and sample size
inp[,mean:=cases_total/sample_size_total]
inp[,standard_error:=sqrt(mean*(1-mean)/sample_size_total)]
# subset to unique rows by year and sex 
inp<-unique(inp)
# drop cases and sample size from this
inp[,c("cases_total","sample_size_total"):=NULL]
setnames(inp,"mean","hosp_mean")
setnames(inp,"standard_error","hosp_standard_error")

# only keep 2008
gg_inp_by_year <- ggplot(data=inp,aes(x=age_start,y=hosp_standard_error,color=as.factor(year_start)))+facet_wrap("sex")+geom_point()
gg_inp_by_year
inp <- inp[year_start==2008,]
inp[,c("year_start","year_end"):=NULL]

# COMPARE MARKETSCAN TO HCUP ----------------------------------------------

# compare ms to hospital # 
dt<-merge(claims,inp,by=c("sex","age_start","age_end"), all.x = T)
dt[,ratio:=ms_mean/hosp_mean] 
dt[,ratio_se:=sqrt((ms_mean/hosp_mean)*((ms_standard_error^2/ms_mean^2)+(hosp_standard_error^2/hosp_mean^2)))]

# UPLOAD HOSPITAL WITH ADJUSTMENT -----------------------------------------

datasheet <- get_epi_data(bundle)
datasheet <- datasheet[cv_inpatient==1,]

# merge on new CFs
datasheet <- merge(datasheet,dt[,.(age_start,ratio,ratio_se,sex)],by=c("age_start","sex"),all.x=T)

# calculate new mean and SE
datasheet[is.na(ratio)==F,mean:=mean*ratio]
datasheet[is.na(ratio)==F,standard_error:=sqrt(standard_error^2*ratio_se^2+ratio^2*standard_error^2+mean*ratio_se^2)]
datasheet[is.na(ratio)==F,c("lower","upper","uncertainty_type_value"):=NA]
datasheet[standard_error<1,c("cases","sample_size"):=NA]
datasheet[standard_error>=1 & is.na(cases)==F & is.na(sample_size)==F,c("mean","standard_error"):=NA]
datasheet<-datasheet[is.na(standard_error)==T|standard_error<1]
datasheet[mean>1,mean:=1]
datasheet[,c("ratio","ratio_se"):=NULL]

# add note
datasheet[!is.na(note_modeler),note_modeler:=paste0(note_modeler,"| adjusted with custom CF based on HCUP 2008 and US Claims 2010")]
datasheet[is.na(note_modeler),note_modeler:="adjusted with custom CF based on HCUP 2008 and US Claims 2010"]

# write file and upload
date <- Sys.Date()
write.xlsx(datasheet, paste0('FILEPATH'), sheetName = "extraction")
upload_epi_data(bundle_id = bundle, filepath = paste0('FILEPATH'))

## END
