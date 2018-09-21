# BPH age-specific crosswalk # 

# clear workspace # 
rm(list=ls())

# load packages # 
require(data.table)
require(openxlsx)
require(magrittr)
require(ggplot2)

# set roots and directories # 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/' 
  h_root <- '/homes/USERNAME/'
} else { 
  j_root <- 'J:/'
  h_root <- 'H:/'
}
central_dir<-"FILEPATH"
data_dir<-"FILEPATH"
doc_dir<-"FILEPATH"
output_dir<-"FILEPATH"
filename<-"FILENAME.xlsx"

# source functions # 
source(paste0(central_dir,'get_location_metadata.R'))
source(paste0(central_dir,'get_envelope.R'))
source(paste0(central_dir,'get_population.R'))

# load data # 
dt<-read.xlsx(paste0(data_dir,filename))%>%as.data.table()

# get location data table # 
loc_dt<-get_location_metadata(22)

# only keep US data # 
us_locs<-loc_dt[parent_id==102 | location_id==102,location_id]
dt<-dt[location_id%in%us_locs]

# only keep necessary cols #
cv_cols<-grep(x = names(dt),pattern = "cv_",value = T)
dt<-dt[,c(cv_cols,"location_id","location_name","sex","year_start","year_end","age_start","age_end","mean","cases",
          "sample_size","standard_error"),with=F]

# subset into marketscan and hospital, drop cv_ cols # 
ms<-dt[cv_marketscan_all_2000==1 | cv_marketscan_all_2010==1 | cv_marketscan_all_2012==1]
ms[,grep(x=names(ms),pattern = "cv_"):=NULL]
hosp<-dt[cv_hospital==1]
hosp[,grep(x=names(hosp),pattern = "cv_"):=NULL]

# process MS # 
# sum cases and sample size by age to get prevalence for US by age # 
ms[,c("cases_total","sample_size_total"):=lapply(.SD,sum),.SDcols=c("cases","sample_size"),
   by=c("age_start","age_end","year_start","year_end")]

# drop loc_id, loc name, cases, and sample_size now that we've collapsed across locations # 
ms[,c("location_id","location_name","cases","sample_size","mean"):=NULL]

# calculate new mean #
ms[,mean:=cases_total/sample_size_total]

# calculate new standard error #
ms[,standard_error:=sqrt(mean*(1-mean)/sample_size_total)]

# drop cases and sample size #
ms[,c("cases_total","sample_size_total"):=NULL]

# subset to unique rows by year and sex # 
ms<-unique(ms)

# create ms_2012 df to compare hospital to # 
ms_2012<-ms[year_start==2012]
setnames(ms_2012,"mean","ms_mean")
setnames(ms_2012,"standard_error","ms_standard_error")

# change age_end from 100 to 99 to be consistent with hospital data #
ms_2012[age_end==100,age_end:=99]

# process hospital # 
# calculate sample_size from mean and standard_error # 
hosp[,sample_size:=(mean*(1-mean))/(standard_error^2)]

# calculate cases from mean and sample_size # 
hosp[,cases:=mean*sample_size]

# sum cases and population across all locations # 
hosp[,c("cases_total","sample_size_total"):=lapply(.SD,sum),.SDcols=c("cases","sample_size"),
     by=c("age_start","age_end")]

# drop extra cols # 
hosp[,c("location_id","location_name","mean","standard_error","cases","sample_size","year_start","year_end"):=NULL]

# calculate mean and standard error from cases and sample size # 
hosp[,mean:=cases_total/sample_size_total]
hosp[,standard_error:=sqrt(mean*(1-mean)/sample_size_total)]

# subset to unique rows by year and sex # 
hosp<-unique(hosp)

# drop cases and sample size from this #
hosp[,c("cases_total","sample_size_total"):=NULL]
setnames(hosp,"mean","hosp_mean")
setnames(hosp,"standard_error","hosp_standard_error")

# compare ms to hospital # 
compare<-merge(ms_2012,hosp,by=c("sex","age_start","age_end"), all.x = T)
compare[,ratio:=ms_mean/hosp_mean] 
compare[,ratio_se:=sqrt((ms_mean/hosp_mean)*((ms_standard_error^2/ms_mean^2)+(hosp_standard_error^2/hosp_mean^2)))]

# plot # 
gg<-ggplot(data=compare,aes(x=age_start,y=ratio))+
  geom_point()+
  geom_smooth(method = "loess")
gg

# save ratios for documentation # 
write.csv(compare,paste0(doc_dir,"FILENAME.csv"),row.names = F,na="")

# adjust input data # 
dt_adj<-read.xlsx(paste0(data_dir,filename))%>%as.data.table()
dt_adj<-merge(dt_adj[cv_hospital==1],compare[,.(age_start,age_end,ratio,ratio_se)],by=c("age_start","age_end"),all.x=T)
dt_adj[,mean:=mean*ratio]
dt_adj[,standard_error:=sqrt(standard_error^2*ratio_se^2+ratio^2*standard_error^2+mean*ratio_se^2)]
dt_adj[,c("lower","upper","uncertainty_type_value"):=NA]
dt_adj[,c("ratio","ratio_se"):=NULL]

write.xlsx(dt_adj,"FILEPATH",sheetName="extraction")
