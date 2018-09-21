# ---HEADER--------------------------------------------------------------------------------------------------
# Author: 
# Date: 5/22/2016
# Project: CKD Envelope Readjustment 
#------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/' 
  h_root <- '/homes/USERNAME/'
} else { 
  j_root <- 'J:/'
  h_root <- 'H:/'
}

# load packages 
require(data.table)

# source functions
source(FILEPATH) #get_draws
source(FILEPATH) #get_ids
#-------------------------------------------------------------------------------------------------------------

#Pass arguments from qsub  
args<-commandArgs(trailingOnly = TRUE)
loc<-as.numeric(args[1])
output_version<-as.numeric(args[2])

#Source age map
age_dt<-fread(FILEPATH)
ages<-age_dt[age_group_id%in%c(2:20,30:32,235),age_group_id]

#specify years and sexes that we want
years<-c(seq(1990,2010,5),2016)
sexes<-c(1,2)

#pull in dismod results for stage 3 
# PREV 
s3_prev <-get_draws(gbd_id_field='modelable_entity_id', 
                gbd_id= 2018, 
                source='epi', 
                measure_ids=5, 
                location_ids=loc,
                year_ids=years,
                sex_ids=sexes, 
                age_group_ids=ages,
                status='best')
#INC
s3_inc <-get_draws(gbd_id_field='modelable_entity_id', 
                      gbd_id= 2018, 
                      source='epi', 
                      measure_ids=6, 
                      location_ids=loc,
                      year_ids=years,
                      sex_ids=sexes, 
                      age_group_ids=ages,
                      status='best')
#pull in dismod results for stage 4 
# PREV 
s4_prev <-get_draws(gbd_id_field='modelable_entity_id', 
                        gbd_id= 2019, 
                        source='epi', 
                        measure_ids=5, 
                        location_ids=loc,
                        year_ids=years,
                        sex_ids=sexes, 
                        age_group_ids=ages,
                        status='best')
#INC
s4_inc <-get_draws(gbd_id_field='modelable_entity_id', 
                       gbd_id= 2019, 
                       source='epi', 
                       measure_ids=6, 
                       location_ids=loc,
                       year_ids=years,
                       sex_ids=sexes, 
                       age_group_ids=ages,
                       status='best')
#pull in dismod results for stage 5 
# PREV 
s5_prev <-get_draws(gbd_id_field='modelable_entity_id', 
                        gbd_id= 2022, 
                        source='epi', 
                        measure_ids=5, 
                        location_ids=loc,
                        year_ids=years,
                        sex_ids=sexes, 
                        age_group_ids=ages,
                        status='best')
#INC
s5_inc <-get_draws(gbd_id_field='modelable_entity_id', 
                       gbd_id= 2022, 
                       source='epi', 
                       measure_ids=6, 
                       location_ids=loc,
                       year_ids=years,
                       sex_ids=sexes, 
                       age_group_ids=ages,
                       status='best')
#pull enveople model results 
# PREV 
env_prev <-get_draws(gbd_id_field='modelable_entity_id', 
                        gbd_id= 10731, 
                        source='epi', 
                        measure_ids=5, 
                        location_ids=loc,
                        year_ids=years,
                        sex_ids=sexes, 
                        age_group_ids=ages,
                        status='best')

env_inc <-get_draws(gbd_id_field='modelable_entity_id', 
                     gbd_id= 10731, 
                     source='epi', 
                     measure_ids=6, 
                     location_ids=loc,
                     year_ids=years,
                     sex_ids=sexes, 
                     age_group_ids=ages,
                     status='best')

#set id variable to merge by 
idvars<-c('location_id','sex_id','age_group_id','year_id')

#drop modelable_entity_id, model_version_id, and measure_id
s3_prev<-s3_prev[,c('modelable_entity_id','model_version_id','measure_id'):=NULL]
s3_inc<-s3_inc[,c('modelable_entity_id','model_version_id','measure_id'):=NULL]

s4_prev<-s4_prev[,c('modelable_entity_id','model_version_id','measure_id'):=NULL]
s4_inc<-s4_inc[,c('modelable_entity_id','model_version_id','measure_id'):=NULL]

s5_prev<-s5_prev[,c('modelable_entity_id','model_version_id','measure_id'):=NULL]
s5_inc<-s5_inc[,c('modelable_entity_id','model_version_id','measure_id'):=NULL]

env_prev<-env_prev[,c('modelable_entity_id','model_version_id','measure_id'):=NULL]
env_inc<-env_inc[,c('modelable_entity_id','model_version_id'):=NULL]

#rename "draws_#" cols 
draw_cols<-paste0("draw_",0:999)

s3_cols<-paste0("s3_draw_",0:999)
setnames(s3_prev, draw_cols, s3_cols)
setnames(s3_inc, draw_cols, s3_cols)

s4_cols<-paste0("s4_draw_",0:999)
setnames(s4_prev, draw_cols, s4_cols)
setnames(s4_inc, draw_cols, s4_cols)

s5_cols<-paste0("s5_draw_",0:999)
setnames(s5_prev, draw_cols, s5_cols)
setnames(s5_inc, draw_cols, s5_cols)

env_cols<-paste0("env_draw_",0:999)
setnames(env_prev, draw_cols, env_cols)

# merge all datasets 
df<- merge(s3_prev, s4_prev, by=idvars,with=FALSE)
df<- merge(df, s5_prev, by=idvars,with=FALSE)
df<- merge(df, env_prev, by=idvars,with=FALSE)

# sum stage draws   
sum_cols<-paste0("sum_stage_",0:999)
df[, (sum_cols) := lapply(1:1000, function(draw) rowSums(df[,c(s3_cols[draw],s4_cols[draw],s5_cols[draw]),with=F]))]

# scale stage prevalence draws by envelope draws 
ratio_cols<-paste0("ratio_",0:999)
df[,(ratio_cols) := lapply(1:1000, function(draw) get(env_cols[draw])/get(sum_cols[draw]))]
df<-df[,!c(s3_cols,s4_cols,s5_cols,sum_cols,env_cols),with=F]

# merge scalars onto stage dfs
s3_prev<-merge(s3_prev,df, by=idvars, with=F)
s3_prev[,(s3_cols):= lapply(1:1000, function(draw) get(s3_cols[draw])*get(ratio_cols[draw]))]
s3_prev[,(ratio_cols):=NULL]
setnames(s3_prev,s3_cols,draw_cols)
s3_prev[,measure_id:=5]

s3<-rbindlist(list(s3_prev,env_inc),use.names = T)

s4_prev<-merge(s4_prev,df, by=idvars, with=F)
s4_prev[,(s4_cols):= lapply(1:1000, function(draw) get(s4_cols[draw])*get(ratio_cols[draw]))]
s4_prev[,(ratio_cols):=NULL]
setnames(s4_prev,s4_cols,draw_cols)
s4_prev[,measure_id:=5]

s5_prev<-merge(s5_prev,df, by=idvars, with=F)
s5_prev[,(s5_cols):= lapply(1:1000, function(draw) get(s5_cols[draw])*get(ratio_cols[draw]))]
s5_prev[,(ratio_cols):=NULL]
setnames(s5_prev,s5_cols,draw_cols)
s5_prev[,measure_id:=5]

#specify output directory 
out_dir_s3<-FILEPATH
dir.create(out_dir_s3,recursive = TRUE)

out_dir_s4<-FILEPATH
dir.create(out_dir_s4,recursive = TRUE)

out_dir_s5<-FILEPATH
dir.create(out_dir_s5,recursive = TRUE)

#output csv 
write.csv(s3_prev,paste0(out_dir_s3,'/',loc,'.csv'),row.names=F,na='')
write.csv(s4_prev,paste0(out_dir_s4,'/',loc,'.csv'),row.names=F,na='')
write.csv(s5_prev,paste0(out_dir_s5,'/',loc,'.csv'),row.names=F,na='')

