# ---HEADER--------------------------------------------------------------------------------------------------
# Date: 5/22/2016
# Project: CKD Envelope Readjustment 
#------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
} else { 
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}

# load packages 
require(data.table)

# source functions
source(paste0(h_root,'FILEPATH.R'))
source_shared_functions(c("get_draws","get_ids"))
#-------------------------------------------------------------------------------------------------------------

#Pass arguments from qsub  
args<-commandArgs(trailingOnly = TRUE)
loc<-as.numeric(args[1])
out_dir_10732<-args[2]
out_dir_10733<-args[3]
out_dir_10734<-args[4]

#Source age map
age_dt<-get_age_map(age_group=5)
ages<-age_dt[,age_group_id]

#specify years and sexes that we want
years<-c(seq(1990,2010,5),2017)
sexes<-c(1,2)

#pull in dismod results for stage 3 
# PREV 
s3_prev <-get_draws(gbd_id_type='modelable_entity_id', 
                gbd_id= 2018, 
                source='epi', 
                measure_id=5, 
                location_id=loc,
                year_id=years,
                sex_id=sexes, 
                age_group_id=ages,
                status='best')
# PREV 
s4_prev <-get_draws(gbd_id_type='modelable_entity_id', 
                        gbd_id= 2019, 
                        source='epi', 
                        measure_id=5, 
                        location_id=loc,
                        year_id=years,
                        sex_id=sexes, 
                        age_group_id=ages,
                        status='best')
# PREV 
s5_prev <-get_draws(gbd_id_type='modelable_entity_id', 
                        gbd_id= 2022, 
                        source='epi', 
                        measure_id=5, 
                        location_id=loc,
                        year_id=years,
                        sex_id=sexes, 
                        age_group_id=ages,
                        status='best')

# PREV 
env_prev <-get_draws(gbd_id_type='modelable_entity_id', 
                        gbd_id= 10731, 
                        source='epi', 
                        measure_id=5, 
                        location_id=loc,
                        year_id=years,
                        sex_id=sexes, 
                        age_group_id=ages,
                        status='best')

idvars<-c('location_id','sex_id','age_group_id','year_id')

#drop modelable_entity_id, model_version_id, and measure_id
s3_prev<-s3_prev[,c('measure_id','metric_id','model_version_id','modelable_entity_id'):=NULL]

s4_prev<-s4_prev[,c('measure_id','metric_id','model_version_id','modelable_entity_id'):=NULL]

s5_prev<-s5_prev[,c('measure_id','metric_id','model_version_id','modelable_entity_id'):=NULL]

env_prev<-env_prev[,c('measure_id','metric_id','model_version_id','modelable_entity_id'):=NULL]

#rename "draws_#" cols 
draw_cols<-paste0("draw_",0:999)

s3_cols<-paste0("s3_draw_",0:999)
setnames(s3_prev, draw_cols, s3_cols)

s4_cols<-paste0("s4_draw_",0:999)
setnames(s4_prev, draw_cols, s4_cols)

s5_cols<-paste0("s5_draw_",0:999)
setnames(s5_prev, draw_cols, s5_cols)

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

#output csv 
write.csv(s3_prev,paste0(out_dir_10732,'/',loc,'.csv'),row.names=F,na='')
write.csv(s4_prev,paste0(out_dir_10733,'/',loc,'.csv'),row.names=F,na='')
write.csv(s5_prev,paste0(out_dir_10734,'/',loc,'.csv'),row.names=F,na='')

