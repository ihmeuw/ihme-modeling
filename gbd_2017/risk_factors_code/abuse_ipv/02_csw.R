## Intimate Partner Violence, sex but not csw proportion for HIV PAF calculation

# SET-UP ------------------------------------------------------------------

time_start <- Sys.time()

library(pacman)
p_load(data.table,dplyr)

shared_functions_dir <- 'FILEPATH'
source(paste0(shared_functions_dir,'/get_demographics.R'))
source(paste0(shared_functions_dir,'/get_model_results.R'))

years <- get_demographics(gbd_team='cod')$year_id
estimation_years <- get_demographics(gbd_team='epi')$year_id
ages <- c(8:20,30:32,235)

# GET MODEL RESULTS -------------------------------------------------------

sex <- get_model_results(gbd_team='epi', gbd_id='2638', location_id=-1, location_set_id=35)
idu <- get_model_results(gbd_team='epi', gbd_id='2637', location_id=-1, location_set_id=35)
other <- get_model_results(gbd_team='epi', gbd_id='2639', location_id=-1, location_set_id=35)
csw <- get_model_results(gbd_team='epi', gbd_id='2636', location_id=-1, location_set_id=35)

# merge
prep <- function(dt,dt_name){
  setnames(dt,'mean',dt_name)
  dt <- dt[sex_id==2 & age_group_id%in%ages,]
  dt[,c('model_version_id','sex_id','measure_id','measure','lower','upper'):=NULL]
  return(dt)
}
sex <- prep(sex,'sex')
idu <- prep(idu,'idu')
other <- prep(other,'other')
csw <- prep(csw,'csw')
dt <- Reduce(merge, list(sex,idu,other,csw))

# SQUEEZE HIV PROPORTION MODELS -------------------------------------------

dt[,total:=sum(sex,idu,other)]
dt[,sex:=sex/total]
dt[,idu:=idu/total]
dt[,other:=other/total]
dt[,total:=NULL]

# GET PROPORTION SEX BUT NOT CSW ------------------------------------------

dt[,prop_sex_not_csw:=(1-csw)*sex]

# SAVE TO HIV-IPV PAF INPUTS FOLDER ---------------------------------------

dt <- dt[,c('location_id','year_id','age_group_id','prop_sex_not_csw')]
write.csv(dt,paste0(main_dir,'/input/csw_paf.csv'),row.names=F)
