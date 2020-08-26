##################################################################################
## Intimate Partner Violence, sex but not csw proportion for HIV PAF calculation
## AUTHOR: 
## DATE: June 2018
## PROJECT: Intimate partner violence, GBD risk factors
## PURPOSE: (1) get HIV proportion models from dismod (sex,  IDU, other, CSW)
##          (2) squeeze sex, IDU, other to 100%
##          (3) multiply proportion sexual transmission by proportion sexual that
##                  is transmitted by CSW to get proportion HIV due to CSW
##          (4) subtract proportion due to CSW from proportion due to sex to get
##                  proportion HIV due to sex but not CSW
##          (5) export .csv to input folder for IPV-HIV paf calculation
## NOTE: only works on the cluster
##################################################################################

# SET-UP ------------------------------------------------------------------

library(pacman)
p_load(data.table,dplyr)

shared_functions_dir <- 'FILEPATH'
source(paste0(shared_functions_dir,'/get_demographics.R'))
source(paste0(shared_functions_dir,'/get_model_results.R'))

years <- get_demographics(gbd_team='cod')$year_id
estimation_years <- get_demographics(gbd_team='epi')$year_id
ages <- c(8:20,30:32,235)

decomp <- 'step4' ## WILL NEED TO MODIFY EACH TIME
status <- 'best'

# note: main_dir argument specified in master script

# GET MODEL RESULTS -------------------------------------------------------

sex <- get_model_results(gbd_team = "epi", gbd_id='24812', location_id=-1, location_set_id=35, decomp_step=decomp, status=status) #replacing me_id 2638
idu <- get_model_results(gbd_team='epi', gbd_id='2637', location_id=-1, location_set_id=35, decomp_step=decomp, status=status)
other <- get_model_results(gbd_team='epi', gbd_id='2639', location_id=-1, location_set_id=35, decomp_step=decomp, status=status)
csw <- get_model_results(gbd_team='epi', gbd_id='2636', location_id=-1, location_set_id=35, decomp_step=decomp, status=status)

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

# squeeze sex, idu, other to 100%
dt[,total:=sex+idu+other]
dt[,sex:=sex/total]
dt[,idu:=idu/total]
dt[,other:=other/total]
dt[,total:=NULL]

# GET PROPORTION SEX BUT NOT CSW ------------------------------------------

# (1-csw) = proportion of sexual transmission that is not csw. multiply by the proportion
# total sexual transmission to get proportion of all hiv that is due to sex but not csw.
dt[,prop_sex_not_csw:=(1-csw)*sex]

# SAVE TO HIV-IPV PAF INPUTS FOLDER ---------------------------------------

dt <- dt[,c('location_id','year_id','age_group_id','prop_sex_not_csw')]
write.csv(dt,paste0(main_dir,'/input/csw_paf_step4.csv'),row.names=F)