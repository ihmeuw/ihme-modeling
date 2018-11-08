## Intimate Partner Violence, HIV PAF calculation, child job

# SETUP -------------------------------------------------------------------

time_start <- Sys.time()

library(data.table)
library(dplyr)
library(svMisc, lib.loc="FILEPATH")

shared_functions_dir <- 'FILEPATH'
source(paste0(shared_functions_dir,'/get_demographics.R'))
source(paste0(shared_functions_dir,'/get_draws.R'))

## get arguments
print(commandArgs())
loc_id <- as.numeric(commandArgs()[4])
main_dir <- as.numeric(commandArgs()[5])
ver <- as.numeric(commandArgs()[6])
print(paste(loc_id,main_dir,ver,sep=", "))
main_dir <- 'FILEPATH'
date <- Sys.Date()

csw <- paste0(main_dir,'csw_paf.csv')
hiv_dir <- 'FILEPATH'
age_group_mapping <- paste0(main_dir,'age_mapping.csv')
relative_risk <- paste0(main_dir,'rr_draws.csv')

years <- get_demographics(gbd_team='cod')$year_id
estimation_years <- get_demographics(gbd_team='epi')$year_id
ages <- c(8:20,30:32,235)

# GET IPV EXPOSURE --------------------------------------------------------

ipv_exp <- get_draws(gbd_id_type='modelable_entity_id', gbd_id=2452, location_id=loc_id,
                     sex_id=2, status='best', source='epi', age_group_id=ages,
                     year_id=years, measure_id=18)
ipv_exp <- ipv_exp[,c('measure_id','metric_id','model_version_id','modelable_entity_id'):=NULL]

# melt to get draws long
ipv_exp <- melt(ipv_exp, id.vars=c('location_id','year_id','age_group_id','sex_id'))
setnames(ipv_exp,'value','ipv_exp')
ipv_exp[,draw_num:=as.numeric(substr(variable,6,10))]
ipv_exp[,variable:=NULL]

# interpolate to get all years
print("Interpolating exposure draws")
for(d in 0:999){
  svMisc::progress(d, max.value = 999, progress.bar=T)
  for(age in ages){
    subset <- ipv_exp[age_group_id==age & draw_num==d,c('year_id','ipv_exp')]
    int <- approx(y=subset$ipv_exp, x=subset$year_id, xout=years, method='linear', rule=2)
    new <- data.table(location_id=loc_id, year_id=int[[1]], age_group_id=age, draw_num=d, sex_id=2, ipv_exp=int[[2]])
    ipv_exp <- rbind(ipv_exp,new,fill=T)
  }
}
ipv_exp <- unique(ipv_exp, by=c('year_id','age_group_id','draw_num'))


# GET PROPORTION HIV DUE TO SEX NOT CSW -----------------------------------

sex_not_csw <- read.csv(csw) %>% as.data.table
sex_not_csw <- sex_not_csw[location_id==loc_id,]

# interpolate prop_sex_not_csw to get all years
for(age in ages){
  subset <- sex_not_csw[age_group_id==age,c('year_id','prop_sex_not_csw')]
  int <- approx(y=subset$prop_sex_not_csw, x=subset$year_id, xout=years, method='linear', rule=2)
  new <- data.table(location_id=loc_id, year_id=int[[1]], age_group_id=age, sex_id=2, prop_sex_not_csw=int[[2]])
  sex_not_csw <- rbind(sex_not_csw,new,fill=T)
}
sex_not_csw <- unique(sex_not_csw, by=c('year_id','age_group_id'))
sex_not_csw[,sex_id:=2] # estimation years were getting sex_id=NA without this line

# merge onto ipv_exposure
dt <- merge(ipv_exp,sex_not_csw, by=c('location_id','year_id','age_group_id','sex_id'))

# GET HIV INCIDENT DRAWS --------------------------------------------------

hiv_incidence <- read.csv(paste0(hiv_dir,'/',loc_id,'_hiv_incidence_draws.csv')) %>% as.data.table
hiv_incidence <- hiv_incidence[age_group_id>=8 & sex_id==2,] # females 15 years or older
setnames(hiv_incidence,"inc1000","inc0") # incidence draws were 1-1000 instead of 0-999

# melt to get draws long
hiv_incidence <- melt(hiv_incidence, id.vars=c('location_id','year_id','age_group_id','sex_id'))
setnames(hiv_incidence,'value','hiv_inc')
hiv_incidence[,draw_num:=as.numeric(substr(variable,4,10))]
hiv_incidence[,variable:=NULL]

# CALCULATE PAF -----------------------------------------------------------

# read in relative risk draws
rr_draws <- read.csv(relative_risk) %>% as.data.table

# merge ipv exposure with relative risk
dt <- merge(dt, rr_draws, by='draw_num')

# calculate PAF on incidence  [Prevalence of IPV * (RR - 1)] / [Prevalence of IPV * (RR - 1) + 1]
dt[,paf:=ipv_exp*(rr-1)/(ipv_exp*(rr-1)+1)]

# merge PAF on HIV incidence
dt <- merge(dt, hiv_incidence, by=c('location_id','year_id','age_group_id','sex_id','draw_num'))

# apply proportion HIV incidence that is sexually transmitted and not from CSW to total HIV incidence
dt[,sexnoncsw_incidence:=hiv_inc*prop_sex_not_csw]

# merge on age_table to get age_start
age_table <- read.csv(age_group_mapping) %>% as.data.table
dt <- merge(dt,age_table,by='age_group_id')

# expand to get one year age groups
expand_age <- function(i){
  copy <- copy(dt)
  copy[,age_start:=age_start+i]
  return(copy)
}
dt <- rbindlist(lapply(0:4,expand_age))
dt[,age_group_id:=NULL]

# Denominator: cumulative incidence of all HIV
dt[,birth_yr:=year_id-age_start]
dt <- dt[order(draw_num,birth_yr,age_start)]
dt[,denominator:=1-cumprod(1-hiv_inc), by=c('draw_num','birth_yr')]

# Numerator: cumulative incidence of HIV due to IPV
dt[,numerator:=1-cumprod(1-paf*sexnoncsw_incidence), by=c('draw_num','birth_yr')]

# calculate PAF on prevalence of HIV due to IPV
dt[denominator>0,paf_prev:=numerator/denominator]
dt[denominator==0,paf_prev:=0]

# take averages over age to get 5 year age groups
round5 <- function(x) return(5*floor(x/5))
dt[,age_start:=round5(age_start)]
dt[,paf_prev:=mean(paf_prev),by=c('age_start','year_id','draw_num')]
dt <- unique(dt, by=c('age_start','year_id','draw_num'))

# reshape wide on draw
dt[,draw:=paste0("draw_",draw_num)]
dt <- dcast(dt, year_id+age_start~draw, value.var='paf_prev')

# merge on information about age_group_id
dt <- merge(dt,age_table,by='age_start')
dt[,age_start:=NULL]

# duplicate, same paf for ylds (3) and ylls (4)
dt[,measure_id:=3]
dt_yll <- copy(dt)
dt_yll[,measure_id:=4]
dt <- rbind(dt,dt_yll)

# expand and add cause_id to prepare for save_results_risk (948-950 & 300)
expand_cause <- function(i){
  copy <- copy(dt)
  copy[,cause_id:=i]
  return(copy)
}
dt <- rbindlist(lapply(c(300,948:950),expand_cause))

# save in country and sex specific files ready for upload
write.csv(dt,"FILEPATH/",loc_id,"_2.csv"),row.names=F)

# time of completion
time_end <- Sys.time()
print(time_end-time_start)

# END