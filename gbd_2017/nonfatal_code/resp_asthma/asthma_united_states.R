## Asthma in the United States

# SET UP ------------------------------------------------------------------

rm(list=ls())

library(pacman)
p_load(data.table,dplyr,ggplot2)

# shared functions
shared_functions_dir <- 'FILEPATH'
source(paste0(shared_functions_dir,'/get_epi_data.R'))
source(paste0(shared_functions_dir,'/get_population.R'))
source(paste0(shared_functions_dir,'/get_age_metadata.R'))

# GET AND PREP DATA -------------------------------------------------------

# get prepped asthma data
dt <- get_epi_data(129)

# only keep US locations
dt <- dt[location_id %in% c(523:573,102)]

# cull more
dt <- dt[measure=="prevalence" & is_outlier==0 & (is.na(group_review)|group_review==1) & !nid%in%c(115199,316470),]
dt[nid==111392,case_diagnostics:="WHEEZE_AND_DIAGNOSIS"]
dt[!case_diagnostics%in%c("DIAGNOSIS","WHEEZE","WHEEZE_AND_DIAGNOSIS"),case_diagnostics:="OTHER"]

# CLASSIFY DATA -----------------------------------------------------------

dt[nid%in%c(244369:244371,336847:336850), type:="Marketscan"]
dt[nid%in%c(115168), type:="YRBS"]
dt[nid%in%c(47962,48604,111392), type:="NHANES"]
dt[nid%in%c(111334,111335), type:="ISAAC"]
dt[nid%in%c(97455), type:="MEPS"]
dt[nid%in%c(29983,115199), type:="BRFSS"]
dt[nid%in%c(40229,40280,40330,41257,41522,41635,42357,42427,42517,42525,42609,91569,110301,151813,218067,238008), type:="NHIS"]
dt[nid%in%c(139707), type:="ECRHS"]
dt[is.na(type), type:="Other"]

# AGGREGATE STATE DATA ----------------------------------------------------

ms <- dt[type=="Marketscan",]

# get US population data
pop <- get_population(location_id = c(523:573,102), age_group_id=c(2:21,30:32,235), sex_id=c(1,2), year_id=unique(ms$year_start))
# get age table
age_table <- read.csv('FILEPATH/age_table.csv') %>% as.data.table
# prep pop for merge with ms
pop <- merge(pop, age_table, by='age_group_id')
setnames(pop,'year_id','year_start')
pop[sex_id==2,sex:="Female"]
pop[sex_id==1,sex:="Male"]
pop <- pop[,c('sex','age_start','year_start','location_id','population')]

# merge on
ms <- merge(ms,pop,by=c('sex','age_start','year_start','location_id'),all.x=T,all.y=F)

# calculate population weighted mean for United States
ms[,pop_sum:=sum(population),by=c('sex','age_start','year_start')]
ms[,pop_weight:=population/pop_sum]
ms[,mean:=sum(mean*pop_weight),by=c('sex','age_start','year_start')]
ms <- unique(ms, by=c('sex','age_start','year_start'))
ms[,location_name:='United States']
ms[,location_id:=102]
ms[,c('pop_weight','pop_sum','population'):=NULL]

# add to dt
dt <- dt[type!="Marketscan",]
dt <- rbind(dt,ms)

# CROSSWALK COEFFICIENT FOR MARKETSCAN ------------------------------------

# subset data to NHANES and NHIS (reference), plus Marketscan, and remove MS 2000 & 2015
dt <- dt[type%in%c('NHANES','NHIS','Marketscan'),]

# calculate average by age and category (NHANES/NHIS vs Marketscan vs Marketscan2000)
dt[type=='Marketscan' & year_start!=2000, category:='Marketscan']
dt[type=='Marketscan' & year_start==2000, category:='Marketscan2000']
dt[type!='Marketscan',category:='Reference']
dt[,mean:=mean(mean), by=c('age_start','category')]
dt <- unique(dt,by=c('age_start','category'))

# subset to where there is age overlap
ages_ref <- unique(dt[category=='Reference',]$age_start)
ages_ms <- unique(dt[category=='Marketscan',]$age_start)
dt <- dt[age_start%in%ages_ref & age_start%in%ages_ms,]

# get age weights
age_weights <- get_age_metadata(age_group_set_id=12)
setnames(age_weights,c('age_group_years_start','age_group_weight_value'),c('age_start','weight'))
age_weights <- age_weights[age_start%in% unique(dt$age_start),]
age_weights[,total:=sum(weight)]
age_weights[,weight:=weight/total] # rescale weights to 100% after dropping unneeded age groups

# merge age weights onto data
dt <- merge(dt,age_weights,by='age_start')

# age-standardize by sex and category
dt[,mean:=mean*weight]
dt[,mean:=sum(mean),by=c('category')]
dt <- unique(dt, by=c('category'))

# calculate log ratios for marketscan/reference
dt <- dcast(data=dt, location_id~category, value.var='mean')
dt[,ratio:=log(Marketscan/Reference)]
dt[,ratio2000:=log(Marketscan2000/Reference)]
r <- round(unique(dt$ratio),digits=3)
r2000 <- round(unique(dt$ratio2000),digits=3)
print(paste0('Set DisMod Marketscan coefficient to ',r))
print(paste0('Set DisMod Marketscan2000 coefficient to ',r2000))

## END

