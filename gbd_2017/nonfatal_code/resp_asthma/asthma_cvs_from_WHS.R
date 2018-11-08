## Estimate cv_wheeze and cv_diagnosis from WHS

# SET-UP ------------------------------------------------------------------

rm(list=ls())
library(pacman)
pacman::p_load(data.table, haven, dplyr, survey, ggplot2, metafor)
date <- Sys.Date()

# get tabulated asthma, wheeze, and diagnosis variables from World Health Survey microdata
dt <- read.csv("FILEPATH") %>% as.data.table
dt <- dt[var %in% c('asthma','wheeze','diagnosis')]

# take mean of males and females by age and location
dt[, mean:=mean(mean), by=c('age_start','ihme_loc_id','var')]
dt[, sample_size:=sum(sample_size), by=c('age_start','ihme_loc_id','var')]
dt <- unique(dt, by=c('age_start','ihme_loc_id','var'))

# reshape
dt <- dcast(dt, nid + ihme_loc_id + age_start + age_end ~ var, value.var = c('mean','sample_size'))

# GET AGE WEIGHTS ---------------------------------------------------------

# get age-weights
source("FILEPATH/get_age_metadata.R")
age_weights <- get_age_metadata(12)
setnames(age_weights,'age_group_years_start','age_start')

# WHS tabulations start at age 10, so subset weights accordingly
age_weights <- age_weights[age_start>=10,]

# collapse 5 yr weights to 10 yr age groups to match WHS tabulations
age_weights$group <- sort(rep(1:9,times=2))
age_weights[,age_weight:=sum(age_group_weight_value),by='group']
age_weights <- unique(age_weights,by='group')
age_weights[,age_end:=age_start+9]
age_weights <- unique(age_weights[,c('age_start','age_end','age_weight')])

# rescale to 100%
age_weights[,total:=sum(age_weight)]
age_weights[,age_weight:=age_weight/total]
age_weights[,total:=NULL]

# merge on age-weights
dt <- merge(dt,age_weights,by=c('age_start','age_end'))

# CALCULATE AGE-STANDARDIZED MEANS ----------------------------------------

# multiply prevalence by age weight and sum over age, by sex/location
dt[, as_asthma := mean_asthma * age_weight]
dt[, as_asthma := sum(as_asthma), by = 'ihme_loc_id']
dt[, as_wheeze := mean_wheeze* age_weight]
dt[, as_wheeze:= sum(as_wheeze), by= 'ihme_loc_id']
dt[, as_diagnosis := mean_diagnosis* age_weight]
dt[, as_diagnosis:= sum(as_diagnosis), by= 'ihme_loc_id']

# calculate new sample sizes
dt[, sample_size_asthma:= sum(sample_size_asthma), by='ihme_loc_id']
dt[, sample_size_wheeze:= sum(sample_size_wheeze), by= 'ihme_loc_id']
dt[, sample_size_diagnosis:= sum(sample_size_diagnosis), by= 'ihme_loc_id']
dt <- unique(dt,by= 'ihme_loc_id')

# re-calculate standard error
dt[, as_asthma_se:=sqrt(as_asthma*(1-as_asthma)/sample_size_asthma)]
dt[, as_wheeze_se:=sqrt(as_wheeze*(1-as_wheeze)/sample_size_wheeze)]
dt[, as_diagnosis_se:=sqrt(as_diagnosis*(1-as_diagnosis)/sample_size_diagnosis)]

# CALCULATE RATIOS --------------------------------------------------------

# age-standardized ratios
dt[,cv_wheeze:=as_wheeze/as_asthma]
dt[,cv_diagnosis:=as_diagnosis/as_asthma]

# keep ratios only where >0 and not undefined
dt <- dt[cv_wheeze>0 & cv_diagnosis>0,]

# calculate standard error of ratios
dt[,cv_wheeze_se:=sqrt((as_wheeze/as_asthma)*((as_wheeze_se^2/as_asthma^2)+(as_asthma_se^2/as_asthma^2)))]
dt[,cv_diagnosis_se:=sqrt((as_diagnosis/as_asthma)*((as_diagnosis_se^2/as_asthma^2)+(as_asthma_se^2/as_asthma^2)))]

# META-ANALYSIS -----------------------------------------------------------

model_wheeze <- rma.uni(data = dt, yi = cv_wheeze, sei = cv_wheeze_se)
model_diagnosis <- rma.uni(data = dt, yi = cv_diagnosis, sei = cv_diagnosis_se)

pdf(paste0("FILEPATH/cv_estimation_WHS_",date,".pdf"))
forest(model_wheeze, showweights = T,
       xlab = "Exp beta for cv_wheeze from WHS data", refline = 1, title="cv_wheeze")
forest(model_diagnosis, showweights = T,
       xlab = "Exp beta for cv_diagnosis from WHS data", refline = 1, title="cv_diagnosis")

dev.off()

## END