## ########################################################################################################
## 	Adult Mortality through Sibling Histories: #2. Cleaning data to ready for analysis
##
##
##	Desription: This R-file conducts the first steps in preparing the raw sibling data for analysis.
##		Input: Output from step 1 (allsibhistories.dta)
##		Steps:
##				1. Merge sibling information with the country identifier variables
##				2. Create new sibship and PSU IDs that are numeric, not string, to save space
##				3. Change Ethiopia dates to standard calendar
##
##	NOTE: IHME OWNS THE COPYRIGHT

rm(list=ls())
library(haven)
library(readr)
library(data.table)
library(plyr)

# Starting point: Input set with missing siblings appended
args <- commandArgs(trailingOnly = T)

input_dir <- args[1]
input <- fread(FILEPATH)


survid <- input[, .N, by=c('location_name','surveyyear')]
survid[, `:=`(id=.GRP, N=NULL)]

# merge survid onto all_sibs
with_survid <- merge(input, survid, by=c('location_name','surveyyear'))

# Coerce rand, yod columns to numeric
with_survid <- with_survid[, `:=`(rand=as.numeric(rand), yod=as.integer(yod))]

# create weights and dummies for sex
with_survid[, `:=`(male=ifelse(sex==1,1,0), female=ifelse(sex==2,1,0),
                   death=ifelse(is.na(yod),0,1), aged=ifelse(yod-yob<0,NA,yod-yob))]

# generate 5 year age groups for age at death
with_survid[, agedcat := ifelse((aged%/%5)*5<75,(aged%/%5)*5,75)]
with_survid[yod-yob %between% c(1,4) & !is.na(yod), agedcat := 1]

# recode non-standard sexes
with_survid[!(sex==1 | sex==2), sex:=NA]

# 5 year age blocks for age at time of interview
with_survid[, age := yr_interview-yob]
with_survid[, ageblock := (age%/%5)*5]

with_survid[ageblock==50 & id_sm==0, ageblock := 45]


#########################################################################################
  # 1. For alive sibs, compute sex distribution by age and survey
#########################################################################################

#  Redistribute siblings of unknown sex to males and females

#  For alive siblings of unknown sex, redistribute according to sex distribution
# of alive sibs within each age group, by survey

with_survid[, `:=`(males=sum(male, na.rm=T),females=sum(female, na.rm=T)),
             by=c('id','ageblock')]
with_survid <- with_survid[order(id,ageblock,males,females)]
with_survid[is.na(males), males := max(males), by=c('id','ageblock')]
with_survid[is.na(females), females := max(females), by=c('id','ageblock')]
with_survid[, pctmale := males/(males+females), by=c('id','ageblock')]
with_survid[is.na(pctmale), pctmale := 0.5]




#########################################################################################
  # 2. Randomly assigns sex to unknowns based on sex distribution of age group and survey
#########################################################################################
set.seed(5)
with_survid[,rnd := runif(.N)]
with_survid[, sex := as.integer(sex)]
with_survid[is.na(sex) & death==0, sex := ifelse(rnd<=pctmale, 1L, 2L), by=c('id','ageblock')]

with_survid[,c('males','females','rnd','pctmale') := NULL]


#########################################################################################
  # 3. For dead siblings, computes sex distribution by age (pools surveys)
#########################################################################################

# For dead sibs of unknown sex, redistribute according to sex distribution of dead sibs
# within age group of death, pooling over all surveys (not enough deaths to do within survey)
d_redist <- copy(with_survid)

d_redist[death==1, `:=`(males=sum(male,na.rm=T), females=sum(female,na.rm=T)),
         by='agedcat']
d_redist <- d_redist[order(males,females,agedcat)]
d_redist[is.na(males), males:=max(males), by='agedcat']
d_redist[is.na(females), males:=max(females), by='agedcat']

d_redist[, pctmale := males/(males+females), by='agedcat']

#########################################################################################
  # 4. Randomly assigns sex to unknowns based on sex distribution by age (across surveys)
#########################################################################################

d_redist[, rnd := runif(.N)]
d_redist[, sex := as.integer(sex)]
d_redist[death==1 & is.na(sex),
         sex := ifelse(rnd<=pctmale, 1L,0L)]

# column genocide
d_redist[, c('males','females','rnd','pctmale','male','female',
             'id','death','aged','agedcat','age','ageblock') := NULL]

# drop siblings where alive/dead status unknown
d_redist <- d_redist[alive==0 | alive==1]


#########################################################################################
  # 5. Create a list of surveys analyzed along with the sample size (number of total siblings)
#########################################################################################

sibhistlist <- d_redist[, .N, by=c('iso3','surveyyear','svy')]
sibhistlist[, surveyyear := surveyyear+1]

sapply(sibhistlist,class)

# write outputs
fwrite(sibhistlist,FILEPATH)
fwrite(d_redist, FILEPATH)



