#############################################################################################
## Description: This file outputs CBH estimates in the appropriate format for 5q0 estimation
#############################################################################################

## Prep R
rm(list=ls())
library(foreign); library(foreach)

## Set directories
raw.dir<-"FILEPATH"
raw.file<-"strFinalFileName.dta"

save.dir<-"FILEPATH"
savefile<-"strSurveyName"

## Read data
add.data = read.dta(paste(raw.dir,raw.file,sep=""), convert.underscore = TRUE, convert.factors=F)

## Set variables
add.data$q5 = add.data$q5
add.data$lb.q5 = add.data$lb.q5
add.data$ub.q5 = add.data$ub.q5
add.data$source.date = add.data$year
add.data$t = add.data$year
add.data$in.direct = "direct"
add.data$compiling.entity = "new"
add.data$data.age = "new"
add.data$source = survey_name
add.data$nid = nid

add.data = add.data[,c("iso3","t","q5","source","source.date","in.direct","compiling.entity","data.age","sd.q5","log10.sd.q5", "nid")]
head(add.data)

## Fill in years (i.e. get one estimate per year)       
t1 <- t2 <- add.data
t1$t <- t1$t + 0.5  ## this is only valid if all periods are two year periods (i.e. this would need to be
t2$t <- t2$t - 0.5  ## modified if there was a coded fatal discontinuity that introduces non two-year periods)
add.data <- rbind(t1, t2)

## Save
write.dta(add.data, paste(gbd.env.dir, gbd.env.file, sep=""))
