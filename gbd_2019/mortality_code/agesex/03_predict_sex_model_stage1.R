## Title: Predict sex model stage 1
# set-up ------------------------------------------------------------------

rm(list=ls())

library(argparse)
library(data.table)
library(dplyr)
library(splines)

parser <- ArgumentParser()
parser$add_argument('--version_id', type='character', required=TRUE, help='The version for this run of age-sex')
parser$add_argument('--version_5q0_id', type='integer', required=TRUE, help='The 5q0 version for this run of age-sex')
parser$add_argument('--start_year', type='integer', required=TRUE, help='Starting year of estimation')
parser$add_argument('--gbd_year', type='integer', required=TRUE, help='Ending year of estimation')
args <- parser$parse_args()

version_id <- args$version_id
version_5q0_id <- args$version_5q0_id
start_year <- args$start_year
end_year <- args$gbd_year

output_dir <- "FILEPATH"

if(dir.exists(output_dir)==F) dir.create(output_dir)
if(dir.exists(paste0(output_dir,'/sex_model'))==F) dir.create(paste0(output_dir,'/sex_model'))
if(dir.exists(paste0(output_dir,'/sex_model/stage_1'))==F) dir.create(paste0(output_dir,'/sex_model/stage_1'))

# get input ---------------------------------------------------------------

# get 5q0 summaries
files5q0 <- list.files("FILEPATH"), full.names=T)
mean_5q0 <- data.table()
for(file in files5q0){
  temp <- read.csv(file)
  mean_5q0 <- rbind(mean_5q0,temp,fill=T)
}
mean_5q0[,q5_both:=mean]

# get live births
births <- read.csv(paste0(output_dir,'/live_births.csv')) %>% as.data.table
births <- births[,c('year','location_id','sex','births')]
births <- unique(births)
births <- dcast(births, year+location_id~sex,value.var='births')
births[,birth_sexratio:=female/male]
births[,year:=year+0.5]

# get locations
locs <- read.csv(paste0(output_dir,'/as_locs_for_stata.csv')) %>% as.data.table
locs <- locs[level_all==1,.(ihme_loc_id,standard)]
fake_regions <- read.csv(paste0(output_dir,'/st_locs.csv')) %>% as.data.table
fake_regions <- fake_regions[keep==1]
setnames(fake_regions,'region_name','gbdregion')
fake_regions[,c('location_id','keep'):=NULL]
locs <- merge(locs,fake_regions,by='ihme_loc_id')

# merge and predict -------------------------------------------------------

dt <- merge(mean_5q0,births,by=c('location_id','year'), all.x=T)
dt <- merge(dt,locs,by='ihme_loc_id')

# predict
model <- readRDS(paste0(output_dir,'/fit_models/sex_model_object.RDS'))
dt$logit_q5_sexratio_pred <- predict(model, newdata=dt, allow.new.levels = TRUE)

# transform
dt[,q5_sexratio_pred := exp(logit_q5_sexratio_pred) / (1+exp(logit_q5_sexratio_pred))]
dt[,q5_sexratio_pred := (q5_sexratio_pred*0.7) + 0.8]

# generate predicted values for male and female 5q0 using sex ratio at birth
dt[,q_u5_female:=q5_both*(1+birth_sexratio)/(1+q5_sexratio_pred*birth_sexratio)]
dt[,q_u5_male:=q_u5_female*q5_sexratio_pred]
if(nrow(dt[q_u5_female*q_u5_male<=0])>0)  stop("Error: q5_female and q5_male must be greater than zero")

# format and save
dt <- dt[order(ihme_loc_id,year)]
setnames(dt,c('q5_both','q5_sexratio_pred'),c('q_u5_both','q_u5_sexratio_pred'))
dt <- dt[,.(ihme_loc_id,year,logit_q5_sexratio_pred,q_u5_both,q_u5_sexratio_pred,q_u5_female,q_u5_male)]
write.csv(dt,paste0(output_dir,'/sex_model/stage_1/sex_model_stage1.csv'), row.names=F)

# END