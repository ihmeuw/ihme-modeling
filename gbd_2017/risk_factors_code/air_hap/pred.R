#----HEADER----------------------------------------------------------------------------------------------------------------------
# Project: air_hap
# Purpose: model to predict HAP exposure based on personal monitoring database
#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else { 
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
}

# Set formatting objects
location_set_version_id <- 398
air_pm_exp_version <- 33
date <- format(Sys.Date(), "%m%d%y")
in_root <- "FILEPATH"
out_root <- "FILEPATH"
in_crosswalk <- "FILEPATH"
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_ids.R")

air_pm <- file.path("FILEPATH",air_pm_exp_version,"air_pm_covariate.csv")

# Package Setup
pacman::p_load(data.table, DT, plyr, knitr, lme4, nlme, arm, magrittr, ggplot2, ini,stringr)


#Prep square for predicting out later
ubcov.function.dir <- "FILEPATH"
source("FILEPATH/db_tools.r"))
square <- make_square(location_set_version_id=location_set_version_id, 
                      year_start=1990, year_end=2017,
                      0, 0,
                      covariates="sdi")

#Import updated dataset
dt <- fread(paste0(in_root, "lmer_input_06062018.csv"))

#remove duplicate rows
setkey(dt)
dt<-unique(dt)

#only keep relevant columns
dt <- dt[!is.na(year_id),.(location_id,super_region_id,region_id,ihme_loc_id,year_id,title,pmstddeviation,pm_mean,mean_geom,
            pm_se,traditional,ics,gasstove,stove_type,wood,gas,kerosene,dung,coal,charcoal,crop_residue,biomass,
            women,personal_exp,kit,kit_indoor,living,measure_std,cooking,logpm,outlier,pm_median)]
dt[,outlier:=0]

#log transform 
dt[is.na(logpm),logpm:=log(pm_mean)]
#use median if mean doesnt exits
dt[is.na(logpm),logpm:=log(pm_median)]

#replace missingness in fuel type indicators with 0, assume they are not included
for(ft in c("wood","gas","kerosene","dung","coal","charcoal","crop_residue","biomass")) {
  dt[is.na(get(ft)),(ft):=0]
}

#create fuel type indicators
dt[,non_solid:=0]
dt[,solid:=1]
dt[gas==1|kerosene==1,non_solid:=1]
dt[non_solid==1 & wood!=1 & dung!=1 & coal!=1 & charcoal!=1 & crop_residue!=1 & biomass!=1, solid:=0]
dt[non_solid==1 & wood!=1 & dung!=1 & coal!=1 & charcoal!=1 & crop_residue!=1 & biomass!=1, solid:=0]
#outlier rows with only non-solid fuel
dt[non_solid==1&solid==0,outlier:=1]


#Merge on location hierarchy
LocationHier <- get_location_metadata(location_set_id = 22) %>% as.data.table()
dt <- merge(dt,LocationHier[,.(location_id,is_estimate,location_name,location_name_short,super_region_id,super_region_name,region_id,region_name,ihme_loc_id)], by=c("location_id","super_region_id","region_id",
                                  "ihme_loc_id"))

#Make some necessary exclusions 
dt <- dt[title == "Relationship between pulmonary function and indoor air pollution from coal combustion among adult residents in an inner-city area of southwest China", outlier:=1]
dt <- dt[title == "Indoor Air Pollution and Respiratory Illness in Children from Rural India: A Pilot Study", outlier:=1]

# Merge on sdi
sdi <- get_covariate_estimates(covariate_id = 881)
dt_sdi <- merge(dt, sdi[,.(location_id,year_id,mean_value)], by = c("location_id", "year_id"), all.x = T)
dt_sdi <- dt_sdi[, sdi := mean_value]
dt_sdi$mean_value <- NULL

# Create vars for monitor location (personal, kitchen, living room) and measure standard
dt_sdi <- dt_sdi[personal_exp == 1, monitor_loc := "personal"]
dt_sdi <- dt_sdi[kit == 1 & personal_exp == 0, monitor_loc := "kitchen"]
dt_sdi <- dt_sdi[kit == 0 & personal_exp == 0, monitor_loc := "living"]
dt_sdi <- dt_sdi[measure_std==1, measure_std_new := 0] #in new, 0 is sandard measurement (either 24 or 48 hr)
dt_sdi <- dt_sdi[measure_std==0, measure_std_new := 1] #in new, 1 is non-standard measurement
dt_sdi[,monitor_loc:=as.factor(monitor_loc)]
dt_sdi[, monitor_loc := relevel(monitor_loc, ref='personal')]



# merge on air_pm exp to subtract off ambient exposure
# pull from covariate database to get every year ** must be up to date **
air_pm <- get_covariate_estimates(covariate_id = 106)

#merge
dt_sdi <- merge(dt_sdi,air_pm[,.(year_id,mean_value,location_id)],by=c("year_id","location_id"),all.x=T)
dt_sdi[,hap_pm:=exp(logpm)-mean_value]
dt_sdi[,log_pm:=log(hap_pm)]
#for now, outlier studies for which measured hap is less than ambient
dt_sdi[hap_pm<=0,outlier:=1]
message(paste0("Warning: ",nrow(dt_sdi[hap_pm<0])," outliered rows due to hap lower than ambient"))

#Run model
#SDI
mod_sdi <- lm(log_pm ~ sdi + monitor_loc + measure_std_new + non_solid, data = dt_sdi[outlier==0]) # Current best model
square <- square[, measure_std_new := 0]
square <- square[, monitor_loc := "personal"]
square <- square[, non_solid := 0]
preds <- c("mean", "standard_error")
out <- square[, (preds) := predict(mod_sdi, square[.BY], allow.new.levels=T, se.fit= T), by=c('location_id', 'year_id')]
out <- out[, predict_pm := exp(mean)]

write.csv(out, file = paste0(out_root, "lm_map_", date,  ".csv"), row.names = F)

#Generate draws from mean and standard error
draws.required <- 1000
draw.colnames <- c(paste0("draw_", 1:draws.required))
out[, index := seq_len(.N)]
out[, (draw.colnames) := rnorm(draws.required, mean=mean, sd=standard_error) %>%
      exp %>%
      as.list,
    by="index", with=F]
write.csv(out, file = paste0(out_root, "lm_pred_", date, ".csv"), row.names = F)

###############################################################################################
###########Generate Man, Woman, Child Ratios###################################################
###############################################################################################

crosswalk <- fread("FILEPATH")

#assign grouping variable
crosswalk[Group=="Female" & age_start>=15, grouping:="female"]
crosswalk[Group=="Male" & age_start>=15, grouping:="male"]
crosswalk[age_end<=25, grouping:="child"]

means <- crosswalk[,mean(personal_pm),by=grouping]
draws <- rbind(means, data.table(grouping="all",V1=mean(means$V1)))

mean_draws <- copy(draws)
mean_draws[, c(draw.colnames) := rnorm(draws.required, mean=V1,sd=20) %>% as.list, by=grouping]

ratio_draws <- copy(mean_draws)
ratio_draws[,c(draw.colnames):=lapply(.SD,function(x) x/x[4]),.SDcols=draw.colnames]

ratio_draws[,ratio:=V1/V1[4]]
setnames(ratio_draws,"V1","mean_pm2.5")

write.csv(ratio_draws[1:3,],file= paste0(out_root, "crosswalk_",date,".csv"), row.names=F)
