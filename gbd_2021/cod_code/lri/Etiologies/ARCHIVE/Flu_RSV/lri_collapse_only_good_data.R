
######################################################################################
## Follow general pattern of LRI survey prep but only keep surveys with best definition ##
######################################################################################

#Toggle for whether we drop 0-case rows for which SE cannot be calculated, & keep SE
dropping_zeros = FALSE

### PREP ###
#Set filepaths
root <- # filepath
currentdata <- # filepath

library(boot)
#Geospatial setup for the cluster
#Set number of cores
numcores = 30
#Set repo
repo <- # filepath
setwd(repo)
#Source MBG central code
source('filepath/collapse_functions.R')
source('filepath/covariate_functions.R')
source('filepath/holdout_functions.R')
source('filepath/mbg_functions.R')
source('filepath/misc_functions.R')
source('filepath/polygon_functions.R')
source('filepath/post_estimation_functions.R')
source('filepath/prep_functions.R')
source('filepath/seegMBG_transform_functions.R')
source('filepath/shiny_functions.R')
source('filepath/shapefile_functions.R')
#Read in packages
package_lib <- # filepath
.libPaths(package_lib)
package_list <- c('survey', 'foreign', 'rgeos', 'data.table','raster',
                  'INLA', 'plyr', 'foreach', 'doParallel', 'ggplot2', 
                  'lme4', 'dplyr', 'matrixStats', 'rgdal', 'seegSDM', 'seegMBG', 'stringr')
for(package in package_list) {
  library(package, lib.loc = package_lib, character.only=TRUE)
}
j_root <- # filepath

#Read in supplementary information
locs <- fread("filepath")
locs <- locs[,c("location_name","location_id","super_region_name","super_region_id","region_name","region_id","parent_id", "ihme_loc_id","exists_subnational")]

mm_scalar <- read.csv("filepath") #seasonality scalars & info from surveys without month microdata
dm.coeffs <- fread("filepath") #DisMod crosswalk coefficients
duration <- read.csv("filepath") #LRI duration draws

#Read in dataset, rename columns
load(paste0(root, currentdata))
lri <- as.data.table(all)
rm(all)
setnames(lri, old = c('survey_name', 'iso3', 'year_start', 'year_end'), new = c('survey_series', 'ihme_loc_id', 'start_year', 'end_year'))

#Merge locs & seasonality scalars onto dataset
lri <- join(lri, locs, by = "ihme_loc_id")

months <- fread("filepath") #seasonality scalars
# Use GAM scalar #
months$scalar <- months$gamscalar

scalar <- months[,c("scalar","month","region_name")]
setnames(lri, old = 'int_month', new = 'month')
lri <- join(lri, scalar, by=c("region_name","month"))

### CLEAN UP DATA ###
#Change NA's to 9's in symptom columns
lri$had_fever[is.na(lri$had_fever)] = 9
lri$had_cough[is.na(lri$had_cough)] = 9
lri$diff_breathing[is.na(lri$diff_breathing)] = 9
lri$chest_symptoms[is.na(lri$chest_symptoms)] = 9

## Keep only surveys with best definition ##
lri$chest <- ave(lri$chest_symptoms, lri$nid, FUN=function(x) min(x, na.rm=T))
lri$fever <- ave(lri$had_fever, lri$nid, FUN=function(x) min(x, na.rm=T))

lri$keep <- ifelse(lri$chest==0 & lri$fever==0, 1, 0)
best <- subset(lri, keep==1)

#Drop any NIDs completely missing difficulty breathing responses
best$tabulate <- ave(best$diff_breathing, best$nid, FUN= function(x) min(x))
best <- subset(best, tabulate != 9)

#Drop rows w/o sex, round ages, drop over 5s
setnames(best, old = 'sex_id', new = 'child_sex')
best$child_sex[is.na(best$child_sex)] = 3
best$age_year <- floor(best$age_year)
best <- subset(best, age_year <= 4)

#Reassign pweight = 1 if missing or 0
#Include pwt_adj indicator if reassigned to 1
best <- best[, pwt_adj := 0]
best <- best[is.na(pweight) | pweight == 0, pwt_adj := 1]
best <- best[is.na(pweight), pweight := 1]
best <- best[pweight == 0, pweight := 1]

best <- best[, start_n := .N, by = c("nid", "ihme_loc_id", "start_year")]

#Drop if missing PSU 
best <- subset(best, !is.na(psu))
best <- best[,psudrop_n := .N, by = c("nid", "ihme_loc_id", "start_year")]

### COLLAPSE TO COUNTRY-YEAR-AGE-SEX ###
#Data frame for outputs
df <- data.frame()

#Loop over surveys to 1) Assign case definitions, 2) Incorporate seasonality, 3) Apply surveydesign & collapse to country-year-age-sex
nid.new <- unique(best$nid[!is.na(best$nid)])
num <- 1
for(n in nid.new) {
  print(paste0("On survey number ",num," of ",length(nid.new)))
  temp <- subset(best, nid == n)
  
  temp$diff_breathing <- ifelse(temp$diff_breathing==1,1,0)
  temp$chest_symptoms <- ifelse(temp$chest_symptoms==1,1,0)
  temp$diff_fever <- ifelse(temp$had_fever==1 & temp$diff_breathing==1,1,0)
  temp$chest_fever <- ifelse(temp$had_fever==1 & temp$chest_symptoms==1,1,0)
  temp$obs <- 1
  
  #Pull in recall period (if bsswlank, assume it's 2 weeks)
  temp$recall_period <- ifelse(is.na(temp$recall_period_weeks), 2, temp$recall_period_weeks)
  recall_period <- max(temp$recall_period, na.rm=T)

  #Add missing month seasonality scalars
  # if(n %in% mm_scalar$nid) {
  #   mm_scalar = as.data.table(mm_scalar)
  #   mm_short = mm_scalar[, c("nid", "country", "avg_scalar")]
  #   mm_short$scalar = as.numeric(NA) #add on NA column so it only merges on rows missing scalars
  #   #merge in missing month scalars
  #   temp = temp[, -c(11, 15)]
  #   temp = merge(temp, mm_short, by.x = c("nid", "ihme_loc_id", "scalar"), by.y = c("nid", "country", "scalar"), all.x = TRUE)
  #   temp$scalar = ifelse(!is.na(temp$avg_scalar), temp$avg_scalar, temp$scalar)
  #   temp = within(temp, rm(avg_scalar))
  # }
  #Apply seasonality scalars
  scalar.dummy <- max(temp$scalar)
  if(is.na(scalar.dummy)) {
    temp$scalar <- 1
  } 
  temp$diff_breathing <- temp$diff_breathing * temp$scalar
  temp$diff_fever <- temp$diff_fever * temp$scalar
  temp$chest_symptoms <- temp$chest_symptoms * temp$scalar
  temp$chest_fever <- temp$chest_fever * temp$scalar
  
  #Apply survey design & collapse
    if(length(unique(temp$psu))>1){
      dclus <- svydesign(id=~psu, weights=~pweight, data=temp)
      prev <- svyby(~diff_breathing, ~child_sex + age_year, dclus, svymean, na.rm=T)
      prev$diff_fever <- svyby(~diff_fever, ~child_sex + age_year, dclus, svymean, na.rm=T)$diff_fever
      prev$chest_symptoms <- svyby(~chest_symptoms, ~child_sex + age_year, dclus, svymean, na.rm=T)$chest_symptoms
      prev$chest_fever <- svyby(~chest_fever, ~child_sex + age_year, dclus, svymean, na.rm=T)$chest_fever
      prev$sample_size <- svyby(~diff_breathing, ~child_sex + age_year, dclus, unwtd.count, na.rm=T)$count
      prev$ihme_loc_id <- unique(substr(temp$subname,1,3))
      prev$location <- unique(temp$location_name)
      prev$start_year <- unique(temp$start_year)
      prev$end_year <- unique(temp$end_year)
      prev$nid <- unique(temp$nid)
      prev$sex <- ifelse(prev$child_sex==2,"Female",ifelse(prev$child_sex==1,"Male","Both"))
      prev$age_start <- prev$age_year
      prev$age_end <- prev$age_year + 1
      prev$recall_period <- recall_period
      
      df <- rbind.data.frame(df, prev)
    } 
  num <- num + 1
}

#Export for further prep #
write.csv(df, "filepath")

