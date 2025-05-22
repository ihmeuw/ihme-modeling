#-------------------Header------------------------------------------------
# Author: 
# Date: 
# Purpose: Prep NO2 RR extractions for evidence scoring

#------------------Set-up--------------------------------------------------

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

project <- "-P proj_erf "
sge.output.dir <- " -o FILEPATH -e FILEPATH  "
#sge.output.dir <- "" # toggle to run with no output files


# load packages, install if missing
lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","ggplot2","openxlsx","metafor","pbapply")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

#------------------Directories and shared functions-----------------------------

model.version <- 22 # GBD 2023 for all years we have data

out_dir <- paste0("FILEPATH/air_no2/rr/models/", model.version)
dir.create(out_dir,recursive = T)

# load shared functions
source("FILEPATH/r/get_outputs.R")
source("FILEPATH/r/get_covariate_estimates.R")

# load current locations list
source("FILEPATH/r/get_location_metadata.R")
location_set_version <- 35
locations <- get_location_metadata(location_set_version, release_id = 16)



#------------------Prep RR data-------------------------------------------------

data <- read.xlsx(file.path("FILEPATH"),startRow=3, colNames=F, skipEmptyRows = T, skipEmptyCols = F, sheet = 1) %>% as.data.table
names(data) <- read.xlsx(paste0("FILEPATH"), sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names

# remove observations flagged for exclusion (multiple studies on the same cohort)
# NOTE: 81 observations were removed
data <- data[is_outlier!=1]

# drop observations with an undefined NO2_conc_increment for now
data <- data[nid!=416839]
# drop observations with missing NO2 exposure distribution information
data <- data[nid!=436414]

#------------------Convert exposures to ppb-------------------------------------

# convert all NO2 concentration increments to ppb
# using US EPA conversion factor of 0.5319 * micrograms/m^3 = ppb
# 1 ppb = 1.88 micrograms/m^3
data[,NO2_conc_increment:=as.numeric(NO2_conc_increment)]
data[NO2_conc_units=="microgram/m^3", NO2_conc_increment:=NO2_conc_increment*.5319]

# convert all NO2 exposure means to ppb
data[NO2_conc_units=="microgram/m^3", NO2_conc_min:=as.numeric(NO2_conc_min)*.5319]
data[NO2_conc_units=="microgram/m^3", NO2_conc_2.5:=as.numeric(NO2_conc_2.5)*.5319]
data[NO2_conc_units=="microgram/m^3", NO2_conc_97.5:=as.numeric(NO2_conc_97.5)*.5319]
data[NO2_conc_units=="microgram/m^3", NO2_conc_sd:=as.numeric(NO2_conc_sd)*.5319]
data[NO2_conc_units=="microgram/m^3", NO2_conc_median:=as.numeric(NO2_conc_median)*.5319]

# convert IQR to numeric value
convert_to_numeric <- function(x){
  iqr <- strsplit(x, "-")
  iqr <- as.numeric(iqr[[1]])
  numeric <- iqr[2]-iqr[1]
  return(numeric)
}

data <- data[grepl("-",NO2_conc_iqr), NO2_conc_iqr:=convert_to_numeric(NO2_conc_iqr)]
data[,NO2_conc_iqr:=as.numeric(NO2_conc_iqr)]
data[NO2_conc_units=="microgram/m^3", NO2_conc_iqr:=NO2_conc_iqr*.5319]

data[,NO2_conc_units:="ppb"]

#------------------Estimate 5th/95th percentile concentrations----------------------
# estimate median if missing
data[is.na(NO2_conc_median), NO2_conc_median := NO2_conc_mean]

# estimate the sd if missing
data[is.na(NO2_conc_sd), NO2_conc_sd := NO2_conc_iqr/1.35]  # SD can be estimated by IQR/1.35
data[is.na(NO2_conc_sd), NO2_conc_sd :=(NO2_conc_max-NO2_conc_min)/4]  # SD can be estimated by range/4

# for the rows missing the median/SD, use the average median/SD across all (or if there's enough data use average in SR or R)
data <- merge(data,locations[,c("ihme_loc_id","super_region_id","region_id")],by="ihme_loc_id")

missing <- data[is.na(NO2_conc_sd)|is.na(NO2_conc_median)]
missing_r_ids <- unique(missing$region_id)

for (id in missing_r_ids){
  median <- mean(data[region_id==id,NO2_conc_median],na.rm=T)
  sd <- mean(data[region_id==id,NO2_conc_sd],na.rm=T)
  data[is.na(NO2_conc_median) & region_id==id, NO2_conc_median:=median]
  data[is.na(NO2_conc_sd) & region_id==id, NO2_conc_sd:=sd]
}

# estimate 5th and 95th percentile concentrations (use mean, sd, & z-score)
data[is.na(NO2_conc_mean), NO2_conc_mean:=NO2_conc_median] # for rows missing the mean concentration, estimate with the median
data[,NO2_conc_5 := NO2_conc_mean - NO2_conc_sd * 1.645]
data[,NO2_conc_95 := NO2_conc_mean + NO2_conc_sd * 1.645]

# if our estimates for 5th and 95th are outside the range of the study, fix to min/max
data[NO2_conc_5 < NO2_conc_min, NO2_conc_5 := NO2_conc_min]
data[NO2_conc_95 > NO2_conc_max, NO2_conc_95 := NO2_conc_max]

# if our estimates are below 0, set to 0
data[NO2_conc_5<0, NO2_conc_5:= 0]
data[NO2_conc_95<0, NO2_conc_95:=0]

#------------------rescale RRs to exp_conc 5th/95th & calculate the SE from CI bounds------------------------------------

# rescale & transform data into log space
data[,log_mean_rescaled:=log(mean^((NO2_conc_95-NO2_conc_5)/NO2_conc_increment))]
data[,log_upper_rescaled:=log(upper^((NO2_conc_95-NO2_conc_5)/NO2_conc_increment))]
data[,log_lower_rescaled:=log(lower^((NO2_conc_95-NO2_conc_5)/NO2_conc_increment))]

data[,log_se:=(log_upper_rescaled -log_lower_rescaled)/(qnorm(0.975)*2)]

# new GBD 2020: for multiple datapoints from the same cohort, we want to weight their SEs by sqrt(n)
weight_ss <- function(c){
  
  # make a temporary dataset for each nid
  temp <- data[study==c]
  
  # if sample size is empty, assign it to 1 (so we can avoid NAs; we'll put it back at the end)
  temp[is.na(sample_size),sample_size:=1]
  
  if (nrow(temp)>1){
    
      # do each sample size separately (because we only want to weight observations that use the same sample)
      ss <- unique(temp[,sample_size])
      for (s in ss){
        
        n_observations <- nrow(temp[sample_size==s])
        temp[sample_size==s,log_se_weighted:=(log_se*sqrt(n_observations))]
      }
    
  } else {
    
    # if there is only 1 observation for that nid, we can skip the whole weighting process
    temp[,log_se_weighted:=log_se]
  }
  
  # put back the NA for sample size
  temp[sample_size==1,sample_size:=NA]
  
  return(temp)
}

temp <- pblapply(unique(data$study),weight_ss,cl=4) %>% rbindlist

# plot to check weighted vs original SEs
plot <- ggplot(temp, aes(x = log_se_weighted, y = log_se, color = nid)) +
  geom_point()+
  geom_abline(slope = 1, intercept = 0)
print(plot)

data <- temp


#------------------Code continuous/multilevel covariates as binary-----------------------------------
# These are the covariates we need to convert:

##### cv_duration_fup #####
# make sure all cross-sectional studies have a follow-up time of 0
# this means that, for cross-sectional studies, we count them as <5 years fup
data[source_type=="cross-sectional", duration_fup_measure:=NA]
data[source_type=="cross-sectional", duration_fup_units:=NA]
data[source_type=="cross-sectional", value_of_duration_fup:=0]

# convert follow-up time to a binary indicator covariate
data[,cv_duration_fup:=ifelse(value_of_duration_fup>=5,0,1)]


##### cv_confounding_uncontrolled #####
# make binary indicator covariates for each of the levels of confounding uncontrolled (0,1,2)
# we leave out one of the levels (Level 0) to avoid co-linearity
setnames(data,"cv_counfounding_uncontrolled","cv_confounding_uncontrolled")
data[,cv_confounding_uncontrolled_1:=ifelse(cv_confounding_uncontrolled==1,1,0)]
data[,cv_confounding_uncontrolled_2:=ifelse(cv_confounding_uncontrolled==2,1,0)]

# drop unnecessary info
data[,cv_confounding_uncontrolled:=NULL]

##### cv_selection_bias #####
# if cv_selection_bias is missing, fill in with the worst-case scenario (2)
data[is.na(cv_selection_bias),cv_selection_bias:=2]

# make binary indicator covariates for each of the levels of selection bias (0,1,2)
# we leave out one of the levels (Level 0) to avoid co-linearity
data[,cv_selection_bias_1:=ifelse(cv_selection_bias==1,1,0)]
data[,cv_selection_bias_2:=ifelse(cv_selection_bias==2,1,0)]

# drop unnecessary info
data[,cv_selection_bias:=NULL]

##### cv_exposure_timeframe #####
windows <- unique(data$cv_exposure_timeframe)

# if any studies are missing an exposure timeframe, assume it's "current"
data[is.na(cv_exposure_timeframe), cv_exposure_timeframe:="current"]

# make indicator covariates for each of the exposure windows
data[,cv_exp_prenatal:=cv_postnatal] # we already have a covariate for prenatal exposure 
data[,cv_exp_lifetime:=ifelse(cv_exposure_timeframe=="lifetime",1,0)] # lifetime exposure
data[,cv_exp_birth:=ifelse(cv_exposure_timeframe=="birth",1,0)] # birth year/birth address exposure
data[,cv_exp_current:=ifelse(cv_exposure_timeframe=="current",1,0)] # current year/current address exposure
data[,cv_exp_yo_incidence:=ifelse(cv_exposure_timeframe=="year of incidence",1,0)] # year of incidence

# the leftover categories are: first 3 years, first 2 years, preconceptional, first trimester, second trimester, & third trimester
# the preconceptional and pregnancy trimesters are already captured in the "cv_exp_prenatal" covariate, so it's okay to drop that info
# (we're not modeling specific trimesters of exposure for now--not enough data)
# code first 2 or 3 years to "birth" (for birth address exposure)
data[cv_exposure_timeframe=="other (first 3 years)" | cv_exposure_timeframe=="other (first 2 years)", cv_exp_birth:=1]

# drop unnecessary info
data[,cv_exposure_timeframe:=NULL]
data[,cv_postnatal:=NULL]


# clean up the dataset and get rid of unnecessary columns
# these covariates are the same for all rows of data (exclude them): 
# cv_exposure_study,cv_exposure_selfreport,cv_outcome_unblinded,cv_reverse_causation,cv_confounding_nonrandom
data <- data[,c("nid","field_citation_value","file_path","source_type",
                "location_name","location_id","ihme_loc_id","study",
                "measure","sample_size",
                # rr estimates
                "log_mean_rescaled","log_se","log_se_weighted",
                # exposure
                "NO2_conc_mean","NO2_conc_median","NO2_conc_iqr", "NO2_conc_5", "NO2_conc_95",
                # covariates
                "cv_subpopulation","cv_exposure_population","cv_PM_controlled","cv_outcome_selfreport",
                "cv_confounding_uncontrolled_1","cv_confounding_uncontrolled_2",
                "cv_selection_bias_1","cv_selection_bias_2",
                "cv_exp_prenatal","cv_exp_lifetime","cv_exp_birth","cv_exp_current",
                "cv_exp_yo_incidence","cv_duration_fup")]


#------------------Additional data prep (revisions due to ES recommendation changes)------------------------------------

#### do a little more pre-processing for covariates (we have too many for CovFinder)

data[is.na(cv_exp_prenatal),cv_exp_prenatal:=0]

# remove "current" from the exposure windows covariates to make it the default (avoid colinearity)
data[,cv_exp_current:=NULL]

#------------------Code covariates as interaction terms too------------------------------------------
# to do this, we multiply all the cv columns by the exposure column 
# when we used a spline approach, we used the 5th to 95th NO2_exp_conc, because that's what we used to fit the model

data[,cv_exp_subpopulation:=cv_subpopulation*(NO2_conc_95-NO2_conc_5)]
data[,cv_exp_exposure_population:=cv_exposure_population*(NO2_conc_95-NO2_conc_5)]
data[,cv_exp_PM_controlled:=cv_PM_controlled*(NO2_conc_95-NO2_conc_5)]
data[,cv_exp_outcome_selfreport:=cv_outcome_selfreport*(NO2_conc_95-NO2_conc_5)]
data[,cv_exp_confounding_uncontrolled_1:=cv_confounding_uncontrolled_1*(NO2_conc_95-NO2_conc_5)]
data[,cv_exp_confounding_uncontrolled_2:=cv_confounding_uncontrolled_2*(NO2_conc_95-NO2_conc_5)]
data[,cv_exp_selection_bias_1:=cv_selection_bias_1*(NO2_conc_95-NO2_conc_5)]
data[,cv_exp_selection_bias_2:=cv_selection_bias_2*(NO2_conc_95-NO2_conc_5)]
data[,cv_exp_exp_prenatal:=cv_exp_prenatal*(NO2_conc_95-NO2_conc_5)]
data[,cv_exp_exp_lifetime:=cv_exp_lifetime*(NO2_conc_95-NO2_conc_5)]
data[,cv_exp_exp_birth:=cv_exp_birth*(NO2_conc_95-NO2_conc_5)]
data[,cv_exp_exp_yo_incidence:=cv_exp_yo_incidence*(NO2_conc_95-NO2_conc_5)]
data[,cv_exp_duration_fup:=cv_duration_fup*(NO2_conc_95-NO2_conc_5)]



#------------------Save prepped file-----------------------------------------------------------------
# save this prepped file in the 
print(nrow(data))
length(names(data)[grepl("cv_",names(data))])

# how many locations?
length(unique(data$location_id))
# how many nids?
length(unique(data$nid))

write.csv(data, file=paste0(out_dir,"/input_data.csv"),row.names = F)


