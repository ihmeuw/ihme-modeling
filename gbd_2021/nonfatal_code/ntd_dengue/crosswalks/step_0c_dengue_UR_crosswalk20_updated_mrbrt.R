### NTDs Dengue
# Description - Fit and apply under-reporting adjusment using MR-BRT

### GBD 2020 IMPLEMENT UNDER-REPORTING ADJUSTMENT USING MR-BRT

##1) LOAD PACKAGES AND CENTRAL FUNCTIONS

library(crosswalk, lib.loc = "FILEPATH")
library(dplyr)
library(data.table)
library(openxlsx)
library(stringr)
library(tidyr)
library(boot)
library(ggplot2)

# central functions

source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_population.R")
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_bundle_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_crosswalk_version.R')
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_covariate_estimates.R")

###2) IMPLEMENT CROSSWALK

#pull in data for xwalk 
# data for meta-regression model
# -- read in data frame
# -- identify variables for ratio and SE
#    NOTE: ratio must be specified as alternative/reference 
#this csv file (dengue_ef3.csv is based off the under-reporting extraction done for GBD 2019)
dat_metareg <- read.csv("FILEPATH/dengue_ef3.csv") 
dat_metareg<-dat_metareg[dat_metareg$group_review==1,]

dat_metareg$ratio_test<-as.numeric(dat_metareg$ratio_test)
dat_metareg$mean<-dat_metareg$mean_2
#summary
summary(dat_metareg$ratio_test)

#drop missing data on SE from comparison group
dat_metareg<-dat_metareg[!is.na(dat_metareg$se_1),]


###keep mean, se, location_id, year_start, nid, site_memo

data<-dat_metareg %>% 
  select(nid, location_id, mean, year_start, age_start, age_end, site_memo, mean_1, mean_2, se_1, se_2, sample_size1, sample_size2, def_1, def_2)

data$year_id<-data$year_start

#merge with covariate values
#create list of locations
locs_cov<-unique(dat_metareg$location_id)
yrs_cov<-unique(dat_metareg$year_start)

#get covariate estimates - pop density, haqi, dengue probability
covar <- ADDRESS
covs<-get_covariate_estimates(covariate_id=covar, year_id=yrs_cov, decomp_step = ADDRESS,location_id=locs_cov,gbd_round_id=ADDRESS)
covs$year_start<-covs$year_id
covs$sex<-NULL

#merge covariate value to main dataset to fit xwalk model 
data<-merge(data,covs,by=c("location_id","year_id"))
#mean_value=haqi

data$cov<-data$mean_value
 
#need to check distribution of covar
summary(data$haqi)
#write data as csv to save copy

write.csv(data,"FILEPATH/prelim_UR_xwalk.csv")

#compare active (mean_1) to passive (mean_2) by year
ggplot(data, aes(x=mean_1, y=mean_2, color=year_id)) +
  geom_point()

#note we are using linear_to_log since we are modeling incidence
#change to linear_to_logit if you model prevalence
dat_diff <- as.data.frame(cbind(
  delta_transform(
    mean = data$mean_2, 
    sd = data$se_2,
    transformation = "linear_to_log" ),
  delta_transform(
    mean = data$mean_1, 
    sd = data$se_1,
    transformation = "linear_to_log")
))


#recode names 
names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")

#note log_diff or logit_diff
data[, c("log_diff", "log_diff_se")] <- calculate_diff(
  df = dat_diff, 
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref" )


######## prep for running in crosswalk package ##############################

#dataset for meta-regression (e.g., dataset with the M:F comparisons)
df1 <- CWData(
  df = data,          # dataset for metaregression
  obs = "log_diff",       # column name for the observation mean
  obs_se = "log_diff_se", # column name for the observation standard error
  alt_dorms = "def_2",     # column name of the variable indicating the alternative method
  ref_dorms = "def_1",     # column name of the variable indicating the reference method
  covs = list("cov"),     # names of columns to be used as covariates later- HAQI
  study_id = "nid",    # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

#Here we run a model for intercept, just to get the sex-ratio  
fit1 <- CWModel(
  cwdata = df1,            # object returned by `CWData()`
  obs_type = "diff_log", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
  CovModel(cov_name = "intercept"),
  CovModel(cov_name = "cov")),
  #CovModel(cov_name = "cov", spline = XSpline(knots = c(0,.2, .4, .6), degree = 3L, l_linear = TRUE, r_linear = TRUE), spline_monotonicity = "decreasing")),
  gold_dorm = "active",  # the level of `alt_dorms` that indicates it's the gold standard
  use_random_intercept=TRUE)   
# this will be useful when we can have multiple "reference" groups in NMA
#)


#view the results--fixed effects 
fit1$fixed_vars

#store fit1


#############################


##apply crosswalk to incidence data 

#read in all-age original incidence data - from step 0b
dat_original<-read.csv("FILEPATH/prep_URxwalk.csv")
#drop if person*years with small sample size
dat_original<-dat_original[dat_original$unit_type!="Person*year"| dat_original$sample_size>1500,]

#recode any original incidence >.1 to "active"

dat_original<-dat_original%>%
  mutate(reporting=if_else(mean>.075,"active","passive"))

#drop if mean==0
dat_original<-dat_original[dat_original$mean>0,]

#need to merge with covariate values

#get covariate estimates - based on covariate id argument passed above

locs2<-unique(dat_original$location_id)
yrs2<-unique(dat_original$year_start)
covs_dt<-get_covariate_estimates(covariate_id=covid, year_id=yrs2, decomp_step = ADDRESS,location_id=locs2, gbd_round_id=ADDRESS)
covs_dt$year_start<-covs_dt$year_id
covs_dt$sex<-NULL


dat_original<-merge(dat_original,covs_dt,by=c("location_id","year_start"))
#mean_value=haqi
dat_original$haqi<-dat_original$mean_value
dat_original$mean_value<-NULL

setDT(dat_original)
dat_original[, nid := as.character(nid)]
#convert passive to active
dat_original[, c("inc_adj", "inc_adj_se", "diff", "diff_se","data_id") ] <- adjust_orig_vals(
  fit_object = fit1,       # result of CWModel()
  df = dat_original,            # original data with obs to be adjusted
  orig_dorms = "reporting", # name of column with (all) def/method levels
  orig_vals_mean = "mean",  # original mean
  orig_vals_se = "standard_error"  # standard error of original mean
)

#plot comparison between mean and inc2

ggplot(dat_original, aes(x=mean, y=inc_adj, color=reporting)) +
  geom_point(shape=1) + theme_bw()+
  scale_colour_hue(l=50) + 
  xlim(0, .25)+
  ylim(0, .25)+ xlab("Reported Incidence") + ylab("Adjusted Incidence") +ggtitle("Reported v. Adjusted Incidence")


##need to recode exceedingly high incidence after adjustment
summary(dat_original$inc_adj)

# Proceed to next step to sex and age split dengue data

###################################################################

#create final_data = a dataset that will be used to move through additional data prep

final_data<-dat_original

#output final data

#############################################################
final_data$new_cases<-final_data$inc_adj*final_data$sample_size
final_data$old_cases<-final_data$cases
final_data$cases<-NULL

final_data$orig_mean<-final_data$mean
final_data$mean<-final_data$inc_adj
final_data$variance<-final_data$inc_adj_se^2

test<-final_data[final_data$inc_adj>.5,]

#plot observed v. crosswalked data
# Same, but with different colors and add regression lines

ggplot(final_data, aes(x=orig_mean, y=inc_adj, color=reporting)) +
  geom_point(shape=1) + theme_bw()+
  scale_colour_hue(l=50) + 
  xlim(0, .2)+
  ylim(0, .2)+ xlab("Reported Incidence") + ylab("Adjusted Incidence") +ggtitle("Reported v. Adjusted Incidence")

final_data$old_cases<-as.numeric(final_data$old_cases)

#create ratio variable
final_data$ratio<-final_data$inc_adj/final_data$orig_mean
summary(final_data$ratio)

#correct inputs -return to original incidence >.5
final_data2<-final_data%>%
  mutate(new_cases=if_else(inc_adj>.5,old_cases,new_cases))%>%
  mutate(inc_adj=if_else(inc_adj>.5,orig_mean,inc_adj))
  

#copy results of crosswalk for reference/de-bugging as needed: 
write.csv(final_data2,"FILEPATH/dengue_URxwalk.csv")
#otput if you choose to further sex split, same file as above: 
write.csv(final_data2,"FILEPATH/ur_prep_sex_split.csv")

test<-final_data2[final_data2$location_id==17,]
