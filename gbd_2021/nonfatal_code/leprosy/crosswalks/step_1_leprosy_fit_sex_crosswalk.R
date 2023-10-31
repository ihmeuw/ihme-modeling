# SEX SPLIT - NTDS : LEPROSY 
# Purpose: GBD 2020 using updated MR-BRT crosswalk package to calculate adjustment for data reported as "both" sex to "male/female"

library(crosswalk, lib.loc = "FILEPATH")
library(dplyr)
library(data.table)
library(openxlsx)
library(stringr)
library(tidyr)
library(boot)
library(ggplot2)

# central functions
source("FILEPATH/FUNCTION")
source("FILEPATH/FUNCTION")
source("FILEPATH/FUNCTION")
source("FILEPATH/FUNCTION")
source('FILEPATH/FUNCTION')
source('FILEPATH/FUNCTION')
source('FILEPATH/FUNCTION')
source('FILEPATH/FUNCTION')
source("FILEPATH/FUNCTION")

##### LEPROSY SEX SPLIT CODE - IMPLEMENT CROSSWALK MODEL TO ESTIMATE RATIO OF FEMALES (ref) : BOTH (alt) 
Sci_lit    <- fread("FILEPATH")

#sci lit only keep 0 to 99 records 
Sci_lit3 <-Sci_lit[((Sci_lit$age_start==0 & Sci_lit$age_end==99)| location_id==163 | location_id==135),]
data<-Sci_lit

#subset out male and female
data_fem<-data[data$sex=="Female",]
data_male<-data[data$sex=="Male",]

#create "both" dataset for comparison

#merge together male and female data from same population
data<-merge(data_male,data_fem,by=c("nid","location_id","year_start","age_start","age_end","site_memo"))
data <- data['cases.x' != 0]
data <- data['cases.y' != 0]

#keep mean, se, location_id, year_start, nid, site_memo
#note .x and mean_1=male
data<-data %>% 
  select(nid, location_id, year_start, age_start, age_end, cases.x, cases.y, effective_sample_size.x, effective_sample_size.y, sex.x, sex.y, site_memo, mean.x, mean.y, standard_error.x, standard_error.y) %>%
  rename(mean_1 = mean.x,mean_2 = mean.y, se_1=standard_error.x, se_2=standard_error.y, cases_1=cases.x, cases_2=cases.y, sample_size_1=effective_sample_size.x, sample_size_2=effective_sample_size.y, altvar=sex.x, refvar=sex.y)

data$cases_3<-data$cases_1+data$cases_2
data$eff_ss3<-data$sample_size_1+data$sample_size_2
data$mean_3<-data$cases_3/data$eff_ss3
#use se formula for proportion for now - these data are incidence but we are assuming rare disease
data$se_3<-sqrt(((data$mean_3*(1-data$mean_3))/data$eff_ss3))
data$altvar<-"Both"

#create an index of matches
setDT(data)
data[, id := .I]

#write data as csv to have copy of preliminary dataset
write.csv(data,"FILEPATH")
prelim <- read.csv("FILEPATH")

#visualize - plot males:females by year
ggplot(prelim, aes(x=mean_1, y=mean_2, color=year_start)) +
  geom_point() +
  xlab("Males") + ylab("Females")

#means need to be more than zero - replace them with a very small number 
data[mean_2 == 0, mean_2 := .0000001]
data[mean_3 == 0, mean_3 := .0000001]

#use delta_transform() and calculate_diff() to get log(alt)-lot(ref) or logit(alt)-logit(ref)
#note setting females as alternative
#mean_3=alternative, mean_2=reference

#note we are using linear_to_log since we are modeling incidence
#change to linear_to_logit if you model prevalence
dat_diff <- as.data.frame(cbind(
  delta_transform(
    mean = data$mean_3, 
    sd = data$se_3,
    transformation = "linear_to_log" ),
  delta_transform(
    mean = data$mean_2, 
    sd = data$se_2,
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

#use NID for study_id 

#dataset for meta-regression (e.g., dataset with the M:F comparisons)
df1 <- CWData(
  df = data,          # dataset for metaregression
  obs = "log_diff",       # column name for the observation mean
  obs_se = "log_diff_se", # column name for the observation standard error
  alt_dorms = "altvar",     # column name of the variable indicating the alternative method
  ref_dorms = "refvar",     # column name of the variable indicating the reference method
  #covs = list("year_start"),     # names of columns to be used as covariates later- YEAR here is used to test code functionality
  study_id = "nid",    # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

#Here we run a model for intercept, just to get the sex-ratio  
fit1 <- CWModel(
  cwdata = df1,            # object returned by `CWData()`
  obs_type = "diff_log", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept")),
  #CovModel(cov_name = "year_start")),
  #CovModel(cov_name = "year_start", spline = XSpline(knots = c(0,1,2,3,4), degree = 3L, l_linear = TRUE, r_linear = TRUE), spline_monotonicity = "increasing")),
  gold_dorm = "Female")   # the level of `alt_dorms` that indicates it's the gold standard
# this will be useful when we can have multiple "reference" groups in NMA
#)


#view the results--fixed effects 
fit1$fixed_vars

#store final results
df_result <- fit1$create_result_df()
write.csv(df_result, "FILEPATH")

py_save_object(object = fit1, filename = "FILEPATH", pickle = "dill")
