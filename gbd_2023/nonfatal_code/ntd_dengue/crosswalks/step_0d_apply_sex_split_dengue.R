################################################################################
# Dengue sex split code - run fit mr-brt sex crosswalk model and sex split data

################################################################################
#1) LOAD PACKAGES AND CENTRAL FUNCTIONS
#library(crosswalk, lib.loc = "FILEPATH")
rm(list=ls())

##1) LOAD PACKAGES AND CENTRAL FUNCTIONS
#Sys.setenv("RETICULATE_PYTHON" = "FILEPATH")
library(reticulate)
reticulate::use_python("FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")
library(crosswalk002)
library(mrbrt002)
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
################################################################################
# set paths and variables
release_id <- ADDRESS
path <- paste0("FILEPATH")

################################################################################
##### DENGUE SEX SPLIT CODE - IMPLEMENT CROSSWALK MODEL TO ESTIMATE RATIO OF FEMALES (ref) : BOTH (alt) 

data<-read.csv(paste0(path,"FILEPATH"))

data$cases_3<-data$cases_1+data$cases_2
data$eff_ss3<-data$sample_size_1+data$sample_size_2
data$mean_3<-data$cases_3/data$eff_ss3

# use se formula for proportion 
data$se_3<-sqrt(((data$mean_3*(1-data$mean_3))/data$eff_ss3))
data$altvar<-"Both"
data$refvar<-"female"

# create an index of matches
setDT(data)
data[, id := .I]

# write data as csv 
write.csv(data,paste0(path,"FILEPATH"), row.names = FALSE)

# mean_1=males, mean_2=females
# compare males:females by year
ggplot(data, aes(x=mean_1, y=mean_2, color=year)) +
  geom_point()

# use delta_transform() and calculate_diff() to get log(alt)-lot(ref) or logit(alt)-logit(ref)
# note setting females as alternative
# mean_3=alternative, mean_2=reference
# note we are using linear_to_log since we are modeling incidence
# change to linear_to_logit if you model prevalence
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

# recode names 
names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")

# note log_diff or logit_diff
data[, c("log_diff", "log_diff_se")] <- calculate_diff(
  df = dat_diff, 
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref" )

################################################################################
######## prep for running in crosswalk package ##############################

# dataset for meta-regression (e.g., dataset with the M:F comparisons)
df1 <- CWData(
  df = data,          # dataset for metaregression
  obs = "log_diff",       # column name for the observation mean
  obs_se = "log_diff_se", # column name for the observation standard error
  alt_dorms = "altvar",     # column name of the variable indicating the alternative method
  ref_dorms = "refvar",     # column name of the variable indicating the reference method
  
  study_id = "id",    # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

# Here we run a model for intercept, just to get the sex-ratio  
fit1_sex <- CWModel(
  cwdata = df1,            # object returned by `CWData()`
  obs_type = "diff_log", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    crosswalk002::CovModel(cov_name = "intercept")),
  
    gold_dorm = "female")   # the level of `alt_dorms` that indicates it's the gold standard


# save out model
py_save_object(object = fit1_sex, filename = paste0(path,"FILEPATH"), pickle = "dill")

# view the results--fixed effects 
fit1_sex$fixed_vars
fit1_sex$beta
fit1_sex$beta_sd
fit1_sex$gamma

# calculations for GBD write-ups
# beta_mean +/- 1.96 * sqrt(beta_se^2 + gamma)
#-0.06894686 +/- 1.96 * sqrt(0.002691071^2 + 0.005140038) = 0.07167245  ; -0.2095662
#-0.06894686 +/- 1.96 * sqrt(0.002691071^2 + 0.005140038) = 0.07167245  ; -0.2095662

################################################################################
###########END of Crosswalk model fit ###################
## apply sex split to data that have been adjusted for under-reporting
dat2<-as.data.table(read.csv(paste0(path,'FILEPATH'))) 
dat2 <- subset(dat2, select = -c(population))
table(dat2$drop_post_sexsplit)
dat2 <- subset(dat2, is.na(drop_post_sexsplit))
aaa <- subset(dat2, upper < mean)
dat2<-dat2[dat2$measure=="incidence",]

# check for sex specific data
table(dat2$sex)

# store these records and append later
keep_sex<-dat2[(sex!="Both")]

# pull subset to 'both' sex data points only 
dat2 <- dat2[(sex=="Both")]

# we don't model data where mean = 0
dat2<-dat2[inc_adj>0,]

####  apply sex split  ####
# convert both to female
dat2[, c("sex_split", "sex_split_se", "sex_diff", "sex_diff_se", "data_id")] <- adjust_orig_vals(
  fit_object = fit1_sex,       # result of CWModel()
  df = dat2,            # original data with obs to be adjusted
  orig_dorms = "sex", # name of column with (all) def/method levels
  orig_vals_mean = "inc_adj",  # original mean
  orig_vals_se = "inc_adj_se"  # standard error of original mean
)

################################################################################
# create male estimates via subtraction by subtracting mean female estimate from both
#1) generate new case estimates from the adjusted mean
# need female population
# need male population
# list of years in dataset 
years <- unique(dat2$year_start)
# list of locations in dataset
locs <- unique(dat2$location_id)

malepop <- get_population(release_id = release_id, location_id=locs, year_id=years, sex_id = 1, age_group_id=ADDRESS)
fempop <- get_population(release_id = release_id, location_id=locs, year_id=years, sex_id = 2, age_group_id=ADDRESS)

# merge populations
malepop$year_start  <-malepop$year_id
fempop$year_start <- fempop$year_id

dat2_b <- merge(dat2, malepop, by=c("location_id","year_start")) #,"age_group_id", by.x=TRUE, by.y=FALSE))

# rename population variable
dat2_b <- dat2_b %>% 
  dplyr::rename(male_pop = population)

dat2_c <- merge(dat2_b, fempop, by=c("location_id","year_start")) #, all=TRUE, allow.cartesian=TRUE)

dat2_c<-dat2_c %>% 
  dplyr::rename(fem_pop = population)

# drop variables for clean up
dat2_c <- dplyr::select(dat2_c,-c(sex_id.x,sex_id.y,year_id.x,year_id.y, run_id.y,run_id.x, age_group_id.y,age_group_id.x,upper, lower, standard_error, mean, effective_sample_size ))

# calculate %female
# b6356_bs_b[,fem_m_ratio := fem_pop/male_pop]
dat2_c$tot_pop<-dat2_c$fem_pop+dat2_c$male_pop
dat2_c[,fem_per := fem_pop/tot_pop]

# calculate male and female denominator
dat2_c[,fem_ss := (fem_per*sample_size)]
dat2_c[,male_ss := sample_size-(fem_per*sample_size)]

#### take adjusted incidence x female population
# here assumption around 1 year for person-time
# this works fine for prevalence - need to consider for incidence (rare disease assumption)
dat2_c[,fem_cases := sex_split*fem_ss]
dat2_c[,male_cases := new_cases-fem_cases]
dat2_c[,mean_male := male_cases/male_ss]
test<-dat2_c[(male_cases>male_ss)]
test<-dat2_c[(male_cases<0)]
# drop for locations where sample size is not nationally representative
dat2_c<-dat2_c[dat2_c$male_cases<dat2_c$male_ss,]

# need to update the standard error for males and females
# http://www.kaspercpa.com/statisticalreview.htm
# Multiplying a random variable by a constant increases the variance by the square of the constant.

# recalculate SE of males - multiply by the SE from the MR-BRT model
dat2_c[measure == "incidence", se_male := sqrt((1/male_ss)*(male_cases/male_ss)*(1-(male_cases/male_ss)))+sex_split_se]

## split data and append male and female records, took out group_review, ihme_loc_id, 
## case definition, AND group variables because DNE, are they necessary for upload?
males<-dat2_c %>% 
  select(nid, location_id, year_start, year_end, age_start, group_review, case_definition, 
         ihme_loc_id, group, unit_value_as_published, measure, recall_type, representative_name, 
         source_type, unit_type, urbanicity_type, uncertainty_type_value, age_end, seq, is_outlier, 
         case_name, bundle_id, cause_id,male_cases, mean_male, se_male, male_ss) %>%
  rename(mean = mean_male, cases = male_cases, standard_error=se_male, effective_sample_size=male_ss)

males_ss <- males
males_ss$sex <- 'Male'
males_ss$sex_id <- 1

# repeat for females
females<-dat2_c %>% 
  select(nid, location_id, year_start, year_end, age_start, group_review, case_definition, ihme_loc_id, group, unit_value_as_published, measure, age_end, recall_type, representative_name, source_type, unit_type, urbanicity_type, uncertainty_type_value, seq, is_outlier, case_name, bundle_id, cause_id,fem_cases, sex_split,sex_split_se, fem_ss) %>%
  rename(mean = sex_split, cases = fem_cases, standard_error=sex_split_se, effective_sample_size=fem_ss)

females_ss <- females

# need sex_id for age split code
females_ss$sex <- 'Female'
females_ss$sex_id <- 2

# append together
split_dat2<- rbind(males_ss,females_ss)
split_dat2$note_modeler<-"sexsplit"

keep_sex[, standard_error :=inc_adj_se ]

# add sex specific data back in
all_data<-rbind(keep_sex, split_dat2,fill=TRUE)
all_data[, val := mean]
all_data <- subset(all_data, select = -c(upper, lower))

### add seq
all_data[, crosswalk_parent_seq := seq]

## save
write.csv(all_data,paste0(path,"FILEPATH"), row.names = FALSE) 