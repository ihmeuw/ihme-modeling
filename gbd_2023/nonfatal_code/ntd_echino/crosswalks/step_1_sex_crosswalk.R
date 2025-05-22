# CE- Sex-crosswalking
# - MR-BRT Model Fit
# - Adjust Data and Evaluate Plots


### ----------------------- Set-Up ------------------------------

rm(list=ls())
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')



library(crosswalk, lib.loc = FILEPATH)
library(dplyr)
library(data.table)
library(openxlsx)
library(stringr)
library(tidyr)
library(boot)
library(ggplot2)

bundle_version_id <- ADDRESS

# Standard
bundle_version_df <- get_bundle_version(ADDRESS)

all_data<-bundle_version_df

all_data<- subset(all_data, all_data$is_outlier!=1)



all_data[, sample_size := effective_sample_size]
all_data[is.na(mean), mean := cases/sample_size]
all_data[is.na(cases), cases := mean*sample_size]



all_data<- subset(all_data, measure == "incidence")

################################################
## IMPORTANT - droping clinical data to append after sex and age split
#################################################
all_data<- subset(all_data,  source_type != "Facility - inpatient")

all_data <- mutate(all_data, mean = if_else(all_data$nid== ADDRESS, cases / sample_size, mean))

all_data <- mutate(all_data, lower = if_else(nid== ADDRESS, mean - (1.96*standard_error), lower))
all_data <- mutate(all_data, upper = if_else(nid== ADDRESS, mean + (1.96*standard_error), upper))

data <- as.data.table(all_data)

#' [Direct Matches]
data_mf<- data[sex != "Both"]

data_mf[, ]

data_mf[, match := str_c(age_start, age_end, location_name, year_start, year_end, site_memo, clinical_data_type, case_name)]
dm      <- data_mf[, .N, by = c("age_start", "age_end", "location_name", "year_start", "year_end", "site_memo", "clinical_data_type", "case_name" )][N == 2]

matches <- dm[, str_c(age_start, age_end, location_name, year_start, year_end, site_memo, clinical_data_type, case_name)]
matches <- matches[!(is.na(matches))]

data_ss_subset <- data_mf[match %in% matches]

setorder(data_ss_subset, year_start, year_end, age_start, age_end, location_name, site_memo, clinical_data_type, case_name)

data_ss_m <- data_ss_subset[sex == "Male", ]
data_ss_f <- data_ss_subset[sex == "Female",]

# validate -- if all true then ready to clean and prep for mrbrt

all(data_ss_m[, .(year_start, year_end, age_start, age_end, location_name, site_memo,  clinical_data_type, case_name)] == data_ss_f[, .(year_start, year_end, age_start, age_end, location_name, site_memo,  clinical_data_type, case_name)])


#merge together male and female data from same population-
data_merged<-merge(data_ss_m,data_ss_f,by=c("nid","location_name", "location_id","year_start","age_start","age_end","site_memo", "clinical_data_type", "case_name"))
data_merged <- data_merged[cases.x != 0]
data_merged <- data_merged[cases.y != 0]

###keep mean, se, location_id, year_start, nid, site_memo
#note X and mean_1=male
data_merged<-data_merged %>% 
  select(nid, location_id, year_start, age_start, age_end, cases.x, cases.y, effective_sample_size.x, effective_sample_size.y, sex.x, sex.y, site_memo, mean.x, mean.y, standard_error.x, standard_error.y) %>%
  rename(mean_1 = mean.x,mean_2 = mean.y, se_1=standard_error.x, se_2=standard_error.y, cases_1=cases.x, cases_2=cases.y, sample_size_1=effective_sample_size.x, sample_size_2=effective_sample_size.y, altvar=sex.x, refvar=sex.y)

data_merged$cases_3<-data_merged$cases_1+data_merged$cases_2
data_merged$eff_ss3<-data_merged$sample_size_1+data_merged$sample_size_2
data_merged$mean_3<-data_merged$cases_3/data_merged$eff_ss3

data_merged$se_3<-sqrt(((data_merged$mean_3*(1-data_merged$mean_3))/data_merged$eff_ss3))
data_merged$altvar<-"Both"

#create an index of matches
setDT(data_merged)
data_merged[, id := .I]


#compare males:females by year
ggplot(data_merged, aes(x=mean_1, y=mean_2, color=year_start)) +
  geom_point()


### ----------------------- Model Fit + Params ------------------------------

dat_diff <- as.data.frame(cbind(
  delta_transform(
    mean = data_merged$mean_3, 
    sd = data_merged$se_3,
    transformation = "linear_to_log" ),
  delta_transform(
    mean = data_merged$mean_2, 
    sd = data_merged$se_2,
    transformation = "linear_to_log")
))

#recode names 
names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")

#note log_diff or logit_diff
data_merged[, c("log_diff", "log_diff_se")] <- calculate_diff(
  df = dat_diff, 
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref" )

######## prep for running in crosswalk package ##############################

df1 <- CWData(
  df = data_merged,          # dataset for metaregression
  obs = "log_diff",       # column name for the observation mean
  obs_se = "log_diff_se", # column name for the observation standard error
  alt_dorms = "altvar",     # column name of the variable indicating the alternative method
  ref_dorms = "refvar",     # column name of the variable indicating the reference method
  covs = list("year_start"),     # names of columns to be used as covariates later- YEAR here is used to test code functionality
  study_id = "nid",    # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

#Here we run a model for intercept, just to get the sex-ratio  
fit1 <- CWModel(
  cwdata = df1,            # object returned by `CWData()`
  obs_type = "diff_log", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept")),
  gold_dorm = "Female")   # the level of `alt_dorms` that indicates it's the gold standard
#)

#view the results--fixed effects 
fit1$fixed_vars
fit1$gamma
fit1$beta_sd
repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = fit1, 
  cwdata = df1,
  continuous_variables = list("year_start"),
  obs_method = 'Both',
  plot_note = 'ADDRESS', 
  plots_dir = 'FILEPATH', 
  file_name = "FILEPATH",
  write_file = TRUE
)

#Calculate 95% UI

df_result <- fit1$create_result_df()
gamma_val <- df_result$gamma[1]
ci_calc <- subset(df_result, select = c(dorms, beta, beta_sd))
ci_calc <- mutate(ci_calc, gamma = gamma_val)
ci_calc <- mutate(ci_calc, ci_lower = beta - 1.96*sqrt((beta_sd^2) + gamma)) 
ci_calc <- mutate(ci_calc, ci_upper = beta + 1.96*sqrt((beta_sd^2) + gamma))

### ----------------------- Apply Fit to Adjust Data ------------------------------


###pull in original data 
#pull sbuset to Both sex data points only 
data_both <- data[sex == "Both",]

data_both<-data_both[mean>0,]

data_both$unadj_mean<-data_both$mean



#convert both to female
data_both[, c("prev2", "prev2_se", "diff", "diff_se", "a")] <- adjust_orig_vals(
  fit_object = fit1,       # result of CWModel()
  df = data_both,            # original data with obs to be adjusted
  orig_dorms = "sex", # name of column with (all) def/method levels
  orig_vals_mean = "mean",  # original mean
  orig_vals_se = "standard_error"  # standard error of original mean
)

ggplot(data_both, aes(x=mean, y=prev2, color=sex)) +
  geom_point()

#list of years in dataset 
years<-unique(data_both$year_start)
#locations
locs<-unique(data_both$location_id)

#for now - using all ages
malepop<-get_population(release_id=ADDRESS, location_id=locs, year_id=years, sex_id = 1, age_group_id=22)
fempop<-get_population(release_id=ADDRESS, location_id=locs, year_id=years, sex_id = 2, age_group_id=22)

#merge populations

malepop$year_start<-malepop$year_id
fempop$year_start<-fempop$year_id
data_both1<-merge(data_both,malepop,by=c("location_id","year_start"))

#rename 
data_both1<-data_both1 %>% 
  rename(male_pop = population)

data_both1<-merge(data_both1,fempop,by=c("location_id","year_start"))
data_both1<-data_both1 %>% 
  rename(fem_pop = population)




data_both1<-select(data_both1,-c(sex_id.x,sex_id.y,year_id.x,year_id.y, run_id.y,run_id.x, age_group_id.y,age_group_id.x))

data_both1 <- data.table(data_both1)
data_both1[,tot_pop := fem_pop+male_pop]
data_both1[,fem_per := fem_pop/tot_pop]

##calculate male and female denominator
data_both1[,fem_ss := (fem_per*sample_size)]
data_both1[,male_ss := sample_size-(fem_per*sample_size)]

data_both1[,fem_cases := prev2*fem_ss]
data_both1[,male_cases := cases-fem_cases]
data_both1[,mean_male := male_cases/male_ss]

data_both1[measure == "incidence", se_male := sqrt((mean_male*(1-mean_male)/male_ss)) + prev2_se]

##split data and append male and female records
males_data <-data_both1 %>% 
  select(-mean, -cases, -standard_error, -unadj_mean, -prev2, -prev2_se, -diff, -diff_se, -male_pop, -fem_pop, -tot_pop, -fem_per,-fem_ss,  -fem_cases, -a, -effective_sample_size) %>%
  rename(mean = mean_male, cases = male_cases, standard_error=se_male, effective_sample_size=male_ss)

males_data$sex<-"Male"


females_data<-data_both1 %>% 
  select(-mean, -cases, -standard_error, -unadj_mean, -diff, -diff_se, -male_pop, -fem_pop, -tot_pop, -fem_per, -mean_male, -male_cases, -se_male, -male_ss, -a, -effective_sample_size) %>%
  rename(mean = prev2, cases = fem_cases, standard_error=prev2_se, effective_sample_size=fem_ss)

females_data$sex<-"Female"

#append together

split_sex<-rbind(males_data,females_data)

split_sex$note_modeler<-"sexsplit"

# apply to calculate upper and lower -> mean +1.96*(se)
split_sex[, upper := mean + 1.96*(standard_error)]
split_sex[, lower := mean - 1.96*(standard_error)]


###########Agregating sex specific data
data_orig_mf<- data[sex != "Both"]

data_orig_mf$note_modeler<- ""

#append
data_split_app<-rbind(data_orig_mf,split_sex)
 
data_split_app[, crosswalk_parent_seq := seq]
data_split_app[,seq:=NULL]
data_split_app[, seq := ""]

#output csv 
write.csv(data_split_app,"FILEPATH")

#############################################################
###########################
#############                       END
####
####################################################


