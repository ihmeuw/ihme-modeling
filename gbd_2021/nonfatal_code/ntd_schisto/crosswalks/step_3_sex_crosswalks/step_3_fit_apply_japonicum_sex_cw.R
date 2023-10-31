# Schisto - Japonicum Crosswalk Matching 
# - Subsetting Bundle \ 
# - Selecting Diagnostic Criteria
# - MR-BRT Model Fit
# - Adjust Data and Evaluate Plots

### ----------------------- Set-Up ------------------------------

rm(list=ls())
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_population.R")
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_crosswalk_version.R')



library(crosswalk, lib.loc = "FILEPATH/packages/")
library(dplyr)
library(data.table)
library(openxlsx)
library(stringr)
library(tidyr)
library(boot)
library(ggplot2)

### ----------------------- Consolidate Data ------------------------------
#using cleaned up data
all_data <- read.csv("FILEPATH")

data <- as.data.table(all_data)
#' [Direct Matches]

data <- data[sex != "Both"]
# sex specific rows

data <- data[cases != 0]
data<- subset(data, sample_size!=cases)
#
dat_original_japon<- subset(data, case_name=="S japonicum")


dat_original_japon$case_diagnostics <- as.character(dat_original_japon$case_diagnostics)

dat_original_japon<- subset(dat_original_japon, dat_original_japon$case_diagnostics != "NA")

dat_original_japon$num_samples <- as.double(dat_original_japon$num_samples)



unique(dat_original_japon$case_diagnostics)


length(which(dat_original_japon$case_diagnostics == "kk1")) 
length(which(dat_original_japon$case_diagnostics == "kk2")) 
length(which(dat_original_japon$case_diagnostics == "kk3")) 
length(which(dat_original_japon$case_diagnostics == "iha")) 
length(which(dat_original_japon$case_diagnostics == "hatch")) 
length(which(dat_original_japon$case_diagnostics == "elisa")) 

dat_original_japon[, ]

dat_original_japon[, match := str_c(age_start, age_end, location_name, year_start, year_end, site_memo, case_diagnostics)]


dm      <- dat_original_japon[, .N, by = c("age_start", "age_end", "location_name", "year_start", "year_end", "site_memo", "case_diagnostics", "group_review")][N > 1]
dm      <- dm[N == 2]

matches <- dm[, str_c(age_start, age_end, location_name, year_start, year_end, site_memo, case_diagnostics)]
matches <- matches[!(is.na(matches))]
# subset to the matches 

data_ss_subset <- dat_original_japon[match %in% matches]

# sort

setorder(data_ss_subset, year_start, year_end, age_start, age_end, location_name, site_memo, case_diagnostics)

# correct names

data_ss_m <- data_ss_subset[sex == "Male", ]
data_ss_f <- data_ss_subset[sex == "Female",]

# validate -- 

all(data_ss_m[, .(year_start, year_end, age_start, age_end, location_name, site_memo, case_diagnostics)] == data_ss_f[, .(year_start, year_end, age_start, age_end, location_name, site_memo, case_diagnostics)])

#merge together male and female data from same population-
data_merged<-merge(data_ss_m,data_ss_f,by=c("nid","location_name", "location_id","year_start","age_start","age_end","site_memo", "case_diagnostics"))
data_merged <- data_merged[cases.x != 0]
data_merged <- data_merged[cases.y != 0]


###keep mean, se, location_id, year_start, nid, site_memo
#note X and mean_1=male
data_merged<-data_merged %>% 
  select(nid, location_id, year_start, age_start, age_end, case_diagnostics, cases.x, cases.y, effective_sample_size.x, effective_sample_size.y, sex.x, sex.y, site_memo, mean.x, mean.y, standard_error.x, standard_error.y) %>%
  rename(mean_1 = mean.x,mean_2 = mean.y, se_1=standard_error.x, se_2=standard_error.y, cases_1=cases.x, cases_2=cases.y, sample_size_1=effective_sample_size.x, sample_size_2=effective_sample_size.y, altvar=sex.x, refvar=sex.y)

data_merged$cases_3<-data_merged$cases_1+data_merged$cases_2
data_merged$eff_ss3<-data_merged$sample_size_1+data_merged$sample_size_2
data_merged$mean_3<-data_merged$cases_3/data_merged$eff_ss3
#
data_merged$se_3<-sqrt(((data_merged$mean_3*(1-data_merged$mean_3))/data_merged$eff_ss3))
data_merged$altvar<-"Both"

#create an index of matches
setDT(data_merged)
data_merged[, id := .I]


#compare males:females by year
ggplot(data_merged, aes(x=mean_1, y=mean_2, color= as.factor(age_start))) +
  geom_point() +
  ggtitle("Japonicum - Prevalence Male vs Females") +
  xlab("Males") + ylab("Females") +
  geom_smooth(method = "lm")


### ----------------------- Model Fit + Params ------------------------------
#use delta_transform() and calculate_diff() to get log(alt)-lot(ref) or logit(alt)-logit(ref)
#note setting females as alternative
#mean_3=alternative, mean_2=reference

dat_diff <- as.data.frame(cbind(
  delta_transform(
    mean = data_merged$mean_3, 
    sd = data_merged$se_3,
    transformation = "linear_to_logit" ),
  delta_transform(
    mean = data_merged$mean_2, 
    sd = data_merged$se_2,
    transformation = "linear_to_logit")
))

#recode names 
names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")

#note log_diff or logit_diff
data_merged[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = dat_diff, 
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref" )

######## prep for running in crosswalk package ##############################
#NID  must be numeric for the code to work
#use NID for study_id 
unique(data_merged$nid)

#dataset for meta-regression (e.g., dataset with the M:F comparisons)
df1 <- CWData(
  df = data_merged,          # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "altvar",     # column name of the variable indicating the alternative method
  ref_dorms = "refvar",     # column name of the variable indicating the reference method
  covs = list("year_start"),     # names of columns to be used as covariates later- YEAR here is used to test code functionality
  study_id = "nid",    # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

#Here we run a model for intercept, just to get the sex-ratio  
fit1 <- CWModel(
  cwdata = df1,            # object returned by `CWData()`
  obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept")),
  #CovModel(cov_name = "year_start")),
  #CovModel(cov_name = "year_start", spline = XSpline(knots = c(0,1,2,3,4), degree = 3L, l_linear = TRUE, r_linear = TRUE), spline_monotonicity = "increasing")),
  gold_dorm = "Female")   # the level of `alt_dorms` that indicates it's the gold standard
# this will be useful when we can have multiple "reference" groups in NMA
#)

#view the results--fixed effects 
fit1$fixed_vars
fit1$gamma
fit1$beta_sd

#######
#funnel plot

repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = fit1, 
  cwdata = df1,
  continuous_variables = list("year_start"),
  obs_method = 'Both',
  plot_note = 'Funnel plot Japonicum - sex split', 
  plots_dir = 'FILEPATH', 
  file_name = "funnel_plot_mansoni_sex_split_32621",
  write_file = TRUE
)

df_result <- fit1$create_result_df()

### ----------------------- Apply Fit to Adjust Data ------------------------------



bundle_version_id <- ADDRESS

# Standard
bundle_version_df <- get_bundle_version(bundle_version_id,fetch="all")
data_agg <- bundle_version_df
data_agg_japon<- subset(data_agg, case_name=="S japonicum")

data_agg_japon <- data.table(data_agg_japon)
#pull subset to Both sex data points only 
data_agg_japon <- data_agg_japon[sex == "Both",]
#we don't model data where mean = 0
data_agg_japon<-data_agg_japon[mean>0,]
#create variable for unadjusted mean for comparison if needed
data_agg_japon$unadj_mean<-data_agg_japon$mean
#### apply sex split

#convert both to female
data_agg_japon[, c("prev2", "prev2_se", "diff", "diff_se", "a")] <- adjust_orig_vals(
  fit_object = fit1,       # result of CWModel()
  df = data_agg_japon,            # original data with obs to be adjusted
  orig_dorms = "sex", # name of column with (all) def/method levels
  orig_vals_mean = "mean",  # original mean
  orig_vals_se = "standard_error"  # standard error of original mean
)


ggplot(data_agg_japon, aes(x=mean, y=prev2, color=sex)) +
  geom_point()




#list of years in dataset 
years<-unique(data_agg_japon$year_start)
#locations
locs<-unique(data_agg_japon$location_id)


#using all ages
malepop<-get_population(gbd_round_id=ADDRESS, decomp_step="ADDRESS",location_id=locs, year_id=years, sex_id = 1, age_group_id=22)
fempop<-get_population(gbd_round_id=ADDRESS, decomp_step="ADDRESS",location_id=locs, year_id=years, sex_id = 2, age_group_id=22)

#merge populations

malepop$year_start<-malepop$year_id
fempop$year_start<-fempop$year_id
data_agg_japon_b<-merge(data_agg_japon,malepop,by=c("location_id","year_start"))

#rename 
data_agg_japon_b<-data_agg_japon_b %>% 
  rename(male_pop = population)

data_agg_japon_b<-merge(data_agg_japon_b,fempop,by=c("location_id","year_start"))
data_agg_japon_b<-data_agg_japon_b %>% 
  rename(fem_pop = population)

#drop variables for cleap up
data_agg_japon_b<-select(data_agg_japon_b,-c(sex_id.x,sex_id.y,year_id.x,year_id.y, run_id.y,run_id.x, age_group_id.y,age_group_id.x))

#calculate %female
data_agg_japon_b <- data.table(data_agg_japon_b)
data_agg_japon_b[,tot_pop := fem_pop+male_pop]
data_agg_japon_b[,fem_per := fem_pop/tot_pop]

##calculate male and female denominator
data_agg_japon_b[,fem_ss := (fem_per*sample_size)]
data_agg_japon_b[,male_ss := sample_size-(fem_per*sample_size)]

####take adjusted prevalence x female population

data_agg_japon_b[,fem_cases := prev2*fem_ss]
data_agg_japon_b[,male_cases := cases-fem_cases]
data_agg_japon_b[,mean_male := male_cases/male_ss]

#recalculate SE of males - multiply by the SE from the MR-BRT model

data_agg_japon_b[measure == "prevalence", se_male := sqrt((mean_male*(1-mean_male)/male_ss)) + prev2_se]

#
data_agg_japon_b[measure == "prevalence", se_male2 := sqrt(  ((mean_male*(1-mean_male)/male_ss) + (1.96^2) / (4*(male_ss)^2) )  / ((1 + (1.96^2/male_ss))^2)) / (sqrt(male_ss))]

data_agg_japon_b <- mutate(data_agg_japon_b, se_male3 = if_else(male_ss >20, se_male, se_male2))

data_agg_japon_b  <- data.table(data_agg_japon_b)


##split data and append male and female records
males_japon <-data_agg_japon_b %>% 
  select(-mean, -cases, -standard_error, -unadj_mean, -prev2, -prev2_se, -diff, -diff_se, -male_pop, -fem_pop, -tot_pop, -fem_per,-fem_ss,  -fem_cases, -a, -se_male, -se_male2, -effective_sample_size) %>%
  rename(mean = mean_male, cases = male_cases, standard_error=se_male3, effective_sample_size=male_ss)

males_japon$sex<-"Male"

females_japon<-data_agg_japon_b %>% 
  select(-mean, -cases, -standard_error, -unadj_mean, -diff, -diff_se, -male_pop, -fem_pop, -tot_pop, -fem_per, -mean_male, -male_cases, -se_male, -male_ss, -a, -se_male2, -se_male3, -effective_sample_size) %>%
  rename(mean = prev2, cases = fem_cases, standard_error=prev2_se, effective_sample_size=fem_ss)

females_japon$sex<-"Female"

#append together

split_japon<-rbind(males_japon,females_japon)

split_japon$note_modeler<-"sexsplit"

# apply to calculate upper and lower -> mean +1.96*(se)
split_japon[, upper := mean + 1.96*(standard_error)]
split_japon[, lower := mean - 1.96*(standard_error)]


#append data original F M
#pull sbuset M & F 

data_agg_japon_o<- subset(data_agg, case_name=="S japonicum")

data_agg_japon_o <- data.table(data_agg_japon_o)

data_agg_japon_o <- data_agg_japon_o[sex != "Both",]
data_agg_japon_o$note_modeler<- ""

#append
split_japon_b<-rbind(split_japon,data_agg_japon_o)

# crosswalk_parent_seq = seq
# seq = ""  
split_japon_b[, crosswalk_parent_seq := seq]
split_japon_b[,seq:=NULL]
split_japon_b[, seq := ""]

#output csv 
write.csv(split_japon_b,"FILEPATH")


#######################################################
#       APPEND 3 species
# ###    here we will rbind the three species files
##################

final_sex_split_data <- rbind(split_mansoni_b, split_hema_b, split_japon_b)
#apply ceiling of 1 to mean (no cases with mean >1)
test <- subset(final_sex_split_data, mean >=1)

final_sex_split_data$upper[final_sex_split_data$upper>1] <- 1

#lower bound 0 to lower
final_sex_split_data$lower[final_sex_split_data$lower<0] <- 0


final_sex_split_data[, c("group", "group_review", "specificity") := NA]


summary(final_sex_split_data$unit_value_as_published)

final_sex_split_data[is.na(unit_value_as_published), note_cleaner := "ADDRESS"]
final_sex_split_data[is.na(unit_value_as_published), cleaner_id := "ADDRESS"]
final_sex_split_data[is.na(unit_value_as_published), unit_value_as_published := 1]


#correcting sample size
final_sex_split_data <- mutate(final_sex_split_data, sample_size = effective_sample_size)


#saving it as a flat file
openxlsx::write.xlsx(final_sex_split_data, sheetName = "extraction", file = paste0("FILEPATH"))

                           


#####
#########
########    END
#####
####################################

