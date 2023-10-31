#############################################
### Script for out-of-dismod crosswalks
### Using MR-BRT (2019 version)
### Authors: USERNAME
### Last update: 2019-June-28
#############################################


## NOTE: Needs to be run with access to the online drives (e.g. cluster Rstudio session)

##############################################################
## PART I: PREPARING FOR CROSSWALK
##############################################################
## Need to insert code that takes raw extraction data and prepares 2 files:
## 1. cleaned/prepped data for crosswalking
## 2. reduced data for informing crosswalk (ref, alt, ratios, etc.)
##############################################################


# Prepare for the crosswalk you are performing:
file_location <- "FILEPATH/mrbrt_working/nash/"

## **IF** no previous crosswalk needed prior to sex splitting, skip to PART III:

## Settings for sexsplitting (PART V):
## uncomment the one to run:
#splitToPerform <- "liver_A"
#splitToPerform <- "liver_B"
#splitToPerform <- "liver_C"
splitToPerform <- "liver_N"
#splitToPerform <- "liver_O"

## read in the raw data
## Copy the file that is the current file in the database, rename and move to the working folder

file_location_alcohol <- "FILEPATH/neo_liver_alcohol/323/03_review/01_download/"
file_location_hepb <- "FILEPATH/neo_liver_hepb/321/03_review/01_download/"
file_location_hepc <- "FILEPATH/neo_liver_hepc/322/03_review/01_download/"
file_location_nash <- "FILEPATH/neo_liver_nash/3143/03_review/01_download/"
file_location_other <- "FILEPATH/neo_liver_other/324/03_review/01_download/"

## change these files when new uploads needed
alcohol <- read_excel(paste0(file_location_alcohol, "request_205715.xlsx"))
hepb <- read_excel(paste0(file_location_hepb, "request_205943.xlsx"))
hepc <- read_excel(paste0(file_location_hepc, "request_205946.xlsx"))
nash <- read_excel(paste0(file_location_nash, "request_205952.xlsx"))
other <- read_excel(paste0(file_location_other, "request_205949.xlsx"))

fwrite(alcohol, paste0(file_location, "raw_extraction_liver_A.csv"))
fwrite(hepb, paste0(file_location, "raw_extraction_liver_B.csv"))
fwrite(hepc, paste0(file_location, "raw_extraction_liver_C.csv"))
fwrite(nash, paste0(file_location, "raw_extraction_liver_N.csv"))
fwrite(other, paste0(file_location, "raw_extraction_liver_O.csv"))

##########################################################
## PART II: PERFORMING FIRST CROSSWALK
##########################################################
#### Code from USERNAME for MR-BRT ##################
##########################################################

# example_mrbrt_logit_crosswalk.R
#
# USERNAME
# June 2019
#
# Example of how to do a crosswalk when the outcome variable is
# bounded by zero and 1 (logit crosswalk)
#
# This script works for models with and without covariates
#
# The data come from estimates of prevalence/incidence of knee osteoarthritis
#   when using two different sets of diagnostic criteria
#
# See this link for a description of the method:
#   ADDRESS
#

library(dplyr)
library(data.table)
library(metafor, lib.loc = "FILEPATH/R_libraries/")
library(msm, lib.loc = "FILEPATH/R_libraries/")
library(readxl)

library(msm, lib.loc = "FILEPATH/R_libraries/")

#rm(list = ls())

#####
# USER SETTINGS

# original extracted data
# -- read in data frame
# -- specify how to identify reference studies
# -- identify mean, standard error and covariate variables

dat_original <- read_excel(paste0(file_location, "nash_raw_data.xlsx")) %>%
  mutate(age_mid = (age_start + age_end) / 2)
#dat_original <- read_excel(paste0(file_location, "nash_raw_data.xlsx"))
reference_var <- "reference"
reference_value <- 1
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- c() # if no covariates, give c() instead of c("age_mid")

# data for meta-regression model
# -- read in data frame
# -- identify variables for mean and SE of the alternative and reference
#    NOTE: the 'prev' prefix refers to prevalence,
#          although any parameter bounded by 0 and 1 can be modeled

dat_metareg <- read.csv(paste0(file_location, "v2_nash_crosswalk_data.csv")) %>%
  mutate(age_mid = (age_start + age_end) / 2)
prev_ref_var <- "ES_ref"
se_prev_ref_var <- "ref_se"
prev_alt_var <- "ES_alt"
se_prev_alt_var <- "alt_se"

#####

# create datasets with standardized variable names

orig_vars <- c(mean_var, se_var, reference_var, cov_names)
metareg_vars <- c(prev_ref_var, se_prev_ref_var, prev_alt_var, se_prev_alt_var, cov_names)

tmp_orig <- as.data.frame(dat_original) %>%
  .[, orig_vars] %>%
  setnames(orig_vars, c("mean", "se", "ref", cov_names)) %>%
  mutate(ref = if_else(ref == reference_value, 1, 0, 0))

tmp_metareg <- as.data.frame(dat_metareg) %>%
  .[, metareg_vars] %>%
  setnames(metareg_vars, c("prev_ref", "se_prev_ref", "prev_alt", "se_prev_alt", cov_names))

# logit transform the original data
# -- SEs transformed using the delta method
tmp_orig$mean_logit <- log(tmp_orig$mean / (1-tmp_orig$mean))
tmp_orig$se_logit <- sapply(1:nrow(tmp_orig), function(i) {
  mean_i <- tmp_orig[i, "mean"]
  se_i <- tmp_orig[i, "se"]
  deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
})

# logit transform the meta-regression data
# -- alternative
tmp_metareg$prev_logit_alt <- log(tmp_metareg$prev_alt / (1-tmp_metareg$prev_alt))
tmp_metareg$se_prev_logit_alt <- sapply(1:nrow(tmp_metareg), function(i) {
  prev_i <- tmp_metareg[i, "prev_alt"]
  prev_se_i <- tmp_metareg[i, "se_prev_alt"]
  deltamethod(~log(x1/(1-x1)), prev_i, prev_se_i^2)
})

# -- reference
tmp_metareg$prev_logit_ref <- log(tmp_metareg$prev_ref / (1-tmp_metareg$prev_ref))
tmp_metareg$se_prev_logit_ref <- sapply(1:nrow(tmp_metareg), function(i) {
  prev_i <- tmp_metareg[i, "prev_ref"]
  prev_se_i <- tmp_metareg[i, "se_prev_ref"]
  deltamethod(~log(x1/(1-x1)), prev_i, prev_se_i^2)
})

tmp_metareg$diff_logit <- tmp_metareg$prev_logit_alt - tmp_metareg$prev_logit_ref
tmp_metareg$se_diff_logit <- sqrt(tmp_metareg$se_prev_logit_alt^2 + tmp_metareg$se_prev_logit_ref^2)



# fit the MR-BRT model
#repo_dir <- "FILEPATH/run_mr_brt/"
#source(paste0(repo_dir, "run_mr_brt_function.R"))
#source(paste0(repo_dir, "cov_info_function.R"))
#source(paste0(repo_dir, "check_for_outputs_function.R"))
#source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
#source(paste0(repo_dir, "predict_mr_brt_function.R"))
#source(paste0(repo_dir, "check_for_preds_function.R"))
#source(paste0(repo_dir, "load_mr_brt_preds_function.R"))

# fit the MR-BRT model
source("FILEPATH/run_mr_brt/mr_brt_functions.R")

covs1 <- list()
for (nm in cov_names) covs1 <- append(covs1, list(cov_info(nm, "X")))

if (length(covs1) > 0) {

  fit1 <- run_mr_brt(
    output_dir = file_location,   #paste0("FILEPATH"), # user home directory
    model_label = "crosswalk_ex4",
    data = tmp_metareg,
    mean_var = "diff_logit",
    se_var = "se_diff_logit",
    covs = covs1,
    overwrite_previous = TRUE,
    method = "trim_maxL",
    trim_pct = 0.1
  )

} else {
  fit1 <- run_mr_brt(
    output_dir = file_location,    #paste0("FILEPATH"), # user home directory
    model_label = "crosswalk_ex4",
    data = tmp_metareg,
    mean_var = "diff_logit",
    se_var = "se_diff_logit",
    overwrite_previous = TRUE,
    method = "trim_maxL",
    trim_pct = 0.1
  )
}

### plot the model fit
plot_mr_brt(fit1)

######## TEST RUNNING UNIVARIATELY, NOT ON RATIOS (no grouping variable)
library(msm, lib.loc = "FILEPATH")
test <- tmp_orig[tmp_orig$mean!=0,]
fit1 <- run_mr_brt(
  output_dir =  file_location,   #paste0("FILEPATH"), # user home directory
  model_label = "crosswalk_univarTEST",
  data = test,
  mean_var = "mean_log",
  se_var = "se_log",
  covs = list(cov_info("location_id", "X")),
  overwrite_previous = TRUE, ## start putting in options:
  method = "trim_maxL",
  trim_pct = 0.1
)
plot_mr_brt(fit1)
########


preds <- predict_mr_brt(fit1, newdata = tmp_orig)$model_summaries
tmp_orig$adj_logit <- preds$Y_mean
tmp_orig$se_adj_logit <- (preds$Y_mean_hi - preds$Y_mean_lo) / 3.92

tmp_orig2 <- tmp_orig %>%
  mutate(
    prev_logit_adjusted_tmp = mean_logit - adj_logit,
    se_prev_logit_adjusted_tmp = sqrt(se_logit^2 + se_adj_logit^2),
    prev_logit_adjusted = ifelse(ref == 1, mean_logit, prev_logit_adjusted_tmp),
    se_prev_logit_adjusted = ifelse(ref == 1, se_logit, se_prev_logit_adjusted_tmp),
    prev_adjusted = exp(prev_logit_adjusted)/(1+exp(prev_logit_adjusted)),
    lo_logit_adjusted = prev_logit_adjusted - 1.96 * se_prev_logit_adjusted,
    hi_logit_adjusted = prev_logit_adjusted + 1.96 * se_prev_logit_adjusted,
    lo_adjusted = exp(lo_logit_adjusted)/(1+exp(lo_logit_adjusted)),
    hi_adjusted = exp(hi_logit_adjusted)/(1+exp(hi_logit_adjusted))
  )


tmp_orig2$se_adjusted <- sapply(1:nrow(tmp_orig2), function(i) {
  mean_i <- tmp_orig2[i, "prev_logit_adjusted"]
  mean_se_i <- tmp_orig2[i, "se_prev_logit_adjusted"]
  deltamethod(~exp(x1)/(1+exp(x1)), mean_i, mean_se_i^2)
})


# 'final_data' is the original extracted data plus the new variables
final_data <- cbind(
  dat_original,
  as.data.frame(tmp_orig2)[, c("ref", "prev_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
)


# output crosswalked version to files
#fwrite(final_data, paste0(file_location, "nash_crosswalked_06282019.csv"))

##############################################################
## PART III: FORMATTING CROSSWALK OUTPUT
##############################################################
## Need to insert code that takes output data and prepares 3 files:
## 1. combines the N/U data into a single proportion
##############################################################

## find the duplicates (in order to combine N/U groups)

# make exploratory dataset
test <- final_data

setnames(test, "prev_adjusted", "mean_adjusted")

# get T/F flag for whether there are multiple rows for a given combination of age/sex/year/location/nid
test$dup <- duplicated(final_data[, c("sex", "location_id", "year_start", "age_start", "nid")]) |
  duplicated(final_data[, c("sex", "location_id", "year_start", "age_start", "nid")], fromLast=TRUE)

# subset to a dataset of observations with duplicates
test2 <- subset(test, dup==TRUE)


# calculate the sum of the cases across N/U
test3 <- aggregate(cases ~ year_start + sex + nid + location_id + age_start + input_type, test2, sum)
# rename to total cases
colnames(test3) <- gsub("cases", "TotCases", colnames(test3))
# add on the total cases
temp2 <- merge(test2, test3)

# calculate the sum of the proportions across N/U
test4 <- aggregate(mean_adjusted ~ year_start + sex + nid + location_id + age_start + input_type, test2, sum)
# rename to total mean
colnames(test4) <- gsub("mean_adjusted", "TotMean", colnames(test4))
# add on the total mean
temp2 <- merge(temp2, test4)

# calculate the combined SE
test5 <- aggregate(se_adjusted ~ year_start + sex + nid + location_id + age_start + input_type, test2, sum)
# rename to total mean
colnames(test5) <- gsub("se_adjusted", "TotSE", colnames(test5))
# add on the total mean
temp2 <- merge(temp2, test5)

# calculate the combined lower
test6 <- aggregate(lo_adjusted ~ year_start + sex + nid + location_id + age_start + input_type, test2, sum)
# rename to total mean
colnames(test6) <- gsub("lo_adjusted", "Tot_lo", colnames(test6))
# add on the total mean
temp2 <- merge(temp2, test6)

# calculate the combined upper
test7 <- aggregate(hi_adjusted ~ year_start + sex + nid + location_id + age_start + input_type, test2, sum)
# rename to total mean
colnames(test7) <- gsub("hi_adjusted", "Tot_hi", colnames(test7))
# add on the total mean
temp2 <- merge(temp2, test7)


##
# subset to a dataset of observations without duplicates
temp_nodups <- subset(test, dup==FALSE)

# create the same columns for merging
temp_nodups$TotCases <- temp_nodups$cases
temp_nodups$TotMean <- temp_nodups$mean_adjusted
temp_nodups$TotSE <- temp_nodups$se_adjusted
temp_nodups$Tot_lo <- temp_nodups$lo_adjusted
temp_nodups$Tot_hi <- temp_nodups$hi_adjusted

# combine the duplicated and non-duplicated datasets
temp_combined <- rbind(temp2, temp_nodups)


# get unique dataset (only keep single row, will need to overwrite with the new combined values)
temp_combined$dup2 <- duplicated(temp_combined[, c("year_start", "sex", "nid", "location_id", "age_start", "input_type")])
temp_combined2 <- subset(temp_combined, dup2==FALSE)
cols_to_remove <- c("temp", "cv_nash_definition", "lower", "upper", "cases", "ref", "mean", "standard_error",
                    "mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted", "dup", "dup2")
temp_combined3 <- temp_combined2[, ! names(temp_combined2) %in% cols_to_remove, drop=FALSE]
# rename column names for updated values
temp_combined3$case_name <- "N"
colnames(temp_combined3) <- gsub("TotCases", "cases", colnames(temp_combined3))
colnames(temp_combined3) <- gsub("TotMean", "mean", colnames(temp_combined3))
colnames(temp_combined3) <- gsub("TotSE", "standard_error", colnames(temp_combined3))
colnames(temp_combined3) <- gsub("Tot_lo", "lower", colnames(temp_combined3))
colnames(temp_combined3) <- gsub("Tot_hi", "upper", colnames(temp_combined3))

## crosswalked NASH data
nash_crosswalked <- temp_combined3

### ouptut crosswalked data
fwrite(nash_crosswalked, paste0(file_location, "nash_crosswalked_combined_06282019.csv"))

##############################################################
## PART IV: PREPARING DATA FOR SEX-SPLITTING CROSSWALK
## 2. prepares a datset for crosswalking sex
## 3. reduced data for informing crosswalk (ref, alt, ratios, etc.)
##############################################################

## rename file_location and splitToPerform to relevant crosswalk
file_location <- "FILEPATH/mrbrt_working/nash/"
splitToPerform <- "liver_O"

##############

## **IF** NASH or another dataset that needs two crosswalks, use the output from the last section
rawSexSplit <- nash_crosswalked

## **IF** no previous crosswalk needed, read in the raw data
## Copy the file that is the current file in the database to the working folder, and put the name in below
## (here, this file came from "FILEPATH/request_205715.xlsx")
##rawSexSplit <- read_excel(paste0(file_location, "request_205715.xlsx"))
rawSexSplit <- fread(paste0(file_location, "raw_extraction_", splitToPerform, ".csv"))


###############

## set reference (Male) and alternate (Both sexes) flag values
## NOTE: females also get a reference value (accounted for later)
rawSexSplit$reference <- ifelse(rawSexSplit$sex=="Male", 1, 0)

## remove problematic study in Japan (oldest age group just 0 for everything)
rawSexSplit <- rawSexSplit[rawSexSplit$nid != 144366,]
#rawSexSplit <- rawSexSplit[rawSexSplit$age_start!=90,]
## remove problematic study in S Korea (very small sample size, convenience sample)
rawSexSplit <- rawSexSplit[rawSexSplit$nid != 283465,]


# get T/F flag for whether there are multiple rows for a given combination of age/year/location/nid
# (so have duplicates without considering sex)
rawSexSplit$sex_dup <- duplicated(rawSexSplit[, c("location_id", "year_start", "age_start", "nid")]) |
  duplicated(rawSexSplit[, c("location_id", "year_start", "age_start", "nid")], fromLast=TRUE)

# get just the duplicates
temp_sex <- subset(rawSexSplit, sex_dup==TRUE)

# remove duplicate rows with both sexes (this is what we are calculating, to get the Male:Both ratio)
temp_sex <- subset(temp_sex, sex != "Both")


## Start building dataset to inform crosswalk
columnsToKeep <- c("nid", "age_start", "age_end", "year_start", "year_end", "measure", "sample_size",
                   "cases", "mean", "standard_error")

## generate and rename male-only data
male_only <- subset(temp_sex, sex=="Male")
male_only <- subset(male_only, select=c("nid", "age_start", "age_end", "year_start", "year_end", "measure", "sample_size",
                                        "cases", "mean", "standard_error"))
#male_only <- male_only[columnsToKeep]
colnames(male_only) <- gsub("sample_size", "M_sample_size", colnames(male_only))
colnames(male_only) <- gsub("cases", "M_cases", colnames(male_only))
colnames(male_only) <- gsub("mean", "M_mean", colnames(male_only))
colnames(male_only) <- gsub("standard_error", "M_standard_error", colnames(male_only))

## generate and rename female-only data
female_only <- subset(temp_sex, sex=="Female")
female_only <- subset(female_only, select=c("nid", "age_start", "age_end", "year_start", "year_end", "measure", "sample_size",
                                            "cases", "mean", "standard_error"))
#female_only <- female_only[columnsToKeep]
colnames(female_only) <- gsub("sample_size", "F_sample_size", colnames(female_only))
colnames(female_only) <- gsub("cases", "F_cases", colnames(female_only))
colnames(female_only) <- gsub("mean", "F_mean", colnames(female_only))
colnames(female_only) <- gsub("standard_error", "F_standard_error", colnames(female_only))

## merge data together
sex_crosswalk <- merge(male_only, female_only, by = c("nid", "age_start", "age_end", "year_start", "year_end", "measure"))

## get the total mean
sex_crosswalk$Tot_mean <- sex_crosswalk$M_mean + sex_crosswalk$F_mean
## get the total SE (adding at the variance level: tot^2 = M^2 + F^2)
sex_crosswalk$Tot_standard_error <- sqrt(sex_crosswalk$M_standard_error^2 + sex_crosswalk$F_standard_error^2)


## Set the reference and alternative definitions to match USERNAME's MR-BRT code (from example_prep_data_for_metaregression.R)
## here Ref = Male, and Alt = Both
sex_crosswalk$ES_ref <- sex_crosswalk$M_mean
sex_crosswalk$ES_alt <- sex_crosswalk$Tot_mean
sex_crosswalk$ref_se <- sex_crosswalk$M_standard_error
sex_crosswalk$alt_se <- sex_crosswalk$Tot_standard_error


# assumes uncertainty for 'ES_ref' and 'ES_alt' is normally distributed
#   around the point estimate
sex_crosswalk <- sex_crosswalk %>%
  mutate(
    ratio = ES_alt / ES_ref,
    #   ref_se = (ES_ref_UL - ES_ref_LL) / 3.92,
    #   alt_se = (ES_alt_UL - ES_alt_LL) / 3.92,
    ratio_se =
      sqrt((ES_alt^2 / ES_ref^2) * (alt_se^2/ES_alt^2 + ref_se^2/ES_ref^2))
  )


## output each dataset
######## The dataset to inform the crosswlk is: sex_crosswalk
fwrite(sex_crosswalk, paste0(file_location, "sex_crosswalk_ratio_", splitToPerform, ".csv"))
######## The dataset to be crosswalked is: rawSexSplit
fwrite(rawSexSplit, paste0(file_location, "raw_sex_crosswalk_", splitToPerform, ".csv"))



##############################################################
## PART V: PERFORMING A CROSSWALK FOR SEX-SPLITTING
##############################################################
### INSERT USERNAME CODE FOR CROSSWALK AGAIN, THIS TIME FOR SEX
##############################################################

## Settings for this section (change as relevant for crosswalk being performed):
#splitToPerform <- "liver_A"

#file_location <- "USERNAME/mrbrt_working/nash/"

#
# example_mrbrt_logit_crosswalk.R
#
# USERNAME
# June 2019
#
# Example of how to do a crosswalk when the outcome variable is
# bounded by zero and 1 (logit crosswalk)
#
# This script works for models with and without covariates
#
# The data come from estimates of prevalence/incidence of knee osteoarthritis
#   when using two different sets of diagnostic criteria
#
# See this link for a description of the method:
#   ADDRESS
#

library(dplyr)
library(data.table)
library(metafor, lib.loc = "FILEPATH/R_libraries/")
library(msm, lib.loc = "FILEPATH/R_libraries/")
library(readxl)

library(msm, lib.loc = "FILEPATH/R_libraries/")

#rm(list = ls())

#####
# USER SETTINGS


# original extracted data
# -- read in data frame
# -- specify how to identify reference studies
# -- identify mean and standard error variables

dat_original <- fread(paste0(file_location, "raw_sex_crosswalk_", splitToPerform, ".csv")) %>%
  mutate(age_mid = (age_start + age_end) / 2)
reference_var <- "reference"
reference_value <- 1
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- c() # if no covariates, give c() instead of c("age_mid")

# data for meta-regression model
# -- read in data frame
# -- identify variables for mean and SE of the alternative and reference
#    NOTE: the 'prev' prefix refers to prevalence,
#          although any parameter bounded by 0 and 1 can be modeled
#    NOTE: ratio must be calculated as alternative/reference
dat_metareg <- fread(paste0(file_location, "sex_crosswalk_ratio_", splitToPerform, ".csv")) %>%
  mutate(age_mid = (age_start + age_end) / 2)
prev_ref_var <- "ES_ref"
se_prev_ref_var <- "ref_se"
prev_alt_var <- "ES_alt"
se_prev_alt_var <- "alt_se"

#####


# create datasets with standardized variable names
orig_vars <- c(mean_var, se_var, reference_var, cov_names)
metareg_vars <- c(prev_ref_var, se_prev_ref_var, prev_alt_var, se_prev_alt_var, cov_names)

tmp_orig <- as.data.frame(dat_original) %>%
  .[, orig_vars] %>%
  setnames(orig_vars, c("mean", "se", "ref", cov_names)) %>%
  mutate(ref = if_else(ref == reference_value, 1, 0, 0))

tmp_metareg <- as.data.frame(dat_metareg) %>%
  .[, metareg_vars] %>%
  setnames(metareg_vars, c("prev_ref", "se_prev_ref", "prev_alt", "se_prev_alt", cov_names))


# logit transform the original data
# -- SEs transformed using the delta method
tmp_orig$mean_logit <- log(tmp_orig$mean / (1-tmp_orig$mean))
tmp_orig$se_logit <- sapply(1:nrow(tmp_orig), function(i) {
  mean_i <- tmp_orig[i, "mean"]
  se_i <- tmp_orig[i, "se"]
  deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
})

# logit transform the meta-regression data
# -- alternative
tmp_metareg$prev_logit_alt <- log(tmp_metareg$prev_alt / (1-tmp_metareg$prev_alt))
tmp_metareg$se_prev_logit_alt <- sapply(1:nrow(tmp_metareg), function(i) {
  prev_i <- tmp_metareg[i, "prev_alt"]
  prev_se_i <- tmp_metareg[i, "se_prev_alt"]
  deltamethod(~log(x1/(1-x1)), prev_i, prev_se_i^2)
})

# if need to remove NaN values
# tmp_metareg <- tmp_metareg[!is.nan(tmp_metareg$prev_logit_alt),]

# -- reference
tmp_metareg$prev_logit_ref <- log(tmp_metareg$prev_ref / (1-tmp_metareg$prev_ref))
tmp_metareg$se_prev_logit_ref <- sapply(1:nrow(tmp_metareg), function(i) {
  prev_i <- tmp_metareg[i, "prev_ref"]
  prev_se_i <- tmp_metareg[i, "se_prev_ref"]
  deltamethod(~log(x1/(1-x1)), prev_i, prev_se_i^2)
})

tmp_metareg$diff_logit <- tmp_metareg$prev_logit_alt - tmp_metareg$prev_logit_ref
tmp_metareg$se_diff_logit <- sqrt(tmp_metareg$se_prev_logit_alt^2 + tmp_metareg$se_prev_logit_ref^2)

# fit the MR-BRT model
#repo_dir <- "FILEPATH/run_mr_brt/"
#source(paste0(repo_dir, "run_mr_brt_function.R"))
#source(paste0(repo_dir, "cov_info_function.R"))
#source(paste0(repo_dir, "check_for_outputs_function.R"))
#source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
#source(paste0(repo_dir, "predict_mr_brt_function.R"))
#source(paste0(repo_dir, "check_for_preds_function.R"))
#source(paste0(repo_dir, "load_mr_brt_preds_function.R"))


# fit the MR-BRT model
source("FILEPATH/run_mr_brt/mr_brt_functions.R")

covs1 <- list()
for (nm in cov_names) covs1 <- append(covs1, list(cov_info(nm, "X")))

if (length(covs1) > 0) {

  fit1 <- run_mr_brt(
    output_dir = paste0("FILEPATH"), # user home directory
    model_label = "crosswalk_ex2",
    data = tmp_metareg,
    mean_var = "diff_logit",
    se_var = "se_diff_logit",
    covs = covs1,
    overwrite_previous = TRUE, ## start putting in options:
    method = "trim_maxL",
    trim_pct = 0.1             ########### SET TRIM LEVEL AS APPROPRIATE ##########
  )

} else {
  fit1 <- run_mr_brt(
    output_dir = paste0("FILEPATH"), # user home directory
    model_label = "crosswalk_ex2",
    data = tmp_metareg,
    mean_var = "diff_logit",
    se_var = "se_diff_logit",
    overwrite_previous = TRUE, ## start putting in options:
    method = "trim_maxL",
    trim_pct = 0.1             ########### SET TRIM LEVEL AS APPROPRIATE ##########
  )
}


preds <- predict_mr_brt(fit1, newdata = tmp_orig)$model_summaries
tmp_orig$adj_logit <- preds$Y_mean
tmp_orig$se_adj_logit <- (preds$Y_mean_hi - preds$Y_mean_lo) / 3.92

tmp_orig2 <- tmp_orig %>%
  mutate(
    prev_logit_adjusted_tmp = mean_logit - adj_logit,
    se_prev_logit_adjusted_tmp = sqrt(se_logit^2 + se_adj_logit^2),
    prev_logit_adjusted = ifelse(ref == 1, mean_logit, prev_logit_adjusted_tmp),
    se_prev_logit_adjusted = ifelse(ref == 1, se_logit, se_prev_logit_adjusted_tmp),
    prev_adjusted = exp(prev_logit_adjusted)/(1+exp(prev_logit_adjusted)),
    lo_logit_adjusted = prev_logit_adjusted - 1.96 * se_prev_logit_adjusted,
    hi_logit_adjusted = prev_logit_adjusted + 1.96 * se_prev_logit_adjusted,
    lo_adjusted = exp(lo_logit_adjusted)/(1+exp(lo_logit_adjusted)),
    hi_adjusted = exp(hi_logit_adjusted)/(1+exp(hi_logit_adjusted))
  )


tmp_orig2$se_adjusted <- sapply(1:nrow(tmp_orig2), function(i) {
  mean_i <- tmp_orig2[i, "prev_logit_adjusted"]
  mean_se_i <- tmp_orig2[i, "se_prev_logit_adjusted"]
  deltamethod(~exp(x1)/(1+exp(x1)), mean_i, mean_se_i^2)
})


# 'final_data' is the original extracted data plus the new variables
final_data <- cbind(
  dat_original,
  as.data.frame(tmp_orig2)[, c("ref", "prev_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
)

## write out to folders
#fwrite(final_data, paste0(file_location, "nash_sex_split.csv"))


######## To get a funnel plot, run the following and then put the given code in a qlogin session:
source("FILEPATH/run_mr_brt/mr_brt_functions.R")
plot_mr_brt(fit1)


##############################################################
## PART VI: FORMATTING SEX-SPLIT DATA FOR UPLOAD
##############################################################
## Code for taking the sex-crosswalked data and prepares an output
##############################################################

## Note: there are two datapoints that get an NaN for adjusted_se. Should reassign these as the
## original SE
crosswalked_sex <- final_data
crosswalked_sex$se_adjusted <- ifelse(is.nan(crosswalked_sex$se_adjusted),
                                      crosswalked_sex$standard_error, crosswalked_sex$se_adjusted)
setnames(crosswalked_sex, "prev_adjusted", "mean_adjusted")

## The mean_adjusted value for sex=Both should now be the crosswalked M proportion
## Split this out and relabel as Males
both_recode <- crosswalked_sex[crosswalked_sex$sex=="Both",]
both_recode$sex <- "Male"
both_recode$mean <- both_recode$mean_adjusted
both_recode$standard_error <- both_recode$se_adjusted
## will delete values for upper/lower


## The crosswalked F proportion is the difference between the original Both mean and the crosswalked M mean
## split this out and relabel as Females
female_recode <- crosswalked_sex[crosswalked_sex$sex=="Both",]
female_recode$sex <- "Female"
female_recode$mean <- female_recode$mean - female_recode$mean_adjusted
##  Female SE: make the same as males (same since binomial so should be equally distant from extremes)
female_recode$standard_error <- female_recode$se_adjusted


######### also get the Males, rename columns
######### then merge these three together (or rbind, since they should be the same)
######### then drop extra columns so can be uploaded (match old #/names of columns)

crosswalked_sex_combined <- rbind(both_recode, female_recode)

### will need to drop several columns in order to match upload file
crosswalked_sex_combined <- subset(crosswalked_sex_combined,
                                   select = -c(reference, sex_dup, ref, mean_adjusted, se_adjusted, lo_adjusted, hi_adjusted))


## output the sex-split dataset

fwrite(crosswalked_sex_combined, paste0(file_location, "sex_crosswalk_output_", splitToPerform, ".csv"))


#########################################################
########### END OF SCRIPT ###############################
#########################################################
