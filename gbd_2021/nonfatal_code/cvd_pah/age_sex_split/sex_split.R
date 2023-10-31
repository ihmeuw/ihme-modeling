
##
## Author: USERNAME
## Date: DATE
## 
## Purpose: Sex-split PAH data 
##

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2)


###### Paths, args
#################################################################################

central <- "FILEPATH"
bundle_id <- "VALUE"
bundle_version_id <- "VALUE"
folder_root <-  "FILEPATH"
write_file <- "FILEPATH"

gbd_round_id <- "VALUE"
decomp_step <- "VALUE"

lit_path <- "FILEPATH"

outlier_nid <- 440320

###### Functions
#################################################################################

for (i in list.files(central)) source(paste0(central, i))

for (func in paste0(central, list.files(central))) source(paste0(func))
source("/FILEPATH/master_mrbrt_crosswalk_func.R")
source("/FILEPATH/age_split.R")


###### Pull in data
#################################################################################

bundle_data <- get_bundle_version(bundle_version_id)

date <- "VALUE"
data <- fread(paste0(lit_path, "age_split_lit_data_", date, ".csv"))

## Don't use the outliers or group review data points.
data <- data[is_outlier==0 & input_type != "group_review",]
data <- data[!(nid %in% outlier_nid)]
data[, reference := ifelse(cv_icd==1,0,1)]

data_all_types <- copy(data)

bundle_data <- rbind(bundle_data[clinical_data_type!="",],data,fill=T)

###### Sex-split prevalence data
#################################################################################
date <- gsub("-", "_", Sys.Date())

measure <- "prevalence"
prevalence_data <- bundle_data[measure == "prevalence"]

crosswalk_holder <- master_mrbrt_crosswalk(
  model_abbr = "cvd_pah", 
  data = prevalence_data, 
  storage_folder = "FILEPATH",
  xw_measure = "prevalence", 
  decomp_step = "VALUE", 
  reference_def = "reference",
  alternate_defs = "cv_icd",
  gbd_round_id = "VALUE", 
  logit_transform = T, 
  sex_split_only=T,
  ignore_gr=T,
  addl_x_covs = list("male"),
  sex_age_overlap = 0,
  sex_year_overlap = 0,
  sex_age_range = 0,
  sex_year_range = 0,
  remove_x_intercept = F,
  sex_covs = "age_scaled"
)

data <- crosswalk_holder$sex_specific_data
data <- data[is.na(clinical_data_type)]
v1 <- data[cv_icd==0]
write.csv(v1, paste0(write_file, "pre_age_split_", date, "_", measure, "_no_icd.csv"))


###### Sex-split incidence data
#################################################################################

measure <- "incidence"
incidence_data <- copy(data_all_types)[measure == "incidence",]
crosswalk_holder <- master_mrbrt_crosswalk(
  model_abbr = "cvd_pah", 
  data = incidence_data, 
  storage_folder = "FILEPATH",
  xw_measure = "prevalence", 
  decomp_step = "VALUE", 
  gbd_round_id = "VALUE", 
  logit_transform = T, 
  sex_split_only=T,
  ignore_gr=T,
  addl_x_covs = c("male"),
  spline_covs = "age_scaled",
  age_overlap = 5,
  year_overlap = 5,
  age_range = 50,
  year_range = 20,
  remove_x_intercept = F,
  sex_covs = "age_scaled",
  sex_age_overlap = 10,
  sex_year_overlap = 10,
  sex_age_range = 20,
  sex_year_range = 20
)

data <- crosswalk_holder$sex_specific_data
write.csv(data, paste0(write_file, "pre_age_split_", date, "_", measure, ".csv"))


###### Sex-split mtwith data
#################################################################################

mtwith <- copy(data_all_types[measure%in%c("mtwith", "cfr")])

# Turn CFR into with condition mortality
mtwith[measure == "cfr" & cfr_period == "Period: months", `:=` (cfr_period = "Period: years", cfr_value = cfr_value / 12 )]
mtwith[measure == "cfr" & is.na(cfr_value), cfr_value := 1]
mtwith[, cfr_value := as.numeric(cfr_value)]
mtwith[measure == "cfr", mean := -(log(1-mean))/cfr_value]
mtwith[measure == "cfr", measure := "mtwith"]

measure <- "mtwith"

crosswalk_holder <- master_mrbrt_crosswalk(
  model_abbr = "cvd_pah", 
  data = mtwith, 
  storage_folder = "FILEPATH",
  xw_measure = "mtwith", 
  decomp_step = "VALUE", 
  gbd_round_id = "VALUE", 
  logit_transform = T, 
  sex_split_only=T,
  ignore_gr=T,
  addl_x_covs = c("male"),
  spline_covs = "age_scaled",
  age_overlap = 5,
  year_overlap = 5,
  age_range = 50,
  year_range = 20,
  sex_age_overlap = 10,
  sex_year_overlap = 10,
  sex_age_range = 20,
  sex_year_range = 20,
  remove_x_intercept = F,
  sex_covs = "age_scaled",
  r_linear = T,
  l_linear = T,
  knots = c(-1.8, -0.4, 0.5, 2.4)  
)

data <- crosswalk_holder$sex_specific_data
write.csv(data, paste0(write_file, "pre_age_split_", date, "_", measure, ".csv"))

