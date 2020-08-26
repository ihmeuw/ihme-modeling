## Purpose: lead urbanicity crosswalk

rm(list=ls())

date <- gsub("-", "_", Sys.Date())

"%ni%" <- Negate("%in%")

pacman::p_load(rmarkdown, data.table, ggplot2, openxlsx, magrittr, combinat, msm)

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

### Paths, arguments
##################################################################################
### initialization vars
model_abbr <- "lead_urb_xw"
gbd_round_id <- 6
decomp_step <- "step2"
settings_file_path <- "FILEPATH"

## require folders
temp_folder <- "FILEPATH"
plot_source_file <- "FILEPATH"
plot_output_folder <- "FILEPATH"

if(!dir.exists(temp_folder)) dir.create(temp_folder, recursive = T)

### Extracting and formatting all settings variables
##################################################################################

## import settings file
settings <- data.table(read.xlsx(settings_file_path))

# store all settings into a list (for passing into plotting file)
var_collector <- list()

for (single_var in settings$variables) {
  single_setting <- settings[variables == single_var, get(model_abbr)]

  if(single_setting %in% c('T', 'F')) {
    single_setting <- ifelse(single_setting == 'T', T, F)
  }

  # if number only
  if(!grepl("\\D", single_setting) | single_var %in% c('trim_pct')) {
    single_setting <- as.numeric(single_setting)
  }

  if(single_var %in% c('alternate_defs', 'addl_x_covs') && !is.na(single_setting)) {
    single_setting <- unlist(strsplit(single_setting, ","))
  }

  if(single_var %in% c('alternate_defs_cov_id') && !is.na(single_setting)) {
    single_setting <- unlist(strsplit(as.character(single_setting), ","))
    single_setting <- as.numeric(single_setting)
  }

  var_collector[[single_var]] <- single_setting

}

## unpack the list to create variables with the same name of crosswalk inputs
for (name in names(var_collector)) {
  if(!is.na(var_collector[[name]])) {
    assign(name, var_collector[[name]])
  } else {
    assign(name, NULL)
  }
}


###################################################################################
### read in file
# this is data that has been sex-split and crosswalked to arithmetic mean
data <- read.xlsx("FILEPATH") %>% as.data.table

# load urbanicity covariate
prop_urban <- get_covariate_estimates(covariate_id=854, decomp_step = "step2")
#off-set values of 0 or 1 (for locations like Singapore) because they will be logit inputs
prop_urban[mean_value == 1, mean_value := 0.99999]
prop_urban[mean_value == 0, mean_value := 0.00001]
prop_urban[, lt_prop_urban := logit(mean_value)]
prop_urban <- prop_urban[, .(location_id, year_id, lt_prop_urban)]

# merge onto data
data <- merge(data, prop_urban,by=c("location_id","year_id"), all.x=T)

# When point data is representative, copy over location's prop_urban, otherwise use the data to infer urbanicity
# When urbanicity unknown, copy over location's prop_urban as well
data[representative_name == "Nationally representative only" | representative_name == "Representative for subnational location only",
    urbanicity_type := "0"]
data[urbanicity_type == "2" | urbanicity_type == "Urban", lt_urban := 0.99999]
data[urbanicity_type == "3" | urbanicity_type == "Rural", lt_urban := 0.00001]
data[urbanicity_type == "Mixed/both" | urbanicity_type == "Suburban", lt_urban := 0.5]
data[grepl("IND_",ihme_loc_id), lt_urban := NA]

# take logit of prop_urban
data[, lt_urban := logit(lt_urban)]
data[is.na(lt_urban), lt_urban := lt_prop_urban]

# reference data points are the ones where the study urbanicity (lt_urban) equals the national average urbanicity (lt_prop_urban)
data <- data[lt_prop_urban == lt_urban, urb_ref := 1]
data <- data[lt_prop_urban != lt_urban, urb_ref := 0]
# alternate data points are everything else
data <- data[lt_prop_urban != lt_urban, urb_alt := 1]
data <- data[lt_prop_urban == lt_urban, urb_alt := 0]

data[, mean.sexmeanxw := mean]
data[, var.sexmeanxw := variance]
data[, se.sexmeanxw := standard_error]

# add other necessary variables into collector for plotting
var_collector[['original_data']] <- data
var_collector[['temp_folder']] <- temp_folder
var_collector[['gbd_round_id']] <- gbd_round_id
var_collector[['decomp_step']] <- decomp_step

## path for save crosswalk version
save_crosswalk_result_file_name <- "FILEPATH"
save_crosswalk_result_file <- "FILEPATH"


#####
# If you don't have your case defs marked appropriately, do it now..!
#####

crosswalk_holder <- master_mrbrt_crosswalk(

  model_abbr = model_abbr,
  data = data,
  reference_def = reference_def,
  alternate_defs = alternate_defs,
  age_overlap = age_overlap,
  year_overlap = year_overlap,
  age_range = age_range,
  year_range = year_range,
  xw_measure = xw_measure,
  addl_x_covs = addl_x_covs,
  temp_folder = temp_folder,
  sex_split_only = sex_split_only,
  subnat_to_nat = subnat_to_nat,

  sex_age_overlap = sex_age_overlap,
  sex_year_overlap = sex_year_overlap,
  sex_age_range = sex_age_range,
  sex_year_range = sex_year_range,
  sex_fix_zeros = sex_fix_zeros,
  use_lasso = use_lasso,
  id_vars = id_vars,
  opt_method = opt_method,
  trim_pct = trim_pct,
  include_x_intercept = include_x_intercept,
  sex_include_x_intercept = sex_include_x_intercept,
  sex_covs = sex_covs,

  gbd_round_id = gbd_round_id,
  decomp_step = decomp_step,
  logit_transform = logit_transform,

  upload_crosswalk_version = upload_crosswalk_version,
  bundle_version_id = bundle_version_id,
  save_crosswalk_result_file = save_crosswalk_result_file
)