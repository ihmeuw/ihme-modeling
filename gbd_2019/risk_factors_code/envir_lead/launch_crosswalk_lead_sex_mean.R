## Purpose: Lead sex-split & mean crosswalk

rm(list=ls())

date <- gsub("-", "_", Sys.Date())

"%ni%" <- Negate("%in%")

pacman::p_load(rmarkdown, data.table, ggplot2, openxlsx, magrittr, combinat, msm)

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

### Paths, arguments
##################################################################################

# initialization vars
model_abbr <- "lead_sex_mean_xw"
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
data <- read.xlsx("FILEPATH") %>% as.data.table

data[cv_mean_type == 0, gm_mean := 1]
data[is.na(gm_mean), gm_mean := 0]
data[cv_mean_type == 3, median := 1]
data[is.na(median), median := 0]
data[gm_mean == 0 & median == 0, am_mean := 1]
data[is.na(am_mean), am_mean := 0]

setnames(data, "val", "mean")

# need standard error
data[is.na(standard_error), standard_error := standard_deviation/(sqrt(sample_size))]

data[, mean.orig := mean]
data[, var.orig := variance]
data[, se.orig := standard_error]

## we need to aggregate subnations if there're corresponding national
data <- data[measure == xw_measure]

location <- get_location_metadata(location_set_id=35, gbd_round_id=6)

lapply(X = c("level", "parent_id"), FUN = function(x) if (x %in% names(data)) data[, paste0(x) := NULL])

data <- merge(data, location[, .(location_id, level, parent_id)], by = "location_id")

unique_parent_ids <- c(102)

non_aggr <- data[parent_id %ni% unique_parent_ids][, aggregated := F]
subnats_to_aggr <- data[parent_id %in% unique_parent_ids][, aggregated := T]

combined_data <- NULL

# add other necessary variables into collector for plotting
var_collector[['original_data']] <- data
var_collector[['temp_folder']] <- temp_folder
var_collector[['gbd_round_id']] <- gbd_round_id
var_collector[['decomp_step']] <- decomp_step
if(!is.null(combined_data)) {
  var_collector[['combined_data']] <- combined_data
  data <- combined_data
}


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