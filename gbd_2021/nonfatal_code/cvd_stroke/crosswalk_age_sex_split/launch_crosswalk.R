
##
## Purpose: Master Crosswalk script for the CVD team. Once you pull in your data and provide
##          specifications, master_crosswalk does the following:
##              - Sex splits the data
##              - If necessary, crosswalks between one+ alternate case definitions
##              - If toggled, uploads final crosswalked data to database
##          Afterwards, this script will make an RMarkdown with vetting information, which ideally has:
##              - Title of XW
##              - Date
##              - Alternate definitions to be XWed
##              - Counts of matches and ratios
##              - Betas for sex-split and crosswalk
##              - Plots
##



rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILEPATH"
} else {
  j<- "FILEPATH"
}

date <- gsub("-", "_", Sys.Date())

"%ni%" <- Negate("%in%")

"%contain%" <- function(values,x) {
  tx <- table(x)
  tv <- table(values)
  z <- tv[names(tx)] - tx
  all(z >= 0 & !is.na(z))
}

pacman::p_load(rmarkdown, data.table, ggplot2, doBy, openxlsx)


source('FILEPATH/master_mrbrt_crosswalk_func.R')

source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")

### Paths, arguments
##################################################################################

## receive initialize settings from [command args]
args <- commandArgs(trailingOnly = TRUE)
model_abbr <- as.character(args[1])
gbd_round_id <- as.integer(args[2])
decomp_step <- as.character(args[3])
settings_file_path <- as.character((args[4]))

cat(paste(args))

## Stroke arguments example - 
model_abbr <- "isch_7112"
gbd_round_id <- 7
decomp_step <- "iterative"
settings_file_path <- "FILEPATH/xw_settings.xlsx"

## require folders
plot_output_folder <- 'FILEPATH'
storage_folder <- 'FILEPATH'

plot_source_folder <- 'FILEPATH'

### Extracting and formatting all settings variables
##################################################################################

## import settings file
settings <- data.table(read.xlsx(settings_file_path))

# store all settings into a list (for passing into plotting file)
var_collector <- list()

for (single_var in settings$variables) {
  single_setting <- settings[variables == single_var, get(model_abbr)]
  
  # T/F char to Boolean
  if(single_setting %in% c('T', 'F')) {
    single_setting <- ifelse(single_setting == 'T', T, F)
  }
  
  # number/float char to numeric
  if(!grepl("\\D", single_setting) | single_var %in% c('trim_pct')) {
    single_setting <- as.numeric(single_setting)
  }
  
  # comma cat string to vector
  if(single_var %in% c('alternate_defs', 'addl_x_covs') && !is.na(single_setting)) {
    single_setting <- unlist(strsplit(single_setting, ","))
  }
  
  # mount settings to environment
  if(!is.na(single_setting)) {
    assign(single_var, single_setting)
  } else {
    assign(single_var, NULL)
  }
  
  # also store them into a collection for saving later
  var_collector[[single_var]] <- single_setting

}

## path for save crosswalk version
save_crosswalk_result_file_name <- paste0("xw_results_", date , "_", acause, "_", bundle_id, "_", xw_measure, ".xlsx")
save_crosswalk_result_file <- paste0("FILEPATH", save_crosswalk_result_file_name)


###################################################################
# Get bundle data / save bundle data
##### 
if (if_save_bundle_version) {
  result <- save_bundle_version(bundle_id, decomp_step, include_clinical=TRUE)
  print(sprintf('Request status: %s', result$request_status))
  print(sprintf('Request ID: %s', result$request_id))
  print(sprintf('Bundle version ID: %s', result$bundle_version_id))
  req_id <- result$bundle_version_id
  data <- data.table(get_bundle_version(req_id))
} else {
  data <- data.table(get_bundle_version(bundle_version_id))
}

data <- data[measure == xw_measure]


## inpatient only
if(clinical_informatics == 3) {
  data[, cv_alternate := ifelse(clinical_data_type=="inpatient", 1, 0)]
}

## claims only
if(clinical_informatics == 2) {
  data[, cv_alternate := ifelse(clinical_data_type=="claims", 1, 0)]
}

## all clinical informatics
if(clinical_informatics == 1) {
  data[, cv_alternate := ifelse(clinical_data_type!="", 1, 0)]
  
  ## select all US states and drop US States with HCUP data
  location <- get_location_metadata(location_set_id=35, gbd_round_id=6)
  us <- unlist(unique(location[parent_id==102,c("location_id")]), use.names=F)
  
  # US HCUP data duplicates the Marketscan data
  data <- subset(data, !(location_id %in% us & grepl("HCUP", field_citation_value)))
  
  ## drop data from non-representative source
  data <- subset(data, !(location_name %in% c("Jordan", "Iran", "Meghalaya", "Karnataka", "Nepal", "Kenya")))
  
}

# subset ages, prevalance clinical informatics
if(!is.null(subset_by_age)) {
  if(subset_by_age > 0) {
    data <- data[age_start >= subset_by_age]
  }
}


# add other necessary variables into collector for plotting
var_collector[['original_data']] <- data
var_collector[['storage_folder']] <- storage_folder
var_collector[['gbd_round_id']] <- gbd_round_id
var_collector[['decomp_step']] <- decomp_step

##### 
# crosswalking
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
  storage_folder = storage_folder,
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
  remove_x_intercept = remove_x_intercept,
  sex_remove_x_intercept = sex_remove_x_intercept,
  sex_covs = sex_covs,
  spline_covs = spline_covs,
  r_linear = r_linear,
  l_linear = l_linear,
  spline_monotonicity = spline_monotonicity,
  
  
  gbd_round_id = gbd_round_id,
  decomp_step = decomp_step,
  logit_transform = logit_transform,
  
  upload_crosswalk_version = upload_crosswalk_version,
  bundle_version_id = bundle_version_id,
  save_crosswalk_result_file = save_crosswalk_result_file
)

### Saving data
###################################################################################

###
#
# Rmarkdown render plots for vetting
#
# Plot file for each causes will be generated in an individual folder 
#
# See docs for details of each plotting function
#
#

## save all potentially useful data to local Rdata, for quick recreating xw environment and render two rounds xw comparison plot

crosswalk_holder$var_collector = data.table(t(var_collector))

if(save_rdata_local) {
  save(crosswalk_holder, file = paste0(storage_folder, model_abbr , "_crosswalk_holder.Rdata"))
  message(paste0("\n [INFO] All results saved to ", storage_folder, model_abbr , "_crosswalk_holder.Rdata\nPython objects that were saved during crosswalking can also be found in the folder\n"))
}


### Plotting
###################################################################################


# NOTE: If your crosswalk_holder is no longer in your memory but you want to run diagnostics on it, uncomment the following line to load it in:
#load(paste0(storage_folder, model_abbr , "_crosswalk_holder.Rdata"))

# create folder for plot if it doesn't exist
dir.create(file.path(plot_output_folder, model_abbr), showWarnings = FALSE)

# remove .Rhistory from vetting folder to avoid error if someone else saved their own .Rhistory here when code was run
if (file.exists(paste0(plot_source_folder, '.Rhistory'))){
  file.remove(paste0(plot_source_folder, '.Rhistory'))
}

# call rmarkdown
rmarkdown::render(
  input = paste0(plot_source_folder, 'vetting_report.Rmd'),
  output_file = paste0(plot_output_folder, model_abbr, "/", model_abbr, "_", date, ".html"),
  clean = TRUE,
  
  params = list(storage_folder = storage_folder, model_abbr = model_abbr, 
                crosswalk_holder = crosswalk_holder)
)












