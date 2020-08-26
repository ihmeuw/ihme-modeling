
##
## Date: 5/20/19
## Purpose: Master Crosswalk script for the CVD team. Once you pull in your data and provide
##          specifications, master_crosswalk does the following:
##              - Sex splits the data
##              - If necessary, crosswalks between one+ alternate case definitions
##              - If toggled, uploads final crosswalked data to database
##              * Almost everything is toggle-able. See documentation (FILEPATH) about what features can be edited.
##          Afterwards, this script will make an RMarkdown with vetting information, which ideally has:
##              - Title of XW
##              - Date
##              - Alternate definitions to be XWed
##              - Counts of matches and ratios
##              - Betas for sex-split and crosswalk
##              - Plots
##

##
##
## How to test code manually?
##
## 1. comment [command args] : line 51-57
## 2. uncomment [dummy initialization vars] : line 60-63
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


source('FILEPATH/master_mrbrt_crosswalk_func.R') #master_mrbrt_crosswalk_function.R
source('FILEPATH/MI_xw.R')               #MI_xw.R
source("FILEPATH/aggregate_subnats.R")   #aggreate_subnats.R

source("FILEPATH/save_bundle_version.R") #save_bundle_version.R
source("FILEPATH/get_bundle_version.R")  #get_bundle_version.R

### Paths, arguments
##################################################################################

## receive initialize settings from [command args]
args <- commandArgs(trailingOnly = TRUE)
model_abbr <- as.character(args[1])
gbd_round_id <- as.integer(args[2])
decomp_step <- as.character(args[3])
settings_file_path <- as.character((args[4]))

cat(paste(args))

## [dummy initialization vars]
# model_abbr <- "afib_119"
# gbd_round_id <- 6
# decomp_step <- "step2"
# settings_file_path <- "FILEPATH"

## require folders
temp_folder <- 'FILEPATH/temp_folder/'
plot_source_file <- "FILEPATH/vetting_report.Rmd"
plot_output_folder <- 'FILEPATH/plots/'
storage_folder <- 'FILEPATH/storage/'

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
save_crosswalk_result_file <- paste0("FILEPATH/", save_crosswalk_result_file_name)


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

## create alternative defination column for special cases
if(model_abbr == "cavd_2987") {
  data[, `:=` (cv_claims = ifelse(clinical_data_type=="claims", 1, 0), cv_inpatient = ifelse(clinical_data_type=="inpatient", 1, 0))]
}

if(model_abbr == "angina_115") {
  cols <- grep("^cv_raq", colnames(data), value=TRUE)
  data[, cv_alternate := rowSums(.SD), .SDcols = cols]
  data[!is.na(cv_alternate) & cv_alternate > 1, cv_alternate := 1]
  data <- data[!(is.na(cv_alternate) & clinical_data_type == "inpatient")]
  data[is.na(cv_alternate), cv_alternate := 0]
  data[, reference := ifelse(clinical_data_type=="claims", 1, 0)]
  # 296602 is cv_raq=1; 109878: there are two where group_review==0;
  data[nid == 296602, cv_alternate := 1]
  data[nid == 109878 & group_review != 0, cv_alternate := 1]
  data <- data[nid != 109891]

  data[, which(grepl("^cv_raq", colnames(data))):=NULL]
}


if(model_abbr == "mi_114") {
  ### [ Integrate MI troponin as reference data ] ###
  ##
  # FILEPATH/MI_xw.R
  # prep_mi_troponin <- prep_mi_troponin(data)
  # data <- prep_mi_troponin[["final_dt"]]
  # mi_troponin_crosswalk_holder <- prep_mi_troponin[["crosswalk_holder"]]
  for (x in alternate_defs) data[is.na(get(x)), paste0(x) := 0]
}



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

### Aggregate data ###
data <- aggregate_subnats(data)



# add other necessary variables into collector for plotting
var_collector[['original_data']] <- data
var_collector[['temp_folder']] <- temp_folder
var_collector[['gbd_round_id']] <- gbd_round_id
var_collector[['decomp_step']] <- decomp_step


# save.image(file=paste0(storage_folder, "FILEPATH/mi_114_image.Rdata"))
# message("\n[INFO] mi_114_image before master_mrbrt_crosswalk saved to ", paste0(storage_folder, "special/mi_114_image.Rdata"), "\n")

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

### Plotting
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
if(save_rdata_local) {
  save(crosswalk_holder, var_collector, file = paste0(storage_folder, "all_results_before_plotting/", model_abbr , ".Rdata"))
  message(paste0("\n [INFO] All results saved to ", storage_folder, "all_results_before_plotting/", model_abbr , ".Rdata\n"))
}



# create folder for plot if it doesn't exist
dir.create(file.path(plot_output_folder, model_abbr), showWarnings = FALSE)

# call rmarkdown
rmarkdown::render(
  input = plot_source_file,
  output_file = paste0(plot_output_folder, model_abbr, "/", model_abbr, "_", date, ".html"),
  clean = TRUE,

  params = list(var_collector = var_collector,
                crosswalk_holder = crosswalk_holder)
)












