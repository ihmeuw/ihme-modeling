########################################################
# Date: DATE
# Author: USERNAME
# Purpose: Proportion with intermittent claudication
########################################################
# WORKFLOW
# a. Bundle version 
# b. Sex split
# c. Crosswalk version
# d. Age split - Run dismod model
# e. Age split – Implement age pattern
# a. Review settings, outliers.
# f. Run diagnostics
# Vet model
########################################################

#Run first
library(vctrs, lib.loc="FILEPATH/rstudio/lib/4.1.3.4") 
library(openxlsx)
library(readxl)
library(doBy)
library(ggplot2)
library(doBy)

suppressMessages(library(R.utils))

logs <- "FILEPATH/errors"
date <- gsub("-", "_", Sys.Date())

## Source central functions: 
central <- "FILEPATH"
for (func in paste0(central, list.files(central))) source(paste0(func))


cwversion <- get_version_quota(bundle_id = 120)

old_padprop <- get_crosswalk_version(crosswalk_version_id=40825)


padprop <- get_bundle_data(bundle_id=120)


##SEX SPLIT

"%ni%" <- Negate("%in%")

"%contain%" <- function(values,x) {
  tx <- table(x)
  tv <- table(values)
  z <- tv[names(tx)] - tx
  all(z >= 0 & !is.na(z))
}

library(rmarkdown)
library(data.table)
library(openxlsx)

source('FILEPATH/main_mrbrt_crosswalk_func.R')
source("FILEPATH/aggregate_subnats.R")

source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")

source("FILEPATH/get_covariate_estimates.R")


##################################################################################
# 1) Set arguments and filepaths, load in crosswalk settings
##################################################################################

### Arguments
##################################################################################

args <- commandArgs(trailingOnly = T)
if(length(args)==0){
  ######################################################
  # enter arguments here if running code interactively #
  ######################################################
  acause <- 'cvd_pvd'
  bundle_id <- 120
  xw_measure <- 'proportion'
  xw_index <- 3
  ######################################################
} else {
  # if not running code interactively, arguments will be pulled in from job call
  acause <- as.character(args[1])
  bundle_id <- as.numeric(args[2])
  xw_measure <- as.character(args[3])
  
  xw_index_path <- as.character(args[4])
  xw_index_file <- fread(xw_index_path)
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  xw_index <- as.numeric(xw_index_file[task_id, xw_index])
  
}

### Create variables
##################################################################################

model_abbr <- paste0(acause, "_", xw_measure, "_", bundle_id, "_index", xw_index)

### Paths
##################################################################################

## path for settings sheets
settings_path <- paste0('FILEPATH', acause, '.xlsx')

## path for storage folder
storage_folder <- paste0('FILEPATH', acause, '/')

# create storage folder if it doesn't exist
dir.create(storage_folder, showWarnings=F)

## path for save crosswalk version
save_crosswalk_result_file_name <- paste0("xw_results_", date , "_", model_abbr, ".xlsx")
save_crosswalk_result_file <- paste0(storage_folder, save_crosswalk_result_file_name)

## path for diagnostics
plot_source_folder <- 'FILEPATH'
plot_output_folder <- paste0('FILEPATH', acause, '/')

# create folder for diagnostics if it doesn't exist
dir.create(plot_output_folder, showWarnings=F)

### Extracting and formatting all settings variables
##################################################################################

## import settings file
settings <- as.data.table(read.xlsx(settings_path, sheet=paste0(xw_measure, "_", bundle_id)))
settings <- settings[model_index==xw_index]
if(nrow(settings)!=1){
  stop(paste0('One unique row with xw_index ', xw_index, ' not found.'))
}
settings[, `:=` (Modeler.Notes = NULL, model_index = NULL)]

# store all user-specified args into a list
var_collector <- list()

for (single_var in names(settings)) {
  single_setting <- settings[, get(single_var)]
  
  if(is.null(single_setting)){
    single_setting <- NA
  }
  
  # unlist if necessary
  if(class(single_setting)=='list'){
    single_setting <- unlist(single_setting)
  }
  
  # T/F char to Boolean
  if(single_setting %in% c('T', 'F')) {
    single_setting <- ifelse(single_setting == 'T', T, F)
  }
  
  # number/float char to numeric
  if(!grepl("\\D", single_setting) | single_var %in% c('trim_pct')) {
    single_setting <- as.numeric(single_setting)
  }
  
  # Coerce necessary values to integer
  if(single_var %in% c("degree", "knot_number")){
    single_setting <- as.integer(single_setting)
  }
  
  # comma cat string to vector
  if(single_var %in% c('alternate_defs', 'addl_x_covs', 'knots', 'test_age_overlap', 
                       'test_year_overlap', 'test_age_range', 'test_year_range', 'test_subnat_to_nat', 'test_allow_wide_age_bin_match') & !is.na(single_setting)) {
    single_setting <- unlist(strsplit(single_setting, ","))
    if(single_var %in% c('knots', 'test_age_overlap', 
                         'test_year_overlap', 'test_age_range', 'test_year_range')){
      single_setting <- as.numeric(single_setting)
    }
    if(single_var=='test_subnat_to_nat' | single_var=='test_allow_wide_age_bin_match'){
      single_setting <- as.logical(single_setting)
    }
  }
  
  # mount settings to environment
  if(all(!is.na(single_setting))) {
    assign(single_var, single_setting)
  } else {
    assign(single_var, NULL)
  }
  
  # also store them into a collection for saving later
  var_collector[[single_var]] <- single_setting
  
}

##################################################################################
# 2) Get bundle data / save bundle data
##################################################################################

if (if_save_bundle_version) {
  result <- save_bundle_version(bundle_id=bundle_id, include_clinical=TRUE)
  print(sprintf('Request status: %s', result$request_status))
  print(sprintf('Request ID: %s', result$request_id))
  print(sprintf('Bundle version ID: %s', result$bundle_version_id))
  bundle_version_id <- result$bundle_version_id
  data <- data.table(get_bundle_version(bundle_version_id))
  var_collector[['bundle_version_id']] <- bundle_version_id
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
  location <- get_location_metadata(location_set_id=35, release_id=release_id)
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
if(add_aggregate_subnats){
  data <- aggregate_subnats(data, release_id)
}

# drop values over 1
rw <- nrow(data[mean>=1])
if(rw>0&logit_transform){
  message(paste0("There are ", rw, " row(s) of data with values >= 1. These will be dropped from the data."))
  data <- data[mean<1]
}

# add any necessary covariates for xwalk (ex: sdi, haqi)
if(!is.null(add_cov_to_data)){
  data[, year_id := round((year_start+year_end)/2, 0)]
  gbd_covs <- get_covariate_estimates(release_id=release_id,
                                      covariate_id=add_cov_to_data, 
                                      location_id=unique(data$location_id),
                                      year_id=unique(data$year_id),
                                      sex_id=3,
                                      age_group_id = c(22,27))
  
  if(nrow(gbd_covs)==0){
    stop("No covariate info for both sexes, all ages/age-standardized available. Cannot use sex-specific or age-specific covariates in the crosswalk since bundle data still needs to be sex-split and age-split.")
  }
  
  added_cov_mv_id <- unique(gbd_covs$model_version_id)
  gbd_cov_name <- unique(gbd_covs$covariate_name_short)
  gbd_covs <- gbd_covs[, c('location_id', 'year_id', 'mean_value'), with=F]
  setnames(gbd_covs, 'mean_value', gbd_cov_name)
  
  rw <- nrow(data)
  data <- merge(data, gbd_covs, by=c('location_id', 'year_id'))
  if(nrow(data)!=rw){
    warning(paste0("After merging in covariates, there are now ", nrow(data), " rows of data whereas before there were ", rw, ". Please check!"))
  }
  data[, year_id := NULL]
  
} else {
  added_cov_mv_id <- NULL
}

# add other necessary variables into collector for plotting
var_collector[['added_cov_mv_id']] <- added_cov_mv_id
var_collector[['settings_path']] <- settings_path
var_collector[['storage_folder']] <- storage_folder
var_collector[['save_crosswalk_result_file_name']] <- save_crosswalk_result_file_name
var_collector[['release_id']] <- release_id
var_collector[['model_abbr']] <- model_abbr

##################################################################################
# 3) Run the crosswalk
##################################################################################

crosswalk_holder <- main_mrbrt_crosswalk(
  
  model_abbr = model_abbr,
  data = data,
  storage_folder = storage_folder,
  reference_def = reference_def,
  alternate_defs = alternate_defs,
  xw_measure = xw_measure,
  release_id = release_id,
  logit_transform = logit_transform,
  
  save_crosswalk_result_file = save_crosswalk_result_file,
  bundle_version_id = bundle_version_id,
  
  sex_split_only = sex_split_only,
  
  age_overlap = age_overlap,
  year_overlap = year_overlap,
  age_range = age_range,
  year_range = year_range,
  subnat_to_nat = subnat_to_nat,
  allow_wide_age_bin_match = allow_wide_age_bin_match,
  
  sex_age_overlap = sex_age_overlap,
  sex_year_overlap = sex_year_overlap,
  sex_age_range = sex_age_range,
  sex_year_range = sex_year_range,
  
  spline_covs = spline_covs,
  degree = degree,
  knots = knots,
  knot_placement = knot_placement,
  knot_number = knot_number,
  r_linear = r_linear,
  l_linear = l_linear,
  spline_monotonicity = spline_monotonicity,
  
  trim_pct = trim_pct,
  remove_x_intercept = remove_x_intercept,
  sex_remove_x_intercept = sex_remove_x_intercept,
  addl_x_covs = addl_x_covs,
  sex_covs = sex_covs,
  offset_method = offset_method,
  
  if_save_bundle_version = if_save_bundle_version,
  acause = acause,
  modelable_entity_id = modelable_entity_id,
  model = model,
  bundle_id = bundle_id,
  xw_index = xw_index,
  reference_def_text = reference_def_text,
  clinical_informatics = clinical_informatics,
  subset_by_age = subset_by_age,
  add_aggregate_subnats = add_aggregate_subnats,
  add_cov_to_data = add_cov_to_data,
  added_cov_mv_id = added_cov_mv_id,
  
  test_matches = test_matches,
  plot_output_folder = plot_output_folder,
  test_age_overlap = test_age_overlap,
  test_year_overlap = test_year_overlap,
  test_age_range = test_age_range,
  test_year_range = test_year_range,
  test_subnat_to_nat = test_subnat_to_nat,
  test_allow_wide_age_bin_match = test_allow_wide_age_bin_match
)

### Saving data
###################################################################################

if(class(crosswalk_holder)=='character'){
  
  # stop running the code if the crosswalk matching diagnostics were run
  paste0("Main crosswalk diagnostics will not be generated since full crosswalk was not run and matching diagnostics were generated instead\n\n", crosswalk_holder)
  
} else {
  
  ## save all potentially useful data to local Rdata, for quick recreating xw environment and generating diagnostics
  crosswalk_holder$var_collector <- data.table(t(var_collector))
  
  save(crosswalk_holder, file = paste0(storage_folder, model_abbr , "_crosswalk_holder.Rdata"))
  message(paste0("\nAll results saved to ", storage_folder, model_abbr , "_crosswalk_holder.Rdata\nPython objects that were saved during crosswalking can also be found in the folder\n"))
  
  ##################################################################################
  # 4) Run diagnostics
  ##################################################################################
  
  # if crosswalk_holder is not currently in the environment, re-load it in
  if(!exists('crosswalk_holder')){
    load(paste0(storage_folder, model_abbr , "_crosswalk_holder.Rdata"))
  }
  
  # remove .Rhistory from vetting folder to avoid error if someone else saved their own .Rhistory here when code was run
  if (file.exists(paste0(plot_source_folder, '.Rhistory'))){
    file.remove(paste0(plot_source_folder, '.Rhistory'))
  }
  
  # call rmarkdown for diagnostics
  rmarkdown::render(
    input = paste0(plot_source_folder, 'vetting_report.Rmd'),
    output_file = paste0(plot_output_folder, model_abbr, "_", date, ".html"),
    clean = TRUE,
    
    params = list(storage_folder = storage_folder, model_abbr = model_abbr, 
                  crosswalk_holder = crosswalk_holder)
  )
  
}


## RESULTS FROM ABOVE XW
xwalk_prop<- openxlsx::read.xlsx("FILEPATH/xw_results_2024_03_08_cvd_pvd_proportion_120_index3.xlsx")

padprop <- get_bundle_version(44331)
padprop_split <- padprop[sex!="Both"]


source('FILEPATH/age_split.R')
xwalk_prop <- age_split(df = xwalk_prop, 
                        model_id = 1861,
                        model_version_id = 785197,
                        release_id = 16,
                        measure = 'proportion',
                        global_age_pattern = TRUE,
                        drop_under_1_case = F,
                        gbd_age_groups=F,
                        age_bin_size = 10)


xwalk_prop$midage <- (xwalk_prop$age_start +xwalk_prop$age_end)/2

ggplot(xwalk_prop, aes(x=midage, y=mean)) + 
  geom_point(size = 5)+ 
  geom_errorbarh(aes(xmin=age_start, xmax=age_end))

xwalk_prop_2 <- xwalk_prop[age_end-age_start<25]
df <- xwalk_prop_2
df$standard_error<- ifelse(df$standard_error>1, 0.99, df$standard_error)
df[is.na(cases), cases:= mean/(standard_error^2)]
df[is.na(effective_sample_size), effective_sample_size := mean/(standard_error^2)]
View(df[,c('nid','sex','year_start','year_end','age_start','age_end','measure','mean','lower','upper','standard_error','cases','sample_size','effective_sample_size')])

View(df[!which(effective_sample_size >=  cases)])
df <- df[effective_sample_size >=  cases] 

## NID 120243 SWE issue
View(df[nid==120243])
df[nid==120243, `:=` (ihme_loc_id = 'SWE', location_id = 93, location_name = 'Sweden')]

write.xlsx(df,  "FILEPATH/xw_for_120_alldata_10032024.xlsx", sheetName="extraction")

save_crosswalk_version(bundle_version_id= 44331, "FILEPATH/xw_for_120_alldata_10032024.xlsx", description="10/03/24;all age/sex split data for 120;fixed age-split uploader issue")





