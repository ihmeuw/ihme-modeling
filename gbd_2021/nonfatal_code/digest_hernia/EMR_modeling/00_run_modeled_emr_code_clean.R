###############################################################################
## Purpose: EMR regression using MR BRT, GBD 2020
## This is the default recommended modeled emr settings. Specific causes may have
#   different settings 
###############################################################################

rm(list=ls())

### Packages and directory set up ---------------------------------------------
pacman::p_load(data.table, ggplot2, tidyverse, stringr, DBI, openxlsx, gtools, gridExtra)
date <- Sys.Date()

## Source functions
functions_dir <- "FILEPATH"
repo_dir <- "FILEPATH"
emr_dir <- "FILEPATH"

source(paste0(repo_dir, "mr_brt_functions.R"))
shared_functs <- c("get_bundle_data.R", "get_crosswalk_version.R","save_crosswalk_version.R", 
                   "get_covariate_estimates.R", "get_location_metadata.R", "get_age_metadata.R")
invisible(lapply(shared_functs, function(x) source(paste0(functions_dir, x))))

# This is where the main functions for modeled emr live
source(paste0(emr_dir, "01_modeled_emr_functions.R"))
library(msm, lib.loc = "FILEPATH")


### Set up: Fill these options in ---------------------------------------------

#Filepaths and variable names
home_dir <- "FILEPATH" #general location to save EMR files
cause_name <- "cause_name" #cause name for saving csvs and for use in plot titles
#EMR data 
model_id <- model_id_from_dismod   #Dismod model version ID from which you want to pull EMR

#Upload specs
user_name <- Sys.info()['user'] #for uploader validation
bundle_id <- ID #bundle ID
crosswalk_version_id <- ID #xwalk version to which you want to append the predicted EMR - should be iterative 2020 
bundle_version_id <- ID  #bundle version for crosswalk_version_id specified above 
upload_datapath <- paste0("FILEPATH/2019_modeled_emr_xwalk_", date, ".xlsx") #upload path for new xwalk version
seq_emr <- ID #seq generated for EMR dummy row in bundle version listed above

#MR-BRT decisions
cov_list <- "age_sex_haq"  #covariates included in MR-BRT run (vary if trying different covariates)
trim <- 0.1 #change to trim more/less in MR-BRT regression

#Remove specific locations, inpatient and/or claims
remove_locations <- c() #fill in location_ids you want to exclude from EMR analysis separated by commas
remove_inpatient = FALSE  #remove all inpatient?
remove_marketscan = FALSE #remove all U.S. Marketscan claims?
remove_taiwan <- FALSE #remove Taiwan claims?
remove_singapore <- FALSE #remove Singapore claims?
remove_poland <- FALSE #remove Poland claims?
remove_russia <- FALSE #remove Russia claims?

# Prediction matrix decisions, 10 - 12 age points recommended
age_predict <- c(0,10,20,30,40,50,60,70,80,90,100)

location_level <- c(3,4) # 3 = national only, c(3,4) = national and subnational

### 1) Prep Data --------------------------------------------------------------

## Create date-specific output filepaths if they do not already exist
dir.create(file.path(home_dir, cause_name, date, 'diagnostics'), recursive = TRUE)
bundle_output_path <- paste0(home_dir, cause_name, "/")
date_output_path <- paste0(bundle_output_path, date, "/")
plot_output_path <- paste0(date_output_path, "diagnostics/")

## Pull in EMR data, rename, and remove unneeded variables
dt <- as.data.table(get_emr_data(model_id)) %>%
  rename(nid = source_nid, underlying_nid = source_underlying_nid) %>%
  dplyr::select(-outlier_type_id, -model_version_id, -model_version_dismod_id)

## This function removes certain clinical and location data specified
dt <- remove_clinical_or_location_data(data = dt, locations_to_remove = remove_locations)

## Reformatting data for emr process, adds midage and midyear, changes sex to 0,1
# log mean and standard error. This takes a long time
dt <- reformat_data_for_modeled_emr(data = dt)

## Custom function to merge haqi by year/location, output list of HAQ and data
dt <- merge_haqi_covariate(dt)
haq <- dt[[2]] #grab haq data from list output
dt <- dt[[1]] #grab data table from list output

# Plot distribution of HAQ for all locations
locs <- as.data.table(get_location_metadata(35, gbd_round_id=7))

plot_input_haq_data(data=dt, plot_output_path = plot_output_path, haq_data = haq)

## Export results as csv file
write.csv(dt, paste0(date_output_path, cause_name, "_mrbrt_emr_" ,date, ".csv"), row.names = F)
# dt <- read.csv(paste0(date_output_path, cause_name, "_mrbrt_emr_" ,date, ".csv")) # for testing if needed

### 2) Link and launch a MR-BRT model --------------------------------------

# MR-BRT specs
knot_number <- 3 
spline_type <- 3
spline_tail <- T 
bspline_gprior_mean = "0, 0, 0, 0"  
bspline_gprior_var =  "inf, inf, inf, inf"  
knot_placement <- "frequency"

#Run model
fit1 <- run_mr_brt(
  output_dir = date_output_path,
  model_label = paste0(cause_name, cov_list, date),
  data = paste0(date_output_path, cause_name, "_mrbrt_emr_", date, ".csv"),
  mean_var = "log_ratio",
  se_var = "delta_log_se",
  overwrite_previous = TRUE,
  remove_x_intercept = FALSE,
  method = "trim_maxL",
  study_id = "id",
  trim_pct = trim,
  covs = list(   
    cov_info("midage", "X", degree = spline_type, n_i_knots = knot_number, 
             bspline_gprior_mean = bspline_gprior_mean,
             bspline_gprior_var = bspline_gprior_var,
             r_linear = spline_tail, l_linear = spline_tail, 
             knot_placement_procedure = knot_placement), 
    cov_info("sex_binary", "X"),
    cov_info("haqi_mean", "X", uprior_ub = 0, uprior_lb = "-inf")
  )
)

## Plot trimmed and used input data - note map will be updated in late May 2020
train_data <- fit1$train_data

# custom function to create map from emr input data
map_emr_input(data=train_data)

### 3) Run predictions and plot MR-BRT ----------------------------------------

## Set up matrix for predictions for specified ages (age_predict variable), 
## male/female, all national and subnational locations, all Dismod years
loc_pull <- locs$location_id[locs$level == 3 | locs$level == 4]

pred_df <- expand.grid(year_id=c(1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022), location_id = loc_pull, midage=age_predict, sex_binary=c(0,1))
pred_df <- left_join(pred_df, haq[,c("haqi_mean","location_id","year_id")], by=c("location_id","year_id"))

pred_df$age_start <- pred_df$midage
pred_df$age_end <- pred_df$midage

## Predict mr brt
pred <- predict_mr_brt(fit1, newdata = pred_df, write_draws = F)
check_for_preds(pred)
pred_object <- load_mr_brt_preds(pred)
predicted <- pred_object$model_summaries

setnames(predicted, c("X_sex_binary","X_midage","X_haqi_mean") , c("sex_binary","midage","haqi_mean"))
predicted[, c("X_intercept", "Z_intercept")] <- NULL

final_emr <- cbind(predicted, pred_df)
final_emr <- final_emr[, !duplicated(colnames(final_emr))]
final_emr[, c("Y_negp", "Y_mean_fe", "Y_mean_lo_fe", "Y_mean_hi_fe", "Y_negp_fe", "midage", "haqi_mean")] <- NULL

graph_emr <- graph_mrbrt_results(results = fit1, predicts = predicted)
graph_emr
ggsave(graph_emr, filename = paste0(plot_output_path, "fit_plot_normal.pdf"), width = 10, height = 7)

graph_emr_log <- graph_mrbrt_log(results = fit1, predicts = predicted)
graph_emr_log
ggsave(graph_emr_log, filename = paste0(plot_output_path, "fit_plot_log.pdf"), height = 7, width = 10)


### 4) Apply ratios to the orginal data ---------------------------------------

final_dt <- as.data.table(final_emr)

# Function that will apply the ratios to the data and reformat for upload
final_dt <- apply_mrbrt_ratios_and_format(final_dt, seq_of_dummy_emr = seq_emr, extractor = user_name) 

### 5) Pull in crosswalk version of choice to append EMR data -----------------
xwalk_dt <- get_crosswalk_version(crosswalk_version_id, export=FALSE)
final_combined <- as.data.table(plyr::rbind.fill(xwalk_dt, final_dt))
final_combined[!is.na(crosswalk_parent_seq), seq := ""]

## This is the dataset with specified xwalk version and predicted EMR to be used for nonfatal modeling 
write.xlsx(final_combined, upload_datapath,  sheetName = "extraction")

### 5) Save crosswalk version -------------------------------------------------

# data_filepath <- upload_datapath
# description <- paste0("2020 iterative xwalk w/ predicted EMR prepared on ", date, " appended to xwalk version ID ", crosswalk_version_id) 

# result <- save_crosswalk_version(bundle_version_id=bundle_version_id, 
#                                  data_filepath=data_filepath, 
#                                  description=description)

# print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))


## Run a dismod model!! :)
