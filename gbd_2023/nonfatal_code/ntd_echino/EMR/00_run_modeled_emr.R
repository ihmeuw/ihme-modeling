# CE - EMR using MR-BRT GBD2023

###############################################################################
## Purpose: EMR regression using MR BRT, GBD 2023
###############################################################################

rm(list=ls())

### Packages and directory set up ---------------------------------------------
if (Sys.info()["sysname"] == "Linux") {
FILEPATH
 FILEPATH
  FILEPATH
} else { 
  FILEPATH
  FILEPATH
  FILEPATH
}

pacman::p_load(data.table, ggplot2, tidyverse, stringr, DBI, openxlsx, gtools, gridExtra)
date <- Sys.Date()

## Source functions
functions_dir <- FILEPATH
repo_dir <- FILEPATH
emr_dir <- FILEPATH

source(paste0(repo_dir, FILEPATH))
shared_functs <- c(FILEPATH)
invisible(lapply(shared_functs, function(x) source(paste0(functions_dir, x))))

# This is where the main functions for modeled emr live
source(paste0(FILEPATH))
library(msm)
library(openxlsx)


### Set up: Fill these options in ---------------------------------------------

#Filepaths and variable names
home_dir <- FILEPATH
cause_name <- "ADDRESS" 
model_id <- ADDRESS  

#Upload specs
user_name <- Sys.info()['ADDRESS'] 
bundle_id <-ADDRESS
crosswalk_version_id <- ADDRESS
bundle_version_id <- ADDRESS 
upload_datapath <- ADDRESS
seq_emr <- ADDRESS

#MR-BRT decisions
cov_list <- "age_sex_haq"  
trim <- 0.1 

#Remove specific locations, inpatient and/or claims
remove_locations <- c() 
remove_inpatient = FALSE  
remove_marketscan = FALSE
remove_taiwan <- FALSE 
remove_singapore <- FALSE
remove_poland <- FALSE 
remove_russia <- FALSE 

# Prediction matrix decisions,
age_predict <- c(0,10,20,30,40,50,60,70,80,90,100)

location_level <- c(3)

### 1) Prep Data --------------------------------------------------------------

## Create date-specific output filepaths if they do not already exist
dir.create(file.path(ADDRESS), recursive = TRUE)
bundle_output_path <- paste0(ADDRESS)
date_output_path <- paste0(ADDRESS)
plot_output_path <- paste0(ADDRESS)

## Pull in EMR data, rename, and remove unneeded variables
dt <- as.data.table(get_emr_data(model_id)) %>%
  rename(nid = source_nid, underlying_nid = source_underlying_nid) %>%
  dplyr::select(-outlier_type_id, -version_id, -model_version_dismod_id)

## This function removes certain clinical and location data specified
dt <- remove_clinical_or_location_data(data = dt, locations_to_remove = remove_locations)

## Reformatting data for emr process
dt <- reformat_data_for_modeled_emr(data = dt)

## Custom function to merge haqi by year/location, output list of HAQ and data
dt <- merge_haqi_covariate(dt)
haq <- dt[[2]] 
dt <- dt[[1]]

# Plot distribution of HAQ for all locations
locs <- as.data.table(get_location_metadata(release_id = ADDRESS))

plot_input_haq_data(data=dt, plot_output_path = plot_output_path, haq_data = haq)

## Export results as csv file
write.csv(dt, paste0(date_output_path, ADDRESS), row.names = F)


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
  model_label = paste0(ADDRESS),
  data = paste0(FILEPATH)),
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

## Plot trimmed and used input data 
train_data <- fit1$train_data

# custom function to create map from emr input data
map_emr_input(data=train_data)

### 3) Run predictions and plot MR-BRT ----------------------------------------

loc_pull <- locs$location_id[locs$level == 3 | locs$level == 4]

pred_df <- expand.grid(year_id=c(1990, 1995, 2000, 2005, 2010, 2015, 2020, 2021, 2022, 2023), location_id = loc_pull, midage=age_predict, sex_binary=c(0,1))
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
ggsave(graph_emr, filename = paste0(ADDRESS), width = 10, height = 7)

graph_emr_log <- graph_mrbrt_log(results = fit1, predicts = predicted)
graph_emr_log
ggsave(graph_emr_log, filename = paste0(ADDRESS), height = 7, width = 10)


### 4) Apply ratios to the orginal data ---------------------------------------

final_dt <- as.data.table(final_emr)

# Function that will apply the ratios to the data and reformat for upload
final_dt <- apply_mrbrt_ratios_and_format(final_dt, seq_of_dummy_emr = seq_emr, extractor = user_name) 

### 5) Pull in crosswalk version of choice to append EMR data -----------------
xwalk_dt <- get_crosswalk_version(ADDRESS)
table(xwalk_dt$measure)
xwalk_dt <- subset(xwalk_dt, measure != "mtexcess")
xwalk_dt <- subset(xwalk_dt, select= -c(seq))

#getting original crosswalk_parent_set from the bundle
bundle_seq  <- get_bundle_version(ADDRESS ,fetch="all") 
bundle_seq <- subset(bundle_seq, measure != "mtexcess")
bundle_seq <- subset(bundle_seq, select = c(nid,  location_id, age_start, age_end, year_start, year_end, sex, seq ))
bundle_seq <- unique(bundle_seq, by=c("study_id" "location_id", "age_start", "age_end", "year_start", "year_end", "sex"))




xwalk_dt2 <- merge(x=xwalk_dt , y = bundle_seq, by=c("nid", "location_id", "age_start", "age_end", "year_start", "year_end", "sex"), all.x = T) 

xwalk_dt2[, crosswalk_parent_seq := seq]

final_combined <- as.data.table(plyr::rbind.fill(xwalk_dt2, final_dt))
final_combined[!is.na(crosswalk_parent_seq), seq := ""]

final_combined[,seq:=NULL]
final_combined[, seq := ""]

## This is the dataset with specified xwalk version and predicted EMR to be used for nonfatal modeling 
write.xlsx(final_combined, FILEPATH)

### 5) Save crosswalk version -------------------------------------------------

 data_filepath <- upload_datapath
 description <- paste0(FILEPATH) 

 result <- save_crosswalk_version(bundle_version_id=bundle_version_id, 
                                 data_filepath=data_filepath, 
                                description=description)

 print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
 
#######
#divide EMR by 3.5

 
 xwalk <- get_crosswalk_version(ADDRESS)
 xwalk_emr <- subset(xwalk, measure == "mtexcess")
 table(xwalk_emr$measure)
 summary(xwalk_emr$mean)

 summary(xwalk_emr$standard_error)
 
 xwalk_inc <- subset(xwalk, measure != "mtexcess")
 summary(xwalk_inc$mean)

 summary(xwalk_inc$standard_error)

 xwalk_emr[,mean := mean/3.5]
 xwalk_emr[,upper := upper/3.5]
 xwalk_emr[,lower := lower/3.5]
 xwalk_emr[,standard_error := standard_error/3.5]
 
 summary(xwalk_emr$mean)
 
 xwalk_new <- rbind(xwalk_inc, xwalk_emr)
 xwalk_new[,seq:=NULL]
 xwalk_new[, seq := ""]


 upload_datapath <- paste0("FILEPATH") 
 openxlsx::write.xlsx(xwalk_new, FILEPATH)
 
 data_filepath <- upload_datapath
 description <- paste0("ADDRESS") 
 
 result <- save_crosswalk_version(bundle_version_id=bundle_version_id, 
                                  data_filepath=data_filepath, 
                                  description=description)
 
