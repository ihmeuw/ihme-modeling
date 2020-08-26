##############################################################
## Purpose: EMR regression using MR BRT 
##############################################################

## Set up
rm(list=ls())

pacman::p_load(data.table, ggplot2, plyr, dplyr, stringr, DBI, openxlsx, gtools)
library(msm)

functions_dir <- FILEPATH


## FILL THESE IN
#Filepaths
home_dir <- FILEPATH
cause_path <- FILEPATH
cause_name <- FILEPATH
#EMR data decisions
model_id <- OBJECT
remove_hospital = FALSE  
remove_marketscan = FALSE 
cov_list <- "age_sex_haq"  #covariates included in MR-BRT run 
trim <- 0.3 
knot_number <- 3 #three is standard
spline_type <- 3 # set to three for cubic
spline_tail <- T #set to T for linear tails
bspline_gprior_mean = "0, 0, 0, 0" #needs to be one more 0 than number of knots - this is for non-informative prior
bspline_gprior_var = "inf, inf, inf, inf" #needs to be one more "inf" than number of knots - this is for non-informative prior
knot_placement <- "NA" #"NA" if want evenly spaced knots, "frequency" if density
#Prediction matrix decisions
age_predict <- c(0,10,20,30,40,50,60,70,80,90,100) 
location_level <- c(3,4) # 3 = national only, c(3,4) = national and subnational
#Upload specs
crosswalk_version_id <- OBJECT
bundle_id <- OBJECT
bundle_version_id <- OBJECT
upload_datapath <- FILEPATH
seq_emr <- OBJECT

get_emr_data <- function(model_id){
  odbc <- ini::read.ini(FILEPATH) 
  con_def <- 'clinical_dbview'
  myconn <- RMySQL::dbConnect(RMySQL::MySQL(),
                              host = odbc[[con_def]]$SERVER,
                              username = odbc[[con_def]]$USER,
                              password = odbc[[con_def]]$PASSWORD)
  
  df <- dbGetQuery(myconn, sprintf(paste0("SELECT * FROM epi.t3_model_version_emr where model_version_id = ",
                                          model_id)))
  
  dbDisconnect(myconn)
  return(df)
}


## Pull in EMR data
dt <- get_emr_data(model_id)
dt <- as.data.table(dt)


## Drop unnecessary columns, change names to match extraction sheet
dt[, c("outlier_type_id", "model_version_id", "model_version_dismod_id")] <- NULL
setnames(dt, "source_nid", "nid")
setnames(dt, "source_underlying_nid", "underlying_nid")


## Drop locations if desired
dt <- dt[ ! dt$location_id %in% remove_locations, ]


## Remove marketscan data and/or hospital data, or keep as is
# First, create binary variables for hospital and clinical data based on NID
dt$cv_marketscan <- ifelse((dt$nid==244369 | dt$nid==244370 | dt$nid==244371 | dt$nid==336203 | dt$nid==336848 | dt$nid==336849 | dt$nid==336850), 1, 0)

inpatient_nids <- as.data.table(read.csv(FILEPATH))
inpatient_nids$cv_hospital <- 1
inpatient_nids <- inpatient_nids[, c("merged_nid", "cv_hospital")]
inpatient_nids <- unique(inpatient_nids)
setnames(inpatient_nids, "merged_nid", "nid")
dt <- merge(dt, inpatient_nids, by = "nid", all.x=TRUE)
dt$cv_hospital[is.na(dt$cv_hospital)] <- 0

dt$Taiwan <- ifelse(dt$nid==336203, 1, 0) #375914

# Remove all marketscan US subnational data if desired
if(remove_marketscan == TRUE){
  dt <- dt[cv_marketscan == 0, ]
  print("Marketscan data removed")
}else if(remove_taiwan == TRUE){  
  dt <- dt[Taiwan == 0, ] 
  print("Taiwan claims data removed")
}else{
  print("Claims data retained or not present in bundle")
}

# Remove all hospital data if desired
if(remove_hospital == TRUE){
  dt <- dt[cv_hospital!=1]
  print("Hospital data removed")
}else{
  print("Hospital data retained or not present in bundle")
}

dt[, c("cv_marketscan", "cv_hospital", "Taiwan")] <- NULL


## Cap age_end at 100
dt[age_end >100, age_end := 100]


## Add column for midpoint age for each row, and binary sex variable for each row (male=0, female=1)
dt$midage <- (dt$age_start+dt$age_end)/2
dt$sex_binary <- ifelse(dt$sex_id== 2, 1,0) 


## Add column for haqi subsetted to location_id - use midyear for prevalence data to subset to haqi year_id
source("FILEPATH/get_covariate_estimates.R")
haq <- as.data.table(get_covariate_estimates(
  covariate_id=1099,
  gbd_round_id=6,
  decomp_step='step4'
))

haq2 <- haq[, c("location_id", "year_id", "mean_value", "lower_value", "upper_value")]
setnames(haq2, "mean_value", "haqi_mean")
setnames(haq2, "lower_value", "haqi_lower")
setnames(haq2, "upper_value", "haqi_upper")

dt$year_id <- round((dt$year_start+dt$year_end)/2)

dt[,index:=.I]
n <- nrow(dt)
dt <- merge(dt,haq2, by=c("location_id","year_id"))
dt[, c("index", "haqi_lower", "haqi_upper", "year_id")] <- NULL

if(nrow(dt)!=n){
  print("Warning! Issues in merge of HAQI")
}

# Add study ID to include random effects to MR-BRT model
dt[, id := .GRP, by = c("nid", "location_id")]

# Create dataframe of location hierarchy
locs <- read.csv(FILEPATH)


# Plot distribution of HAQ for all locations
haq3 <- haq2[with(haq2, order(haqi_mean)), ]
haq3 <- haq3[haq3$year_id==2019, ]
haq3[,index:=.I]
haq3 <- as.data.table(haq3)
locs <- as.data.table(locs)
haq3 <- merge(haq3, locs[,c("location_id", "super_region_name")], by = "location_id")

## Convert the ratio (using EMR, which is CSMR/prevalence) to log space
dt$log_ratio <- log(dt$mean)

# Log standard error
dt$delta_log_se <- sapply(1:nrow(dt), function(i) {
  ratio_i <- dt[i, "mean"]
  ratio_se_i <- dt[i, "standard_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

## Export results as csv file
write.csv(dt, FILEPATH, row.names = F)


## Link with launching a MR-BRT model ##
#########################################################################################################################################

repo_dir <- FILEPATH
source(paste0(repo_dir, "mr_brt_functions.R"))


## Run MR-BRT -------------------------------------------------------------------------------------------------

fit1 <- run_mr_brt(
  output_dir = FILEPATH,
  model_label = paste0(cause_name, cov_list, date, "_non_inform_spline"),
  data = paste0(home_dir, cause_path, cause_name, "emr_for_mr_brt", date, ".csv"),
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
    cov_info("haqi_mean", "X", uprior_ub = 0) #include lower prior as well!
  )
)


## Predict MR-BRT --------------------------------------------------------------------------------------------------------------

## Set up matrix for predictions for specified ages (age_predict), both sexes, all national locations, all Dismod years
loc_pull <- unique(locs$location_id)

pred_df <- expand.grid(year_id=c(1990, 1995, 2000, 2005, 2010, 2015, 2017), location_id = locs$location_id[locs$level==location_level], midage=age_predict, sex_binary=c(0,1))
pred_df <- join(pred_df, haq2[,c("haqi_mean","location_id","year_id")], by=c("location_id","year_id"))

pred_df$age_start <- pred_df$midage
pred_df$age_end <- pred_df$midage


## Predict mr brt
pred <- predict_mr_brt(fit1, newdata = pred_df, write_draws = F)
check_for_preds(pred)
pred_object <- load_mr_brt_preds(pred)
predicted <- pred_object$model_summaries

setnames(predicted, "X_sex_binary", "sex_binary")
setnames(predicted, "X_midage", "midage")
setnames(predicted, "X_haqi_mean", "haqi_mean")

predicted[, c("X_intercept", "Z_intercept")] <- NULL

final_emr <- cbind(predicted, pred_df)
final_emr <- final_emr[, !duplicated(colnames(final_emr))]
final_emr[, c("Y_negp", "Y_mean_fe", "Y_mean_lo_fe", "Y_mean_hi_fe", "Y_negp_fe", "midage", "haqi_mean")] <- NULL


## APPLY RATIOS TO THE ORIGINAL DATA AND CREATE THE FINAL DATASET USED FOR NONFATAL MODELING
#########################################################################################################################################

## Add se variable to predictions
final_emr <- as.data.table(final_emr)
final_emr[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]

## Convert to normal space from log space
final_dt <-as.data.table(final_emr)
final_dt[, `:=` (mean = exp(Y_mean), standard_error = deltamethod(~exp(x1), Y_mean, Y_se^2)), by = c("Y_mean", "Y_se")]

## For uploader validation - add in needed columns
final_dt$seq <- seq_emr
final_dt$sex <- ifelse(final_dt$sex_binary == 1, "Female", "Male")
final_dt$year_start <- final_dt$year_id
final_dt$year_end <- final_dt$year_id
final_dt[,c("sex_binary", "year_id", "haqi_mean", "log_ratio", "delta_log_se", "midage", "Y_mean", "Y_se", "Y_mean_lo", "Y_mean_hi")] <- NULL
final_dt <- join(final_dt, locs[,c("location_id", "location_name")], by="location_id")
final_dt$nid <- 416752
final_dt$year_issue <- 0
final_dt$age_issue <- 0
final_dt$sex_issue <- 0
final_dt$measure <- "mtexcess"
final_dt$source_type <- "Facility - inpatient"
final_dt$extractor <- uw_name
final_dt$is_outlier <- 0
final_dt$sample_size <- 1000
final_dt$cases <- final_dt$sample_size * final_dt$mean
final_dt$measure_adjustment <- 1
final_dt$uncertainty_type <- ""
final_dt$recall_type <- "Point"
final_dt$urbanicity_type <- "Mixed/both"
final_dt$unit_type <- "Person"
final_dt$representative_name <- "Nationally and subnationally representative"
final_dt$note_modeler <- "These data are modeled from CSMR and prevalence using HAQI as a predictor"
final_dt$unit_value_as_published <- 1
final_dt$step2_location_year <- "This is modeled EMR that uses the NID for one dummy row of EMR data added to the bundle data"