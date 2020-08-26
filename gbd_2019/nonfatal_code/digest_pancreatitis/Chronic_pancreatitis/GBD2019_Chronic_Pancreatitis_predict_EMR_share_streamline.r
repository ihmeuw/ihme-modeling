####################################################################################################################################
## Purpose: This script is for modeling EMR estimates using MR BRT 
####################################################################################################################################


## Set up
pacman::p_load(data.table, ggplot2, plyr, dplyr, stringr, DBI, openxlsx, gtools)
library(msm, lib.loc = "FILEPATH")
date <- gsub("-", "_", Sys.Date())
date <- Sys.Date()

functions_dir <- "FILEPATH"

source(paste0("FILEPATH/upload_bundle_data.R"))
source(paste0("FILEPATH/save_bundle_version.R"))
source(paste0("FILEPATH/get_bundle_version.R"))
source(paste0("FILEPATH/get_bundle_data.R"))

## FILL THESE IN
home_dir <- "FILEPATH" #location to save files
cause_path <- "EMR/" #location to save files for specific cause of interest
cause_name <- "DISEASE NAME" #cause name for saving csv file
#EMR data decisions
model_id <- "MODEL_ID"   #Dismod model version from which you want to pull EMR (should be step2 best or current step3 iteration if preferable)
remove_hospital = FALSE  #remove all hospital data?
remove_marketscan = FALSE #remove all claims data? (comment out line 100 to remove US Marketscan but retain Taiwan claims)
remove_taiwan <- FALSE #remove Taiwan claims?
remove_locations <- c() #fill in location_ids you want to exclude from EMR analysis separated by commas
#MR-BRT decisions
trim <- 0.10 #change this if you want to trim more/less in MR-BRT regression
cov_list <- paste0("age_sex_haq_", trim, "_maxageat100")  #covariates included in MR-BRT run (vary if you are trying different covariates, but probably this shouldn't change)
age_predict <- c(0,10,20,30,40,50,60,70,80,90,100) #ages you want to predict EMR - don't use too many or Dismod model won't run easily (aim for >35K predicted EMR, ideally 25K)
knot_number <- 3 #three is standard
spline_type <- 3 # set to three for cubic
spline_tail <- T #set to T for linear tails
knot_placement <- "NA" #"NA" if want evenly spaced knots, "frequency" if density
#Upload specs
uw_name <- "USERNAME" #for uploader validation
crosswalk_version_id <- "FILE_ID"  #this is the crosswalk version to which you want to append the EMR data (step2 best)
bundle_id <- "FILE_ID" 
bundle_version_id <-   #this is the bundle version to which you want to save your new crosswalk version after appending EMR - created in previous script!
upload_datapath <- "FILEPATH" #upload path for new crosswalk version
nid <-  "SOURCE_ID"



#Note: if you have trouble with graph_mr_brt (line 252), try commenting out lines 275-6 (annotation_custom) - 
#this adds in a table inset with beta coefficients, but can cause plot generation to crash


## Function to pull in EMR data from specified model version (using a read only view from clinical team) - note this requires DBI package
get_emr_data <- function(model_id){
  odbc <- ini::read.ini('FILEPATH') 
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
dt$cv_marketscan <- ifelse((dt$nid=="FILE_ID"), 1, 0)

inpatient_nids <- as.data.table(read.csv("FILEPATH")) 
inpatient_nids$cv_hospital <- 1
inpatient_nids <- inpatient_nids[, c("merged_nid", "cv_hospital")]
inpatient_nids <- unique(inpatient_nids)
setnames(inpatient_nids, "merged_nid", "nid")
dt <- merge(dt, inpatient_nids, by = "nid", all.x=TRUE)
dt$cv_hospital[is.na(dt$cv_hospital)] <- 0

dt$Taiwan <- ifelse(dt$nid==FILE_ID, 1, 0) #375914

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

## Add column for midpoint age for each row, and binary sex variable for each row (male=0, female=1)
dt$age_end[dt$age_end==124] <- 100
dt$midage <- (dt$age_start+dt$age_end)/2
dt$sex_binary <- ifelse(dt$sex_id== 2, 1,0) 
dt[standard_error / mean >1, `:=` (standard_error = mean, upper = NA, lower = NA)]


## Add column for haqi subsetted to location_id - use midyear for prevalence data to subset to haqi year_id
source(paste0("FILEPATH/get_covariate_estimates.R"))
haq <- as.data.table(get_covariate_estimates(
  covariate_id="DATASET_ID",
  gbd_round_id=6,
  decomp_step='step3'
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

## Convert the ratio (using EMR, which is CSMR/prevalence) to log space
dt$log_ratio <- log(dt$mean)

# Log standard error
dt$delta_log_se <- sapply(1:nrow(dt), function(i) {
  ratio_i <- dt[i, "mean"]
  ratio_se_i <- dt[i, "standard_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


## Export results as csv file
write.csv(dt, "FILEPATH", row.names = F)


## Link with launching a MR-BRT model ##
#########################################################################################################################################

repo_dir <- "FILEPATH"
source(paste0(repo_dir, "mr_brt_functions.R"))


## Run MR-BRT -------------------------------------------------------------------------------------------------

fit1 <- run_mr_brt(
  output_dir = paste0(home_dir, cause_path),
  model_label = paste0(cause_name, cov_list),
  data = dt,
  mean_var = "log_ratio",
  se_var = "delta_log_se",
  overwrite_previous = TRUE,
  remove_x_intercept = FALSE,
  method = "trim_maxL",
  study_id = "id",
  trim_pct = trim,
  covs = list(   
    cov_info("midage", "X", degree = spline_type, n_i_knots = knot_number, 
             r_linear = spline_tail, l_linear = spline_tail, knot_placement_procedure = knot_placement), 
    cov_info("sex_binary", "X"),
    cov_info("haqi_mean", "X", uprior_ub = 0)
  )
)
plot_mr_brt(fit1)

## Predict MR-BRT --------------------------------------------------------------------------------------------------------------

## Set up matrix for predictions for specified ages (age_predict), both sexes, all national locations, all Dismod years
locs <- read.csv("FILEPATH")
loc_pull <- unique(locs$location_id)

pred_df <- expand.grid(year_id=c(1990, 1995, 2000, 2005, 2010, 2015, 2017), location_id = locs$location_id[locs$level==3 | locs$level==4], midage=age_predict, sex_binary=c(0,1))
pred_df <- join(pred_df, haq2[,c("haqi_mean","location_id","year_id")], by=c("location_id","year_id"))

pred_df$age_start <- pred_df$midage
pred_df$age_end <- pred_df$midage


## Predict mr brt
pred <- predict_mr_brt(fit1, newdata = pred_df, write_draws = T)
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


## Graph EMR data and model fit over age by sex and a few HAQ
library(gridExtra)
library(Hmisc)

graph_mrbrt_results <- function(results, predicts){
  data_dt <- as.data.table(results$train_data)
  data_dt[w == 0, excluded := 1][w > 0, excluded := 0]
  model_dt <- as.data.table(predicts)
  quant_haq <- c(7.54 , 51.19, 94.34) 
  model_dt <- model_dt[, haqi_mean := round(haqi_mean, digits =2)]
  model_dt <- model_dt[model_dt$haqi_mean %in% quant_haq, ]
  model_dt$haqi_mean <- as.character(model_dt$haqi_mean)
  model_dt$sex <- ifelse(model_dt$sex_binary == 1, "Female", "Male")
  coefs <- fit1$model_coefs[, c("x_cov", "beta_soln", "beta_var", "gamma_soln")]
  coefs$gamma_soln <- coefs$gamma_soln[1]
  coefs$variance_with_gamma <- coefs$beta_var + coefs$gamma_soln^2
  coefs <- coefs[, c("x_cov", "beta_soln", "variance_with_gamma")]
  coefs[,c("beta_soln","variance_with_gamma")] = format(round(coefs[,c("beta_soln", "variance_with_gamma")],3))
  gg_subset<- ggplot() +
    geom_point(data = data_dt, aes(x = midage, y = exp(log_ratio), color = as.factor(excluded)))+
    scale_fill_manual(guide = F, values = c("midnightblue", "purple")) + 
    labs(x = "Age", y = "EMR") + 
    ggtitle(paste0("MR-BRT Model Results Overlay on EMR Input")) +
    theme_classic() +
    theme(text = element_text(size = 15, color = "black")) +
    geom_line(data = model_dt, aes(x = midage, y = exp(Y_mean), color = haqi_mean, linetype = sex), size=1.5) +
    scale_color_manual(name = "", values = c("blue", "red", "purple", "black", "orange"), 
                       labels = c("Included EMR", "Trimmed EMR", "50th Percentile HAQ", "Min HAQ", "Max HAQ")) +
    annotation_custom(grob = tableGrob(coefs, rows = NULL), 
                      xmin=15, xmax=30, ymin=0.3)
  return(gg_subset)
}

graph_emr <- graph_mrbrt_results(results = fit1, predicts = predicted)
graph_emr
ggsave(graph_emr, filename = "FILEPATH", width = 12, height = 10)




## APPLY RATIOS TO THE ORIGINAL DATA AND CREATE THE FINAL DATASET USED FOR NONFATAL MODELING
#########################################################################################################################################

## Add se variable to predictions
final_emr <- as.data.table(final_emr)
final_emr[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]

## Convert to normal space from log space
final_dt <-as.data.table(final_emr)
final_dt[, `:=` (mean = exp(Y_mean), standard_error = deltamethod(~exp(x1), Y_mean, Y_se^2)), by = c("Y_mean", "Y_se")]

## For uploader validation - add in needed columns
final_dt$seq <- NA
final_dt$sex <- ifelse(final_dt$sex_binary == 1, "Female", "Male")
final_dt$year_start <- final_dt$year_id
final_dt$year_end <- final_dt$year_id
final_dt[,c("sex_binary", "year_id", "haqi_mean", "log_ratio", "delta_log_se", "midage", "Y_mean", "Y_se", "Y_mean_lo", "Y_mean_hi")] <- NULL
final_dt <- join(final_dt, locs[,c("location_id", "location_name")], by="location_id")
final_dt$nid <- nid
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
final_dt$uncertainty_type <- "Confidence interval"
final_dt$recall_type <- "Point"
final_dt$urbanicity_type <- "Mixed/both"
final_dt$unit_type <- "Person"
final_dt$representative_name <- "Nationally and subnationally representative"
final_dt$note_modeler <- "These data are modeled from CSMR and prevalence using HAQI as a predictor"
final_dt$unit_value_as_published <- 1
final_dt$step2_location_year <- "This is modeled EMR that uses the NID for one dummy row of EMR data added to the bundle data"




## Pull in step2 crosswalk version to append EMR data
source("FILEPATH/get_crosswalk_version.R")
xwalk_dt <- get_crosswalk_version(crosswalk_version_id, export=FALSE)

## Set seq correctly for step2 crosswalk version using bundle data for step3 bundle
# First, get_bundle_data
bundle_data <- get_bundle_data(bundle_id, decomp_step = 'step3', export=FALSE) 

#Next, clear seq column in crosswalk version, and re-name crosswalk_parent_seq to step2_seq
xwalk_dt2 <- as.data.table(xwalk_dt)
xwalk_dt2[, "seq"] <- NULL
setnames(xwalk_dt2, "crosswalk_parent_seq", "step2_seq")

xwalk_dt2 <- merge(xwalk_dt2, bundle_data[, c("step2_seq", "seq")], by = "step2_seq")
xwalk_dt2[, "step2_seq"] <- NULL
final_combined <- as.data.table(rbind.fill(xwalk_dt2, final_dt))
setnames(final_combined, "seq", "crosswalk_parent_seq")
final_combined$seq <- ''

# Sometimes clinical data has SE greater than 1, which will cause uploader to fail, so blank these out
final_combined[standard_error >1, standard_error  := ""]

## THIS IS THE EMR DATASET THAT WILL BE USED FOR NONFATAL MODELING WITH DISABLED EMR (appended to crosswalk data)
write.xlsx(final_combined, upload_datapath,  sheetName = "extraction")

## Save a crosswalk version
source("FILEPATH/save_crosswalk_version.R")

data_filepath <- upload_datapath
description <- '2019 step2 crosswalk with predicted EMR - used for step3 modeling'

result <- save_crosswalk_version(bundle_version_id=bundle_version_id, data_filepath=data_filepath, description=description)


