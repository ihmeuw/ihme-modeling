####################################################################################################################################
## Purpose: Diarrhea EMR regression using MR BRT
####################################################################################################################################


## Set up
rm(list=ls())

  j_root <- "filepath"
  h_root <- "filepath"
  l_root <- "filepath"

pacman::p_load(data.table, ggplot2, plyr, dplyr, stringr, DBI, openxlsx, gtools)
library(msm)
date <- gsub("-", "_", Sys.Date())
date <- Sys.Date()

functions_dir <- "filepath"

## FILL THESE IN
#Filepaths
home_dir <- "filepath" #location to save files
cause_path <- "EMR/" #location to save files for specific cause of interest
cause_name <- "Diarrhea_" #cause name for saving csv file
#EMR data decisions
model_id <- 410552   #Dismod model from which you want to pull EMR (should be step2 best or current step3 iteration if preferable)
remove_hospital = TRUE  #remove all hospital data?
remove_marketscan = TRUE #remove all claims data?
remove_locations <- c() #fill in location_ids you want to exclude from EMR analysis separated by commas
#MR-BRT decisions
cov_list <- "age_sex_haq"  #covariates included in MR-BRT run 
age_predict <- c(0,1,5,10,20,30,40,50,60,70,80,90,100) #ages you want to predict EMR 
trim <- 0.1 #change this if you want to trim more/less in MR-BRT regression
knot_number <- 2 #three is standard
spline_type <- 3 # set to three for cubic
spline_tail <- T #set to T for linear tails
knot_placement <- "frequency" #"NA" if want evenly spaced knots, "frequency" if density
#Upload specs
uw_name <- "username" #for uploader validation

## Function to pull in EMR data from specified model version (using a read only view from clinical team) - note this requires DBI package
get_emr_data <- function(model_id){
  odbc <- ini::read.ini('/filepath/.odbc.ini')
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

inpatient_nids <- as.data.table(read.csv("filepath")) #read in hospital NIDs (merged_nid column)
inpatient_nids$cv_hospital <- 1
inpatient_nids <- inpatient_nids[, c("merged_nid", "cv_hospital")]
inpatient_nids <- unique(inpatient_nids)
setnames(inpatient_nids, "merged_nid", "nid")
dt <- merge(dt, inpatient_nids, by = "nid", all.x=TRUE)
dt$cv_hospital[is.na(dt$cv_hospital)] <- 0

# Remove all marketscan US subnational data and Taiwan claims data if desired
if(remove_marketscan == TRUE){
  dt <- dt[cv_marketscan == 0, ]
  dt$Taiwan <- ifelse(dt$nid==375914, 1, 0)
  dt <- dt[Taiwan == 0, ] #comment out if you want to retain Taiwan claims
  print("Claims data removed")
}else{
  print("Claims data retained or not present in bundle")
}
# Remove all marketscan US subnational data and Taiwan claims data if desired
if(remove_hospital == TRUE){
  dt <- dt[cv_hospital!=1]
  print("Hospital data removed")
}else{
  print("Hospital data retained or not present in bundle")
}

dt[, c("cv_marketscan", "cv_hospital", "Taiwan")] <- NULL

## Add column for midpoint age for each row, and binary sex variable for each row (male=0, female=1)
dt$midage <- (dt$age_start+dt$age_end)/2
dt$sex_binary <- ifelse(dt$sex_id== 2, 1,0)


## Add column for haqi subsetted to location_id - use midyear for prevalence data to subset to haqi year_id
source("/filepath/get_covariate_estimates.R")
haq <- as.data.table(get_covariate_estimates(
  covariate_id=1099,
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
dt[, id := .GRP, by = c("nid",  "location_id")]


## Convert the ratio (using EMR, which is CSMR/prevalence) to log space
dt$log_ratio <- log(dt$mean)


# Log standard error
dt$delta_log_se <- sapply(1:nrow(dt), function(i) {
  ratio_i <- dt[i, "mean"]
  ratio_se_i <- dt[i, "standard_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})


## Export results as csv file
write.csv(dt, "filepath", row.names = F)


## Link with launching a MR-BRT model ##
#########################################################################################################################################

repo_dir <- "filepath"
source(paste0(repo_dir, "mr_brt_functions.R"))


## Run MR-BRT -------------------------------------------------------------------------------------------------

fit1 <- run_mr_brt(
  output_dir = paste0(home_dir, cause_path),
  model_label = paste0(cause_name, cov_list),
  data = paste0(home_dir, cause_path, cause_name, "emr_for_mr_brt", date, ".csv"),
  mean_var = "log_ratio",
  se_var = "delta_log_se",
  overwrite_previous = TRUE,
  remove_x_intercept = FALSE,
  method = "trim_maxL",
  study_id = "nid",
  trim_pct = trim,
  covs = list(
    cov_info("midage", "X", degree = spline_type, n_i_knots = knot_number,
             r_linear = spline_tail, l_linear = spline_tail, knot_placement_procedure = knot_placement),
    #cov_info("midage", "X", degree = 2, n_i_knots= 3),
    cov_info("sex_binary", "X"),
    cov_info("haqi_mean", "X")
  )
)


## Predict MR-BRT --------------------------------------------------------------------------------------------------------------

## Set up matrix for predictions
age_info <- read.csv("filepath")
age_info <- subset(age_info, age_pull == 1)
age_pull <- age_info$age_group_id

locs <- read.csv("filepath")
loc_pull <- unique(locs$location_id)

pred_df <- expand.grid(year_id=c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019), 
                       location_id = locs$location_id[locs$level==3], 
                       age_group_id=age_pull, sex_binary=c(0,1))
pred_df <- join(pred_df, haq2[,c("haqi_mean","location_id","year_id")], by=c("location_id","year_id"))
pred_df <- join(pred_df, age_info[,c("age_start","age_end","age_group_id","order")], by="age_group_id")
pred_df$midage <- (pred_df$age_end + pred_df$age_start) / 2


## Only model for ages every 10 years plus age 95...
pred_df2 <- pred_df
pred_df2$age_end <- pred_df2$age_start
pred_df2$midage <- pred_df2$age_start
pred_df2<- pred_df2%>% filter(age_start %in% c(age_predict))


## Predict mr brt
pred <- predict_mr_brt(fit1, newdata = pred_df2)
check_for_preds(pred)
pred_object <- load_mr_brt_preds(pred)
predicted <- pred_object$model_summaries

setnames(predicted, "X_sex_binary", "sex_binary")
setnames(predicted, "X_midage", "midage")
setnames(predicted, "X_haqi_mean", "haqi_mean")

predicted[, c("X_intercept", "Z_intercept")] <- NULL

final_emr <- cbind(predicted, pred_df2)
final_emr <- final_emr[, !duplicated(colnames(final_emr))]
final_emr[, c("Y_negp", "Y_mean_fe", "Y_mean_lo_fe", "Y_mean_hi_fe", "Y_negp_fe", "order", "midage", "haqi_mean", "age_group_id")] <- NULL


## Graph EMR data and model fit over age by sex and a few HAQ
graph_mrbrt_results <- function(results, predicts){
  data_dt <- as.data.table(results$train_data)
  data_dt[w == 0, excluded := 1][w > 0, excluded := 0]
  model_dt <- as.data.table(predicts)
  #quant_haq <- round(quantile(model_dt$haqi_mean, probs = seq(0,1, by = 0.25)), digits = 2)
  quant_haq <- c(7.54 , 51.19, 94.34)
  model_dt <- model_dt[, haqi_mean := round(haqi_mean, digits =2)]
  model_dt <- model_dt[model_dt$haqi_mean %in% quant_haq, ]
  model_dt$haqi_mean <- as.character(model_dt$haqi_mean)
  model_dt$sex <- ifelse(model_dt$sex_binary == 1, "Female", "Male")
  gg_subset<- ggplot() +
    geom_point(data = data_dt, aes(x = midage, y = exp(log_ratio), color = as.factor(excluded)))+
    scale_fill_manual(guide = F, values = c("midnightblue", "purple")) +
    labs(x = "Age", y = "EMR") +
    #scale_y_log10("EMR") +
    #scale_y_continuous("EMR", limits=c(0,150)) + xlab("Age") +
    ggtitle(paste0("MR-BRT Model Results Overlay on EMR Input")) +
    theme_classic() +
    theme(text = element_text(size = 15, color = "black")) +
    geom_line(data = model_dt, aes(x = midage, y = exp(Y_mean), color = haqi_mean, linetype = sex), size=1.5) +
    #geom_ribbon(data = model_dt, aes(x = midage, ymin = Y_mean_lo, ymax = Y_mean_hi), alpha = 0.05) +
    scale_color_manual(name = "", values = c("blue", "red", "purple", "black", "orange"),
                       labels = c("Included EMR", "Trimmed EMR", "50th Percentile HAQ", "Min HAQ", "Max HAQ"))
  return(gg_subset)
}

graph_emr <- graph_mrbrt_results(results = fit1, predicts = predicted)
graph_emr
ggsave(graph_emr, filename = paste0(home_dir, cause_path, cause_name, "fit_graphs_normal.pdf"), width = 12)

ggplot(predicted, aes(x=haqi_mean, y=exp(Y_mean), lty=factor(sex_binary))) + geom_line() + facet_wrap(~midage, scales="free") + theme_bw() +
  ggtitle(paste0("Facets are age at 10 yr intervals"))

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
final_dt$nid <- 270581  #This is the cause specific mortality NID for GBD 2017
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

## Need to get data used in Step 3 best ##
source("/filepath/get_crosswalk_version.R")
step3_data <- get_crosswalk_version(crosswalk_version_id=6857)
  write.xlsx(step3_data, "filepath", sheetName="extraction")

all_data <- rbind.fill(step3_data, final_dt)

all_data[is.na(all_data)] <- ""
all_data$crosswalk_parent_seq <- ""
all_data$unit_value_as_published <- 1

library(openxlsx)

write.xlsx(all_data, "filepath", sheetName="extraction")

source("/filepath/save_crosswalk_version.R")
source("/filepath/save_bundle_version.R")
source("/filepath/upload_bundle_data.R")

####################################################################
## Use this to upload to existing bundle/ME, pretenting they are new
## data for step 4.
####################################################################
write.xlsx(final_dt, "filepath", sheetName="extraction")

upload_bundle_data(bundle_id = 3, decomp_step = "step4", filepath = "filepath")
s4bv <- save_bundle_version(bundle_id = 3, decomp_step = "step4")

df_s4 <- get_bundle_version(bundle_version_id = s4bv$bundle_version_id)
df_s4$crosswalk_parent_seq <- df_s4$seq
df_s4$seq <- ""
df_s4 <- subset(df_s4, sex != "Both")

write.xlsx(df_s4, "filepath", sheetName="extraction")

print(s4bv$bundle_version_id)

save_crosswalk_version(bundle_version_id = 13367, data_filepath = "filepath",
                       description = "New data for step 4, modeled EMR")

######################################################################
## Use this to upload to new bundle/ME ##
######################################################################
## We need to use the new bundle and ME ##
new_bundle <- 6872

upload_bundle_data(bundle_id = new_bundle, decomp_step="step3", filepath = "filepath")
  save_bundle_version(bundle_id=new_bundle, decomp_step="step3")

data <- get_bundle_data(bundle_id=6872, export = F, decomp_step="step3")
data$crosswalk_parent_seq <- ""

write.xlsx(data, "filepath", sheetName="extraction")

save_crosswalk_version(bundle_version_id=11804, data_filepath = "filepath",
                       description = "New bundle: Step3 EMR modeling, MR-BRT using DisMod EMR data from Step3")

