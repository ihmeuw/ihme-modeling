##################################################################################################################################### 
#The intention for this function is to create a dataframe that is the collapsed and merged data that will be used in crosswalks.
# It subsets out the reference data in a bundle and the non-reference data. It cuts these data depending on where the user wants
# them by age and year. The function returns a dataframe that is prepped for calculating the mean effect for the ratio in a crosswalk
# that can be used either in rma() or in MR-BRT.
# Written by USERNAME, DATE - modified by USERNAME
#
# 1. df is the dataframe (i.e. bundle data)
# 2. covariate_name is the name of the binary column for the crosswalk (i.e. cv_)
# 3. marketscan comparison so no matching by year
# 4. location_match is at what level do you want your data merged by location, meaning how close geographically is a match?
#         "exact" merges by location_id; "country" collapses subnationals into single value
#         "region" collapses regional data to single value; "super" collapses super regional data to single value
# 5. year_range is the number of years above or below year_start and year_end of the reference study that will be used for a comparison
######################################################################################################################################


library(plyr)
library(msm, lib.loc = "FILEPATH")
library(readxl)
library(stringr)
library(metafor, lib.loc = "FILEPATH")

rm(list=ls())


locs <- read.csv("FILEPATH")

dt <- as.data.table(read.csv("FILEPATH"))  

covariate_name <- "cv_marketscan"    #set covariate of interest for crosswalk (alt)
location_match <- "exact"
cause_path <- "COPD/"  
cause_name <- "COPD_" 
year_range <- 4
age_range <- 5

# Load date
date <- Sys.Date() #returns current date
date <- gsub("-", "_", date) #date is year_month_day instead of year-month-day

# Create Marketscan covariates 
dt$cv_marketscan_all_2000 <- ifelse((dt$clinical_data_type == "claims" & dt$year_start==2000), 1, 0)
dt$cv_marketscan <- ifelse((dt$clinical_data_type == "claims" & dt$location_name!= "Taiwan" & dt$cv_marketscan_all_2000!=1), 1, 0)

dt <- dt_all[dt_all$location_name=="Kentucky", ]

dt$is_alt <- dt$cv_marketscan
dt$is_reference <- ifelse(dt$cv_marketscan!=1,1,0)

# Create a working dataframe
wdt <- dt[,c("location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean", "cases", "standard_error","sample_size",
             "is_reference", "is_alt", "measure")]
covs <- select(dt, matches("^cv_"))
wdt <- cbind(wdt, covs)
wdt <- join(wdt, locs[,c("location_id","ihme_loc_id","super_region_name","region_name")], by="location_id")

# Create indicators for merging
if(location_match=="exact"){
  wdt$location_match <- wdt$location_id
} else if(location_match=="country"){
  wdt$location_match <- substr(wdt$ihme_loc_id,1,3)
} else if(location_match=="region"){
  wdt$location_match <- wdt$region_name
} else if(location_match=="super"){
  wdt$location_match <- wdt$super_region_name
} else{
  print("The location_match argument must be [exact, country, region, super]")
}

wdt$ihme_loc_abv <- substr(wdt$ihme_loc_id,1,3)

## Collapse to the desired merging
ref <- subset(wdt, is_reference == 1)
nref <- subset(wdt, is_alt == 1)

## Identify matches
setnames(nref, c("mean","standard_error","cases","sample_size", "year_start", "year_end", "age_start", "age_end"), 
         c("n_mean","n_standard_error","n_cases","n_sample_size", "n_year_start", "n_year_end", "n_age_start", "n_age_end"))
  
ref[, c("ihme_loc_id", "ihme_loc_abv", "location_id")] <- NULL
nref[, c("ihme_loc_id", "ihme_loc_abv", "location_id")] <- NULL
  
wmean <- merge(ref, nref, by=c("location_match", "sex"), allow.cartesian = T)
  
#Drop rows if the years are not within range 
wmean$year_lower <- abs(wmean$year_start - wmean$n_year_start)
wmean$year_upper <- abs(wmean$year_end - wmean$n_year_end)
wmean <- wmean[wmean$year_lower <= year_range & wmean$year_upper <= year_range, ] 

#Drop rows if the ages are not within range 
wmean$age_lower <- abs(wmean$age_start - wmean$n_age_start)
wmean$age_upper <- abs(wmean$age_end - wmean$n_age_end)
wmean <- wmean[wmean$age_lower <= age_range & wmean$age_upper <= age_range, ] 

## Drop duplicates
wmean <- unique(wmean)

## Get the ratio
# First get log ratio  
  wmean$ratio <- wmean$n_mean/wmean$mean
  wmean$se <- sqrt(wmean$n_mean^2 / wmean$mean^2 * (wmean$n_standard_error^2/wmean$n_mean^2 + wmean$standard_error^2/wmean$mean^2))
  wmean <- subset(wmean, se > 0)

  # Convert the ratio to log space
  wmean$log_ratio <- log(wmean$ratio)

  # Convert se to log space (from USERNAME)
  wmean$delta_log_se <- sapply(1:nrow(wmean), function(i) {
    ratio_i <- wmean[i, "ratio"]
    ratio_se_i <- wmean[i, "se"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })

# Second get logit difference
  setnames(wmean, "mean", "mean_ref")
  setnames(wmean, "n_mean", "mean_alt")
  
  wmean$se_logit_mean_alt <- sapply(1:nrow(wmean), function(i) {
    ratio_i <- wmean[i, mean_alt]
    ratio_se_i <- wmean[i, n_standard_error]
    deltamethod(~log(x1/(1-x1)), ratio_i, ratio_se_i^2)
  })
  
  
  wmean$se_logit_mean_ref <- sapply(1:nrow(wmean), function(a) {
    ratio_a <- wmean[a, mean_ref]
    ratio_se_a <- wmean[a, standard_error]
    deltamethod(~log(x1/(1-x1)), ratio_a, ratio_se_a^2)
  })
  
  wmean <- wmean %>% # creating the dataset for MR-BRT
    mutate(
      logit_mean_alt = logit(mean_alt),
      logit_mean_ref = logit(mean_ref),
      diff_logit = logit_mean_alt - logit_mean_ref,
      se_diff_logit = sqrt(se_logit_mean_alt^2 + se_logit_mean_ref^2)
    )

# Add study ID
wmean <- as.data.table(wmean)
wmean[, id := .GRP, by = c("nid.x", "nid.y")]
  
#Write output to csv 
write.csv(wmean, paste0("FILEPATH"), row.names = F)


#########################################################################################################################################
## Link with launching and loading an MR-BRT model ##

library(dplyr)
library(data.table)
library(metafor, lib.loc = "FILEPATH")
library(msm, lib.loc = "FILEPATH")
library(readxl)

repo_dir <- "FILEPATH"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))


## Plotting function ----------------------------------------------------------------

plot_mr_brt <- function(model_object, continuous_vars = NULL, dose_vars = NULL, print_cmd = FALSE) {
  dev <- FALSE
  if (dev) {
    model_object = readRDS("FILEPATH")
    continuous_vars = NULL
    dose_vars = NULL
    print_cmd = TRUE
  }
  wd <- model_object[["FILEPATH"]]
  contvars <- paste(continuous_vars, collapse = " ")
  dosevars <- paste(dose_vars, collapse = " ")
  if (!file.exists(paste0(wd, "FILEPATH"))) {
    stop(paste0("No model outputs found at '", wd, "'"))
  }
  contvars_string <- ifelse(
    !is.null(continuous_vars), paste0("--continuous_variables ", contvars), "" )
  dosevars_string <- ifelse(
    !is.null(dose_vars), paste0("--dose_variable ", dosevars), "" )
  cmd <- paste(
    c("export PATH='FILEPATH'",
      "source deactivate",
      "source FILEPATH",
      paste(
        "python FILEPATH",
        "--mr_dir", wd,
        contvars_string,
        dosevars_string
      )
    ), collapse = " && "
  )
  cat("To generate plots, run the following command in a qlogin session:")
  cat("\n", cmd, "\n\n")
  cat("Outputs will be available in:", wd)
}


# Verify model names 
model_name <- "cv_market_to_bold_2010"    #set covariate of interest for crosswalk (alt)
cause_path <- "COPD/"
cause_name <- "COPD_"


#Need to use/create column to specify if within study comparison or between

fit1 <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = paste0(cause_name, covariate_name, "logit"),
  data = paste0("FILEPATH"),
  mean_var = "diff_logit",
  se_var = "se_diff_logit",
  overwrite_previous = TRUE,
  remove_x_intercept = FALSE,
  method = "trim_maxL",
  trim_pct = 0.10
  )


plot_mr_brt(fit1)


## CREATE A RATIO PREDICTION FOR EACH OBSERVATION IN THE ORIGINAL DATA 
#########################################################################################################################################
# Prep original data
actual_data <- as.data.table(read.csv(paste0("FILEPATH")))
actual_data$cv_marketscan_all_2000 <- ifelse((actual_data$clinical_data_type=="claims" & actual_data$location_name!="Taiwan" & actual_data$year_start==2000),1,0)
actual_data$cv_marketscan <- ifelse((actual_data$clinical_data_type=="claims" & actual_data$location_name!="Taiwan"),1,0)
 
# Logit transform original data
 actual_data$mean_logit <- logit(actual_data$mean)
 actual_data$se_logit <- sapply(1:nrow(actual_data), function(i) {
   mean_i <- actual_data[i, mean]
   se_i <- actual_data[i, standard_error]
   deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
 })


## Predict MR-BRT --------------------------------------------------------------------------------------------------------------
# Check for outputs from model
check_for_outputs(fit1, wait_seconds = 15)

# Read raw outputs from model
results1 <- load_mr_brt_outputs(fit1)

names(results1)
coefs <- results1$model_coefs
metadata <- results1$input_metadata
train <- results1$train_data

df_pred <- data.table("cv_marketscan"=c(0, 1))

pred <- predict_mr_brt(fit1, newdata = df_pred, z_newdata = df_pred, write_draws = T)
check_for_preds(pred)
pred_object <- load_mr_brt_preds(pred)
predicted <- pred_object$model_summaries

predicted <- unique(predicted)
predicted <- predicted[1:1, ]

setnames(predicted, "X_intercept", "cv_marketscan")


##: APPLY RATIOS TO THE ORIGINAL DATA AND CREATE THE FINAL DATASET USED FOR NONFATAL MODELING 
#########################################################################################################################################
predicted <- as.data.table(predicted)

names(predicted) <- gsub("model_summaries.", "", names(predicted))
names(predicted) <- gsub("X_d_", "cv_", names(predicted))
predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
predicted[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]
crosswalk_reporting <- copy(predicted) # for reporting later

predicted[, (c("Z_intercept", "Y_negp", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe")) := NULL]
pred_0 <- data.table(cv_marketscan = 0, Y_mean = 0, Y_se =0, Y_se_norm=0)
predicted_new <- rbind(predicted, pred_0)

review_sheet_final <- merge(actual_data, predicted_new, by=c("cv_marketscan"))
review_sheet_final <-as.data.table(review_sheet_final)

setnames(review_sheet_final, "mean", "mean_orig")
review_sheet_final[Y_mean == predicted_new[1,Y_mean], `:=` (mean_logit = mean_logit - Y_mean, se_logit = sqrt(se_logit^2 + Y_se^2))]
review_sheet_final[Y_mean == predicted_new[1,Y_mean], `:=` (mean_new = inv.logit(mean_logit), standard_error_new = deltamethod(~exp(x1)/(1+exp(x1)), mean_logit, se_logit^2)), by = c("mean_logit", "se_logit")]
review_sheet_final[Y_mean == predicted_new[1,Y_mean], `:=` (lower_new = NA, upper_new = NA)]
review_sheet_final[Y_mean == predicted_new[2,Y_mean], `:=` (mean_new = mean_orig, standard_error_new = standard_error)]
review_sheet_final[, (c("Y_mean", "Y_se", "mean_logit", "se_logit")) := NULL]


# For upload validation #
review_sheet_final[is.na(lower), uncertainty_type_value := NA]

#THIS IS THE DATASET THAT WILL BE USED FOR NONFATAL MODELING
write.csv(review_sheet_final, paste0("FILEPATH"), row.names = F)


## Plot predictions vs original data ----------------------------------------------------------------------------------------------------------------------------------
library(ggplot2)
 
review_sheet_final$cv_marketscan <-as.character(review_sheet_final$cv_marketscan) 

gg_scatter <- ggplot(review_sheet_final, aes(x=mean_orig, y=mean_new, color=cv_marketscan)) + geom_point() +
  geom_abline(slope=1, intercept = 0) +
  xlim(0,1) + ylim(0,1) + coord_fixed() +
  ggtitle("Adjusted Means Vs Unadjusted Means") +
  theme_classic()

gg_scatter

# Boxplot by sex
boxplot(ratio~sex,data=wmean, main="Ratios for Marketscan", 
        xlab="Super Region", ylab="Ratio (Alt/Ref)")


# Boxplot by covariate
boxplot(mean~cv_not_rep_new,data=review_sheet_final, main="Original Data", 
        xlab="Population representative or not", ylab="Mean")

