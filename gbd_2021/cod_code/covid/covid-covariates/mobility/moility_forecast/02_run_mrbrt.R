## Purpose: Estimate mandate effects on mobility using MR-BRT. 
##          Forecast mobility using mandate forecasts.
## --------------------------------------------------------------

# load libraries
library(dplyr)
library(data.table)
library(tidyr)
if (R.Version()$major == "3") {
  library(slime, lib.loc = "FILEPATH")
} else if (R.Version()$major == "4") {
  library(slime, lib.loc = "FILEPATH")
}
library(forecast)
library(ggplot2)
library(RColorBrewer)
library('ihme.covid', lib.loc = '/ihme/covid-19/.r-packages/current')
R.utils::sourceDirectory('functions', modifiedOnly = FALSE)

# Some constants we don't expect to change
WORK_ROOT <-  "FILEPATH"
USE_RE = TRUE # whether to allow random coeffs of the SD mandates
RE_VAR <- 0.001 # set the variance of the random coefs
china_no_relax <- F

################
## Setting up ##
################

# set output dir
work_dir <- ihme.covid::get_latest_output_dir(WORK_ROOT)

if (interactive()) {
  WORSE_SCENARIO <- FALSE # set to TRUE if you want to generate worse scenario mobility forecasts
  RELAX_RESID <- TRUE # leave as TRUE / set to TRUE if you want to relax the residual and anticipate effects to 0 by 12/01/21
  sd_pred_version <- "latest"  #"best" or a YYYY_MM_DD.VV version
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument("--WORSE_SCENARIO", type="character", choices = c("TRUE", "FALSE"), help="'TRUE' for a worse scenario, 'FALSE' for reference scenario")
  parser$add_argument("--RELAX_RESID", type="character", choices = c("TRUE", "FALSE"), help="'TRUE' for relaxing residual and anticipate coef to 0 by 12/1/21, 'FALSE' otherwise")
  parser$add_argument("--sd_pred_version", type="character", help="Version of mandate forecasts to inform the mobility projections; defaults to the most recent (ie jobmon's) folder")

  args <- parser$parse_args(get_args())
  for (key in names(args)) {
    assign(key, args[[key]])
  }
  
  print("Arguments passed were:")
  for (arg in names(args)){
    print(paste0(arg, ": ", get(arg)))
  }

  # Update variables
  RELAX_RESID = as.logical(RELAX_RESID)
  WORSE_SCENARIO = as.logical(WORSE_SCENARIO)
  if (sd_pred_version == "jobmon") {
    sd_pred_version <- basename(ihme.covid::get_latest_output_dir(file.path(WORK_ROOT, "mandates")))
  }
}

# Diagnostics
ihme.covid::print_debug(work_dir, RE_VAR)

# Define file paths
SMOOTHED_MOBILITY_DATA_PATH <- file.path(work_dir, "smooth_mobility_with_sd.csv")
SOCIAL_DISTANCING_DATA_PATH <- file.path(work_dir, 'social_distancing.csv')
SMOOTHED_ARIMA_PLOT_PATH <- file.path(work_dir, "smooth_arima_residual.pdf")
MOBILITY_FORECAST_PATH <- file.path(work_dir, 'mobility_forecast_full.csv')
MOBILITY_FORECAST_SMOOTHED_PATH <- file.path(work_dir, 'mobility_forecast_smooth_metric_lift.csv')
MRBRT_SOCIAL_DISTANCING_RE_PATH <- file.path(work_dir, 'mobility_mandate_coefficients.csv')
HIERARCHY_PATH <- file.path(work_dir, 'hierarchy.csv')
VACC_PATH <- file.path(WORK_ROOT, "mandates", sd_pred_version, "vaccine_coverage.csv")
LIFT_PRED_DATA_PATH <- file.path(WORK_ROOT, "mandates", sd_pred_version, "results_for_mobility.csv")
metadata_path <- file.path(work_dir, "02_run_mrbrt.metadata.yaml")

################
## Load Data  ##
################

# location hierarchy
hierarchy <- fread(HIERARCHY_PATH)

# read in the mobility metrics with SD covariates
df_smooth <- fread(SMOOTHED_MOBILITY_DATA_PATH)
df_smooth[, date := as.Date(date)]

# read in the SD dataset which is saved in the previous step
df_sd <- fread(SOCIAL_DISTANCING_DATA_PATH)

# read in sd preds to get locations
df_pred_sd <- fread(LIFT_PRED_DATA_PATH)

# read in vaccine coverage data (for residual relaxation)
vacc_df <- fread(VACC_PATH)
vacc_df <- vacc_df[, date:=as.Date(date)]

##########################################
## Run mr-brt model using slime package ##
##########################################

# leave observed smoothed metric only and create SE variable
df_smooth_mrt <- df_smooth[!is.na(mean)]
df_smooth_mrt[, gpr_se := sqrt(var(mean)/nrow(df_smooth_mrt))]

locs <- intersect(df_smooth_mrt[, location_id], df_pred_sd[, location_id])

# define the data object
data_mrbrt <- MRData(
  df = df_smooth_mrt,
  col_group = 'location_id',
  col_obs = 'mean',
  col_obs_se = 'gpr_se',
  col_covs = c('anticipate', 'sd1_lift', 'sd2_lift', 'sd3_lift' ,'psd1_lift', 'psd3_lift')
)

# define covariates model

# adding priors and bound for the total effects, no restriction on the random effects
linear_cov_model_sd1 <- CovModel('sd1_lift',
                                 use_re = USE_RE, re_var = RE_VAR, bounds = array(c(-Inf, 0))
)
linear_cov_model_sd2 <- CovModel('sd2_lift',
                                 use_re = USE_RE, re_var = RE_VAR, bounds = array(c(-Inf, 0))
)
linear_cov_model_sd3 <- CovModel('sd3_lift',
                                 use_re = USE_RE, re_var = RE_VAR, bounds = array(c(-Inf, 0))
)
linear_cov_model_psd1 <- CovModel('psd1_lift',
                                  use_re = USE_RE, re_var = RE_VAR, bounds = array(c(-Inf, 0))
)
linear_cov_model_psd3 <- CovModel('psd3_lift',
                                  use_re = USE_RE, re_var = RE_VAR, bounds = array(c(-Inf, 0))
)
linear_cov_model_anticipate <- CovModel('anticipate',
                                        use_re = USE_RE, re_var = RE_VAR, bounds = array(c(-Inf, 0))
)

# compile the models
cov_models = CovModelSet(list(linear_cov_model_sd1, linear_cov_model_sd2, linear_cov_model_sd3, linear_cov_model_psd1,
                              linear_cov_model_psd3, linear_cov_model_anticipate))

# run the models 
model = MRModel(data_mrbrt, cov_models)
model$fit_model()

# save the results (effects of SD mandates by location) from the regression
results <- model$result

effs <- NULL
for(loc in locs){
  eff <- results[[toString(loc)]]
  eff <- c(loc, eff)
  names(eff) <- c('location_id', 'sd1_eff', 'sd2_eff', 'sd3_eff', 'psd1_eff', 'psd3_eff', 'anticipate_eff')
  effs <- rbind(effs, eff)
}

effs_locname <- data.table(effs)

colMeans(effs)

# save the random effects of SD mandates from MR-BRT
fwrite(effs_locname, MRBRT_SOCIAL_DISTANCING_RE_PATH)

  
##############################################
## prep forecasted mandates                 ##
##############################################
  
lift_pred <- fread(LIFT_PRED_DATA_PATH)
lift_pred[, date:= as.Date(date)]
  
df_smooth <- merge(df_smooth[location_id %in% lift_pred$location_id], lift_pred[, .(location_id, date, percent)], by = c("location_id", "date"),all.x = T)
df_smooth[, percent_obs := (sd1_lift + sd2_lift + sd3_lift + psd1_lift + psd3_lift)/5]
  
sd_vars <- c('sd1_lift', 'sd2_lift', 'sd3_lift', 'psd1_lift', 'psd3_lift')
df_smooth[, (sd_vars) :=  lapply(.SD, as.double), .SDcols = sd_vars]

df_smooth[, num_on := (sd1_lift + sd2_lift + sd3_lift + psd1_lift + psd3_lift)]
df_smooth[, num_off := 5 - num_on]

df_smooth[, sd1_lift_pred:= sd1_lift]
df_smooth[, sd2_lift_pred:= sd2_lift]
df_smooth[, sd3_lift_pred:= sd3_lift]
df_smooth[, psd1_lift_pred:= psd1_lift]
df_smooth[, psd3_lift_pred:= psd3_lift]

# ramping down - forecasted pct of mandates is less than last obs value (percent<=(num_on/5))
# if a mandate was off, it stays off; if a mandate was on, it ramps down
df_smooth[!is.na(percent) & percent<=percent_obs & percent!=0, sd1_lift_pred:= sd1_lift * (5*percent)/num_on]
df_smooth[!is.na(percent) & percent<=percent_obs & percent!=0, sd2_lift_pred:= sd2_lift * (5*percent)/num_on]
df_smooth[!is.na(percent) & percent<=percent_obs & percent!=0, sd3_lift_pred:= sd3_lift * (5*percent)/num_on]
df_smooth[!is.na(percent) & percent<=percent_obs & percent!=0, psd1_lift_pred:= psd1_lift * (5*percent)/num_on]
df_smooth[!is.na(percent) & percent<=percent_obs & percent!=0, psd3_lift_pred:= psd3_lift * (5*percent)/num_on]

# if percent = 0, then all mandates should be off
df_smooth[percent==0, `:=` (sd1_lift_pred=0,sd2_lift_pred=0, sd3_lift_pred=0, psd1_lift_pred=0, psd3_lift_pred=0)]

# ramping up - forecasted pct of mandates is larger than last obs value (percent>(num_on/5))
# if a mandate was on, it stays on; if a mandate was off, it ramps up
df_smooth[!is.na(percent) & percent>percent_obs & sd1_lift==0, sd1_lift_pred:= (5*(percent - percent_obs))/num_off]
df_smooth[!is.na(percent) & percent>percent_obs & sd2_lift==0, sd2_lift_pred:= (5*(percent - percent_obs))/num_off]
df_smooth[!is.na(percent) & percent>percent_obs & sd3_lift==0, sd3_lift_pred:= (5*(percent - percent_obs))/num_off]
df_smooth[!is.na(percent) & percent>percent_obs & psd1_lift==0, psd1_lift_pred:= (5*(percent - percent_obs))/num_off]
df_smooth[!is.na(percent) & percent>percent_obs & psd3_lift==0, psd3_lift_pred:= (5*(percent - percent_obs))/num_off]

# check that we've divvied up the prob of each mandate correctly (i.e. it tallies up to the predicted percent of mandates) 
df_smooth[, percent_pred := (sd1_lift_pred + sd2_lift_pred + sd3_lift_pred + psd1_lift_pred + psd3_lift_pred)/5]

if(nrow(df_smooth[abs(percent - percent_pred)>1e-8])>0){
  stop("Something went wrong with mandate lift predictions")
}


###################################################
## making forecasts using mr-brt mandate effects ##
###################################################
  
# merge the table of effect sizes to the expanded mobility dataset
df_smooth_w_effs <- merge(df_smooth, effs_locname, by="location_id", all.x = T)

# Slowly ramp mandate activity over 5 days (0.2, 0.4, 0.6, 0.8, 1)
# This helps the mandate effect on mobility not be so abrupt, but instead spread out over
# the first 5 days of the mandate
df_smooth_w_effs_pred <- copy(df_smooth_w_effs)
sd_vars <- c('sd1_lift', 'sd2_lift', 'sd3_lift', 'psd1_lift', 'psd3_lift')
df_smooth_w_effs_pred[, (sd_vars) :=  lapply(.SD, as.double), .SDcols = sd_vars]

# Add a counter to help identify first 5 days of mandate activity
df_smooth_w_effs_pred[, sd1_days := sequence(rle(sd1_lift)$lengths), by=location_id]
df_smooth_w_effs_pred[, sd2_days := sequence(rle(sd2_lift)$lengths), by=location_id]
df_smooth_w_effs_pred[, sd3_days := sequence(rle(sd3_lift)$lengths), by=location_id]
df_smooth_w_effs_pred[, psd1_days := sequence(rle(psd1_lift)$lengths), by=location_id]
df_smooth_w_effs_pred[, psd3_days := sequence(rle(psd3_lift)$lengths), by=location_id]

# No need to ramp up for the first 5 days of the time series
df_smooth_w_effs_pred[date>='2020-01-01' & date<='2020-01-05',
                      c('sd1_days', 'sd2_days', 'sd3_days', 'psd1_days', 'psd3_days') := NA]

# Ramp up
# For the first 5 days of the mandate being on, we ramp up each day by 0.2 until we reach 1
for(sd_var in c('sd1', 'sd2', 'sd3', 'psd1', 'psd3')){
  df_smooth_w_effs_pred[get(paste0(sd_var,'_days'))==1 & get(paste0(sd_var,'_lift'))==1, (paste0(sd_var,'_lift')):= 0.2]
  df_smooth_w_effs_pred[get(paste0(sd_var,'_days'))==2 & get(paste0(sd_var,'_lift'))==1, (paste0(sd_var,'_lift')):= 0.4]
  df_smooth_w_effs_pred[get(paste0(sd_var,'_days'))==3 & get(paste0(sd_var,'_lift'))==1, (paste0(sd_var,'_lift')):= 0.6]
  df_smooth_w_effs_pred[get(paste0(sd_var,'_days'))==4 & get(paste0(sd_var,'_lift'))==1, (paste0(sd_var,'_lift')):= 0.8]
}

# Ramp down
# For the first 5 days of the mandate being off, we ramp down each day by 0.2 until we reach 0
for(sd_var in c('sd1', 'sd2', 'sd3', 'psd1', 'psd3')){
  df_smooth_w_effs_pred[get(paste0(sd_var,'_days'))==1 & get(paste0(sd_var,'_lift'))==0, (paste0(sd_var,'_lift')):= 0.8]
  df_smooth_w_effs_pred[get(paste0(sd_var,'_days'))==2 & get(paste0(sd_var,'_lift'))==0, (paste0(sd_var,'_lift')):= 0.6]
  df_smooth_w_effs_pred[get(paste0(sd_var,'_days'))==3 & get(paste0(sd_var,'_lift'))==0, (paste0(sd_var,'_lift')):= 0.4]
  df_smooth_w_effs_pred[get(paste0(sd_var,'_days'))==4 & get(paste0(sd_var,'_lift'))==0, (paste0(sd_var,'_lift')):= 0.2]
}

# forecasting using the average day effect
df_smooth_w_effs_pred[, pred_mean := sd1_lift_pred*sd1_eff + sd2_lift_pred*sd2_eff + sd3_lift_pred*sd3_eff + 
                        psd1_lift_pred*psd1_eff + psd3_lift_pred*psd3_eff + anticipate*anticipate_eff]


# For US locations, school closures should have only 50% of the estimated effect on mobility starting August 15
us_locs <- hierarchy[grep("USA", ihme_loc_id), location_id]
df_smooth_w_effs_pred[date>='2020-08-15' & location_id %in% us_locs, pred_mean := sd1_lift_pred*sd1_eff +  sd2_lift_pred*(sd2_eff/2) +  sd3_lift_pred*sd3_eff + 
                        psd1_lift_pred*psd1_eff +  psd3_lift_pred*psd3_eff + anticipate*anticipate_eff]


# calculate residuals
df_smooth_w_effs_pred[, resid := mean - pred_mean]


###################################
## Run arima(0,1,0) on residuals ##
###################################

# save unique location_id in the mobility date
locs <- df_smooth_w_effs_pred[,location_id] %>% unique

# creating place holder for upper and lower resid prediction
df_smooth_w_effs_pred[, arima_pred_resid_lower := 0]
df_smooth_w_effs_pred[, arima_pred_resid_upper := 0]

# Running ARIMA(0,1,0) and producing QC plots of observed v.s. predicted residuals
AR <- 0
I <- 1
order <- c(AR,I,0)

for(loc in locs){
  print(paste0(loc, ': ', hierarchy[location_id==loc, location_name]))
  
  # run ARIMA model
  resids <- df_smooth_w_effs_pred[location_id==loc & !is.na(resid), resid]
  ar1_model <- arima(resids, order = order, include.mean = T)
  
  # forecast residuals
  forecasts <- forecast(ar1_model, h=length(df_smooth_w_effs_pred[location_id==loc & is.na(resid), resid]))
  upper <- forecasts$upper[,"95%"]
  lower <- forecasts$lower[,"95%"]
  
  # populate the arima fitted and forecasted values
  df_smooth_w_effs_pred[location_id==loc & !is.na(resid), arima_pred_resid := fitted(ar1_model)]
  
  arima_preds <- predict(ar1_model, n.ahead =length(df_smooth_w_effs_pred[location_id==loc & is.na(resid), resid]))$pred
  df_smooth_w_effs_pred[location_id==loc & is.na(resid), arima_pred_resid := arima_preds]
  
  # include 95% CI, for observed period, the CI is 0 
  df_smooth_w_effs_pred[location_id==loc & is.na(resid), arima_pred_resid_lower := lower]
  df_smooth_w_effs_pred[location_id==loc & is.na(resid), arima_pred_resid_upper := upper]
}

#########################################################################
## producing mobility forecasts using predicted mobility and residuals ##
#########################################################################
  
df_smooth_w_effs_pred[, arima_pred_resid := as.numeric(unclass(arima_pred_resid))]
  

if(china_no_relax == T){
  
  china_locs <- hierarchy[location_id ==6|parent_id == 6|parent_id == 44533,]$location_id
  
  df_smooth_w_effs_pred_china <- df_smooth_w_effs_pred[location_id %in% china_locs,]
  df_smooth_w_effs_pred <- df_smooth_w_effs_pred[!(location_id %in% china_locs),]
  
}


if(RELAX_RESID==T){
  # Linear relaxation of the residual and the anticipate effect to zero after 3 months
  # Don't start relaxing until first predicted date or 2/15/21, whichever is later
  
  # Set the end date to 3 months (90 days) in the future
  relax_end_dt <- max(df_smooth_w_effs_pred[!is.na(mean), date]) + 90 
  
  # First, we need a list of the first day of forecasted mobility for each location
  dates_by_loc <- df_smooth_w_effs_pred[!is.na(mean), max(date) + 1, by="location_id"] 
  setnames(dates_by_loc, "V1", "first_pred_dt")
  
  # If the first prediction is before 2/15/21, force the date to 2/15
  dates_by_loc[first_pred_dt<'2021-02-15', first_pred_dt := as.Date('2021-02-15')]
  
  # Now calculate the number of days over which the relaxation will occur
  dates_by_loc[, days_til_end := as.numeric(relax_end_dt - first_pred_dt) + 1]
  
  # Merge this information back onto the full dataset and calculate the scalar
  df_smooth_w_effs_pred <- merge(df_smooth_w_effs_pred, dates_by_loc, by="location_id")
  df_smooth_w_effs_pred <- df_smooth_w_effs_pred[, days_since_first_pred := as.numeric(date - first_pred_dt)]
  df_smooth_w_effs_pred <- df_smooth_w_effs_pred[, scalar := 1 - (1/days_til_end)*(days_since_first_pred)]
  df_smooth_w_effs_pred <- df_smooth_w_effs_pred[, scalar := ifelse(scalar<0, 0, ifelse(scalar>1,1,scalar))]
  
  # If mobility on the last day of observed data is positive, don't adjust forecasts
  locs_to_leave <- df_smooth_w_effs_pred[date==(first_pred_dt-1) & mean > 0, location_id]
    
  # Apply the scalar to the residual
  df_smooth_w_effs_pred <- df_smooth_w_effs_pred[! location_id %in% locs_to_leave, arima_pred_resid:=arima_pred_resid*scalar]
  
  # Apply the scalar to the anticipate coefficient
  df_smooth_w_effs_pred <- df_smooth_w_effs_pred[! location_id %in% locs_to_leave, remove := anticipate_eff * (1 - scalar)]
  df_smooth_w_effs_pred <- df_smooth_w_effs_pred[location_id %in% locs_to_leave, remove := 0]
  
  # Put it all together to calculate the updated forecast
  df_smooth_w_effs_pred[, pred_gprmean_arima := pred_mean + arima_pred_resid - remove]
  df_smooth_w_effs_pred[, pred_gprmean_arima_upper := pred_mean + arima_pred_resid_upper - remove]
  df_smooth_w_effs_pred[, pred_gprmean_arima_lower := pred_mean + arima_pred_resid_lower - remove]
  
  } else {
  # when relax_resid=F, anticipate and resid remain unchanged
  df_smooth_w_effs_pred[, pred_gprmean_arima := pred_mean + arima_pred_resid]
  df_smooth_w_effs_pred[, pred_gprmean_arima_upper := pred_mean + arima_pred_resid_upper]
  df_smooth_w_effs_pred[, pred_gprmean_arima_lower := pred_mean + arima_pred_resid_lower] 
  
  #when relax_resid=F, cap forecasts at 0
  df_smooth_w_effs_pred[is.na(resid) & pred_gprmean_arima>0, pred_gprmean_arima := 0]
  df_smooth_w_effs_pred[is.na(resid) & pred_gprmean_arima_upper>0, pred_gprmean_arima_upper := 0]
  df_smooth_w_effs_pred[is.na(resid) & pred_gprmean_arima_lower>0, pred_gprmean_arima_lower := 0]
  
  }

if(china_no_relax == T){
  
  # when relax_resid=F, anticipate and resid remain unchanged
  df_smooth_w_effs_pred_china[, pred_gprmean_arima := pred_mean + arima_pred_resid]
  df_smooth_w_effs_pred_china[, pred_gprmean_arima_upper := pred_mean + arima_pred_resid_upper]
  df_smooth_w_effs_pred_china[, pred_gprmean_arima_lower := pred_mean + arima_pred_resid_lower] 
  
  #when relax_resid=F, cap forecasts at 0
  df_smooth_w_effs_pred_china[is.na(resid) & pred_gprmean_arima>0, pred_gprmean_arima := 0]
  df_smooth_w_effs_pred_china[is.na(resid) & pred_gprmean_arima_upper>0, pred_gprmean_arima_upper := 0]
  df_smooth_w_effs_pred_china[is.na(resid) & pred_gprmean_arima_lower>0, pred_gprmean_arima_lower := 0]
  
  df_smooth_w_effs_pred <- rbind(df_smooth_w_effs_pred, df_smooth_w_effs_pred_china, fill = T)
}
  
  #### Define the worse scenario
  if(WORSE_SCENARIO==T){
    # get the value of mobility on the last day of observed data (or 2/15/21, whichever is later)
    start_vals <- df_smooth_w_effs_pred[date==first_pred_dt, .(location_id, start_val = pred_gprmean_arima)]
    df_smooth_w_effs_pred <- merge(df_smooth_w_effs_pred, start_vals, by='location_id', all.x=T)
    
    # calculate the slope required to reach 25% in 8 weeks
    df_smooth_w_effs_pred <- df_smooth_w_effs_pred[, slope := (25 - start_val)/56]
    df_smooth_w_effs_pred <- df_smooth_w_effs_pred[, new := (slope * days_since_first_pred) + start_val]
    
    # adjust forecasts
    df_smooth_w_effs_pred <- df_smooth_w_effs_pred[, pred_gprmean_arima_worse := pred_gprmean_arima]
    df_smooth_w_effs_pred <- df_smooth_w_effs_pred[date>=first_pred_dt, pred_gprmean_arima_worse:=new]
    # after 8 weeks, mobility stays at 25%
    df_smooth_w_effs_pred[days_since_first_pred > 56, pred_gprmean_arima_worse := 25]
    
    # make the final estimate (don't adjust location-days where worse is lower than ref)
    df_smooth_w_effs_pred[date>=first_pred_dt & pred_gprmean_arima_worse > pred_gprmean_arima, pred_gprmean_arima := pred_gprmean_arima_worse]
    
  }
  
  # Add location name
  df_smooth_w_effs_pred$location_name <- NULL #drop if it exists
  df_smooth_w_effs_pred <- merge(df_smooth_w_effs_pred, hierarchy[, .(location_id, location_name)],
                                 by='location_id', all.x=T, sort=F)

  forecasts <- df_smooth_w_effs_pred[, .(location_id, date, location_name, mean, pred_gprmean_arima,
                        sd1_lift, sd2_lift, sd3_lift, psd1_lift, psd3_lift, anticipate,
                        pred_gprmean_arima_lower, pred_gprmean_arima_upper, population)]
  

pdf(SMOOTHED_ARIMA_PLOT_PATH)
for(loc in locs){
  print(paste0(loc, ': ', hierarchy[location_id==loc, location_name]))
  
  # plot the observed vs. predicted residuals
  p <- ggplot(df_smooth_w_effs_pred[location_id==loc,]) + 
    geom_line(aes(x = date, y = arima_pred_resid, color="blue")) + 
    geom_line(aes(x = date, y = resid, color="black")) +
    theme_bw() +
    xlab(paste0(loc, ': ', hierarchy[location_id==loc, location_name])) +
    ylab('residual') +
    scale_color_discrete(name = "residuals", labels = c("observed", "predicted"))
  
  print(p)
  
}
dev.off()


# save this full dataset for plotting purpose
fwrite(df_smooth_w_effs_pred, MOBILITY_FORECAST_PATH)

# add type variable, 1 means forecasted mobility and 0 means observed mobility
forecasts[, type:= as.numeric(is.na(mean))]

# the final forecasts = observed mobility (for the observed time period) + forecaseted mobility (for the forecasted period)
forecasts[, mobility_forecast := pred_gprmean_arima]
forecasts[, lower := pred_gprmean_arima_lower]
forecasts[, upper := pred_gprmean_arima_upper]

forecasts[type==0, mobility_forecast := mean]
forecasts[type==0, lower := mean]
forecasts[type==0, upper := mean]


# Cap mobility forecasts at 0
#if generating the ref scenario, only cap if last day of observed mobility < 0
if(WORSE_SCENARIO==F){
  forecasts[(! location_id %in% locs_to_leave) & type==1 & mobility_forecast>0, mobility_forecast := 0]
  forecasts[(! location_id %in% locs_to_leave) & type==1 & lower>0, lower := 0]
  forecasts[(! location_id %in% locs_to_leave) & type==1 & upper>0, upper := 0]
} 
# if generating the worse scenario, don't cap at all!


# Aggregate to create parent locations
# Make sure to do WA before US
all_parents <- data.table()

for(parent_loc in hierarchy[order(-level)][most_detailed == 0 & level >=3, location_id]){
  
  print(parent_loc)
  
  child_locs <- hierarchy[location_id != parent_loc & parent_id == parent_loc, location_id]
  
  parent_dt <- copy(forecasts[location_id %in% child_locs])
  
  # pop-weighted mean across child locs
  parent_dt <- parent_dt[, .(mobility_forecast = weighted.mean(mobility_forecast, w=population, na.rm=T), 
                             mean = weighted.mean(mean, w=population, na.rm=T),
                             type = max(type), 
                             population = sum(population)), by="date"]
  parent_dt[type==0, mobility_forecast := mean]
  
  
  parent_dt[, location_id := parent_loc]
  parent_dt[, location_name := hierarchy[location_id == parent_loc, location_name]]
  
  all_parents <- rbind(all_parents, parent_dt, fill =T)
  
}

forecasts <- rbind(all_parents, forecasts, fill = T)

forecasts[,population:=NULL]


# save the final forecasts
fwrite(forecasts, MOBILITY_FORECAST_SMOOTHED_PATH)


yaml::write_yaml(
  list(
    script = "02_run_mrbrt.R",
    output_dir = work_dir,
    input_files = ihme.covid::get.input.files()
  ),
  file = metadata_path
)

