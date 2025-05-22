#######################################################################
## author: USERNAME
## date: DATE
## purpose:    -Predict bested SD model
## notes: this used to be in the model_sd.R script, splitting it out for modularity
##
## source("/FILEPATH/predict_sd.R")
##
#######################################################################

rm(list=ls())

library(data.table)
library(lme4)
library(boot)
library(mvtnorm)
library(ggplot2)
library(tools)
library(splines)

central<-"/FILEPATH/"
source(paste0(central, "get_model_results.R"))
source(paste0(central, "get_age_metadata.R"))

source("/FILEPATH/model_helper_functions.R")

#######################################################################
# Arguments and filepath
#######################################################################

me <- "sbp"
release_id <- 16
description <- "SD model with new data, added year_id, switched to spline on age"

sd_mod_folder <- paste0("/FILEPATH/")
output_folder <- paste0("/FILEPATH/")
if(me=='sbp'){
  usual_bp_folder <- "/FILEPATH/"
}

if(me=="sbp"){
  exp_meid <- 2547
  sd_meid <- 15788
  mod_name <- "sbp_sd_mod_release_16_2024_08_10.rds"
} else if(me=="ldl"){
  exp_meid <- 18822
  sd_meid <- 18823
  mod_name <- "ldl_sd_mod_release_16_2024_08_10.rds"
}

#######################################################################
# Pull in mean exposure results and SD model
#######################################################################

# pull in mean exposure results
results <- get_model_results(gbd_team = "stgpr", gbd_id = exp_meid, release_id = release_id)
results <- results[!age_group_id %in% c(22, 27)]
age_meta <- get_age_metadata(release_id = release_id)
results <- merge(results, age_meta[,.(age_group_id, age_group_years_start)], by='age_group_id')
setnames(results, 'age_group_years_start', 'age_start')

# pull out IDs
exp_mvid <- unique(results$model_version_id)
bundle_id <- unique(results$bundle_id)
crosswalk_id <- unique(results$crosswalk_version_id)

# pull in SD model
sd_mod <- readRDS(paste0(sd_mod_folder, mod_name))

# pull in usual BP adjustments
if(me=="sbp"){
  usual_bp <- fread(paste0(usual_bp_folder, 'sbp_usual_bp_adj_ratios.csv'))
}

#######################################################################
# Apply SD model to mean exposure results and create draws
#######################################################################

# get draws of covariates
cov_mat <- vcov(sd_mod)
par_draws <- rmvnorm(n=1000, mean=sd_mod$coefficients, sigma=cov_mat)
pred_math <- "exp(X %*% betas)"

# clear out output folder
unlink(output_folder, recursive=T)
dir.create(output_folder)

# loop over locations and predict, can parallelize at some point 
for(loc in unique(results$location_id)){
  message(paste0("Creating draws for loc_id ", loc, ' | ', which(unique(results$location_id)==loc), ' out of ', length(unique(results$location_id))))
  
  results.l <- results[location_id == loc]
  results.l[, log_mean := log(mean)]
  
  # make design matrix
  X <- make_fixef_matrix(df=results.l, fixefs=attr(terms(sd_mod),"term.labels"), add_intercept = T)
  colnames(X)[1]<-"(Intercept)"
  if(!all(colnames(X)==colnames(par_draws))){stop("design matrix mismatch with draw matrix")}
  
  # predict
  draw_list<-list(betas=par_draws)
  data_list<-list(X=X)
  temp_draws <- predict_draws(prediction_math = pred_math, draw_list=draw_list, data_list=data_list, return_draws=T)
  setnames(temp_draws, names(temp_draws), gsub('draw', 'draw_', names(temp_draws)))
  out <- cbind(results.l[, .(location_id, year_id, age_group_id, sex_id)], temp_draws)
  
  # apply usual bp adjustment
  if(me=="sbp"){
    out <- merge(out, usual_bp[,.(age_group_id, ratio)], by="age_group_id")
    out[, paste0("draw_", 0:999) := lapply(0:999, function(x) {
      get(paste0("draw_", x)) * ratio
    })]
    out[, ratio := NULL]
  }

  # write out data
  write.csv(out, file = paste0(output_folder, loc, "_19.csv"), row.names = F)
}

#######################################################################
# Launch save results job
#######################################################################

tracker_path <- paste0("/FILEPATH/sd_upload_tracker.csv")

command <- paste("sbatch -A proj_cvd --mem=80G -t 48:00:00 -c 10 -p all.q -J",
                 paste0("saving_SD_draws_", me),
                 paste0("-o /FILEPATH/%x_%j.o -e /FILEPATH/%x_%j.e"), 
                 "/FILEPATH/execRscript.sh -s /FILEPATH/upload_predict_sd.R",
                 release_id, sd_meid, output_folder, exp_mvid, bundle_id, crosswalk_id, gsub(' ', '__', description), mod_name, tracker_path)

print(command)
system(command)
