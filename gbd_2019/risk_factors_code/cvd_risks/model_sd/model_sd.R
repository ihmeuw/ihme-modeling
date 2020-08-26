################################
##Purpose: Model SD
#######################################

if(!exists("rm_ctrl")){
  rm(list=objects())
}
os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  lib_path <- "FILEPATH"
  j <- "FILEPATH"
  h <- "FILEPATH"
}

date<-gsub("-", "_", Sys.Date())

library(data.table)
library(lme4)
library(MuMIn, lib='FILEPATH')
library(boot)
library(ggplot2)
library(rhdf5)
library(haven)
library(plyr)
library(dplyr)
library(stringr)
library(DBI)
library(RMySQL)
library(mvtnorm)
library(RColorBrewer)
library(randomForest)

################### SCRIPTS #########################################
central<-"FILEPATH"
code_root <- "FILEPATH"

source(paste0(code_root, "utility/get_recent.R")) 
source(paste0(code_root, "utility/get_user_input.R")) 
source(paste0(code_root, "utility/bind_covariates.R")) 
source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_model_results.R"))
source(paste0(central, "save_results_epi.R"))

################### ARGS AND PATHS #########################################
if(!exists("me")){
  me<-get_me_from_user() ## needs to be "sbp" or "chl" or "ldl"
  decomp_step<-get_step_from_user()
  }

step_num <- gsub('step', '', decomp_step)

pool_oldage <- F
descr <- "Annual SD"
convert_se <- F

input_folder <- paste0("FILEPATH/", me, "_to_sd/")
output <- paste0("FILEPATH", me, "_models/", me, "_sd_mod_decomp", step_num, "_", date, ".rds")
plot_output <- paste0("FILEPATH", me, "_sd_model_", date, ".pdf")

## Get age weights
age_wts <- "FILEPATH/age_weights.csv"

################### UTILITY FUNCTION #########################################
get_ci <- function(mod, interval=.95){
  ## pull out coefs
  coefs<-summary(mod)$coef
  variable<-row.names(coefs)
  coefs<-as.data.table(coefs)
  coefs<-cbind(variable, coefs)
  
  ## get model type names
  if(class(mod)=="lm"){
    se_colname<-"Std. Error"
    est_colname<-"Estimate"
  }
  
  setnames(coefs, c(est_colname, se_colname, grep("Pr(>|*|)", names(coefs), value=T)), c("coef", "se", "p_val"))
  coefs <- coefs[, .(variable, coef, se, p_val)]
  
  ## calculate upper and lower
  z <- abs(qnorm((1-interval)/2))
  coefs[, `:=` (lower=coef-z*se, upper=coef+z*se)]
  ##sy: put in HR space
  coefs[, `:=` (exp_mean=exp(coef), exp_lower=exp(lower), exp_upper=exp(upper))]
  return(coefs)
}

################### GET SD DATA #########################################
message("Modelling sd for ", me)

locs <- get_location_metadata(location_set_id = 22)[,.(location_id, super_region_name, region_name)]
full <- get_recent(input_folder, pattern=paste0("decomp", step_num))

################### CONSTRUCT FORMULAS #########################################
cov_list<-c("sdi", "prev_overweight")

################### CREATE NEW VARS + DEFINE WEIGHTS #########################################
full[, coef_var:=sd/mean]

## add mean^2 as a covariate
full[, rt_mean:=sqrt(mean)]
full[, log_mean:=log(mean)]

## remap age for predictions
if(pool_oldage==T){
  full[, age_group_orig:=age_group_id]
  full[age_group_id>20, age_group_id:=21]
}

################### LOOP OVER MODELS #########################################
## manually set a formula
form <- c("log(sd)~log_mean+factor(age_group_id)+factor(sex_id)")

mod <- lm(formula=as.formula(form), data=full)
fit <- predict(mod)
preds <- predict.lm(mod, interval="confidence", weights=wt)
preds <- cbind(full, preds)

preds <- predict.lm(mod, interval="confidence", weights=wt)

preds <- cbind(full, preds)
preds[, fit:=exp(fit)] ## transform the fit
preds[, diff:=sd-fit]
is.rmse <- sqrt(mean(preds$diff^2))

## get betas
betas<-coef(mod)

################### Predict #########################################
## gen new data and predict
if(pool_oldage==T){
  remap_ages <- setdiff(unique(full$age_group_orig), unique(full$age_group_id))
  
  pred_age_grps <- c(unique(full$age_group_id), rep(21, times=length(remap_ages)-1))
} else {
  pred_age_grps <- unique(full$age_group_id)
}
new_data <- data.table(age_group_id=rep(pred_age_grps, times=2), 
                     sex_id=c(rep(1, times=length(pred_age_grps)), rep(2, times=length(pred_age_grps))),
                     mean=mean(full$mean))
new_data[, log_mean:=log(mean)]

if(pool_oldage==T){new_data[, age_group_orig:=unique(full$age_group_orig)]}
preds <- exp(predict.lm(mod, interval="confidence", newdata = new_data))
preds <- cbind(new_data, preds)

## remap age group id to age
preds[, age_start:=5*age_group_id-25]
preds[age_group_id==30, age_start:=80]
preds[age_group_id==31, age_start:=85]
preds[age_group_id==32, age_start:=90]
preds[age_group_id==235, age_start:=95]
preds[, sex_char:=ifelse(sex_id==1, "Male", "Female")]


################### SAVE OUT #########################################
saveRDS(mod, file=output)
message("Done")
