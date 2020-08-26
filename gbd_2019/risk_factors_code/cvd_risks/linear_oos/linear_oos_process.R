################################
## Purpose: calculate out of sample rmse for one sex-model, process script for oos_all_mods_submit.R script
#######################################

rm(list=ls())
j <- "FILEPATH"

library(data.table)
library(lme4)
library(MuMIn, lib='FILEPATH')
library(boot)
library(ggplot2)
library(rhdf5)
library(arm)
library(haven)
library(plyr)
library(dplyr)
library(stringr)
library(DBI)
library(RMySQL)

################### ARGS AND PATHS #########################################
args <- commandArgs(trailingOnly = TRUE)
start <- as.integer(args[1])
end <- as.integer(args[2])
sexchar <- args[3]
bundle_id <- as.integer(args[4])
date <- args[5]
data_transform <- args[6]
modtype <- args[7]
kos <- args[8]
kos <- as.integer(kos)

if(F){
  sexchar <- "M"
  index <- 1 
  ko <- 0
  data_id <- 4864
  data_transform <- "log"
  modtype <- "lmer"
  date <- gsub("-", "_", Sys.Date())
}

###################  PATHS AND ARGS  #########################################
output_folder <- paste0("FILEPATH", bundle_id, "_", date, "/")
path <- paste0(output_folder, sexchar, "_full_data.rds")


central <- "FILEPATH"
stgpr_central <- "FILEPATH"

suppressMessages(F)
###################  SCRIPTS  #########################################
if(T){
setwd(stgpr_central)

source('register_data.r')
source('setup.r')
source('model.r')
source('rake.r')
source('utility.r')

source("FILEPATH/bind_covariates.R"))
source(paste0(central, "get_location_metadata.R"))
}

################### GET FULL DATA SET #########################################
data <- readRDS(path)

## get saved formuli
forms <- readRDS(paste0(output_folder, "forms.rds"))

for(index in start:end){ ## loop over start to end
  output <- paste0(output_folder, "form_", sexchar, "_", index, ".csv") ## save the output of each model here

  suppressMessages({
  message(forms)
  message(paste("class of forms:", class(forms), length(forms)))
  message(paste("i:", index, class(index)))
  message(forms[index])
  message(class(forms[index]))

  form<-forms[index]

  message(paste0("pre-whitespace clean form:", form))
  })
  
  if(index>length(forms)){ stop("Formula index larger than length of formulas!" )}

  ################### DEFINE PRED.LM FUNCTION #########################################
  pred.lm <- function(df, model, predict_re=0) {
    ## RE form
    re.form <- ifelse(predict_re==1, NULL, NA)
    ## Predict
    if (class(model) == "lmerMod") {
      prior <- predict(model, newdata=df, allow.new.levels=T, re.form=re.form)
    } else {
      prior <- predict(model, newdata=df)
    }
    return(prior)
  }

  ################### CALCULATE RMSES #########################################
  ## male and female formula lists need to be same length (legnth(mforms)==length(fforms) is TRUE)

  message(paste("Formula:", form))

  ## create a ko0 column for all data
  data[, ko0:=0]

  rmses <- list()
  for(ko in 0:kos){  ## ko==1 is the full dataset
    if(ko==0){
      message(paste(" Calculating in-sample RMSE"))
    } else {
      message(paste(" Calculating out-of-sample RMSE for holdout", ko))
    }


    test <- data[get(paste0("ko", ko))==1, ]
    test <- as.data.table(test)
    train <- data[get(paste0("ko", ko))==0, ]

    if(ko==0){
      test <- data  ## this is in sample rmse.  Same as train but includes all subnational locations
      train <- data
    }

    covs <- list()
    message(paste("     Calculating RMSE for formula", index))

    if(modtype=="lmer"){
      mod <- lmer(as.formula(form), data=train)
    }

    if(modtype=="lm"){
      mod<-lm(as.formula(form), data=train)
    }
    preds <- pred.lm(test, mod, predict_re=0)


    if(length(preds)!= nrow(test)){
      stop("Prediction not done for each row, there may be missing values in the data or a covariate!")
    }

    test.t <- cbind(test, preds)  

    test.t[, preds:=transform_data(preds, data_transform, reverse=T)]  ## transform data back

    test.t[, sqrerr:=(data-preds)^2]
    rmse <- sqrt(mean(test.t$sqrerr, na.rm=T))

    covs <- as.character(as.formula(form))[3]  ## pulling out the covariates from the formula

    if(ko==0){
      is.rmse<-rmse


      if(modtype=="lmer"){
        fixd <- fixef(mod)
        fixd_se <- se.fixef(mod)
      }
      if(modtype=="lm"){
        fixd <- coef(mod)
        fixd_se <- coef(summary(mod))[, 2]
      }
      fixd <- fixd[!grepl("age_group_id", names(fixd))]  ## taking out age group betas

      fixd_se <- fixd_se[!grepl("age_group_id", names(fixd_se))]  ## taking out age group SEs

      fixd <- data.table(cov=names(fixd), fixd, fixd_se)

      fixd.m <- melt(fixd, measure.vars=c("fixd", "fixd_se"))
      effects <- dcast(fixd.m, .~cov+variable)
      effects[, .:=NULL]

      message("   Calculating AIC")
      aic <- AIC(logLik(mod))

    } else {

      rmses[[ko]]<-rmse
    }

  } ##########################END KO LOOP###################

  if(length(rmses)==0){
    rmse <- "No holdouts done"
  } else {
    rmse <- unlist(rmses) %>% mean
  }

  out <- data.table(out_rmse=rmse, in_rmse=is.rmse, aic=aic, covs=covs, sex=sexchar)
  out.t <- cbind(out, effects)

  rm(mod) ## clean up the model a bit

  write.csv(out.t, file=output, row.names=F)

  message(paste0("Output saved to", output))
}
