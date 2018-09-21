################################
##author:USERNAME
##date: 3/28/2017
##purpose:    -calculate out of sample rmse for one sex-model
##            -process script for oos_all_mods_submit.R script
##         
#######################################

rm(list=ls())
os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
} else {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
}



library(data.table, lib.loc=lib_path)
library(lme4, lib.loc=lib_path)
library(MuMIn, lib.loc=lib_path)
library(boot, lib.loc=lib_path)
library(ggplot2, lib.loc=lib_path)
library(rhdf5, lib.loc=lib_path)
library(arm, lib.loc=lib_path)
library(haven, lib.loc=lib_path)
library(plyr, lib.loc=lib_path)
library(dplyr, lib.loc=lib_path)
library(stringr, lib.loc=lib_path)
library(DBI, lib.loc=lib_path)
library(RMySQL, lib.loc=lib_path)

################### ARGS AND PATHS #########################################
######################################################

args <- commandArgs(trailingOnly = TRUE)
sexchar<- args[1]
data_id<-args[2]
data_id<-as.integer(data_id)
date<-args[3]
index<-args[4]
index<-as.integer(index)
data_transform<-args[5]
modtype<-args[6]
kos<-args[7]
kos<-as.integer(kos)

if(F){
  date<-"2017_04_04"
  date<-"2017_03_29"
  date<-gsub("-", "_", Sys.Date())
  sexchar<-"M"
  #form<-"log(data)~sdi+vehicles_2_plus_4wheels_pc+as.factor(age_group_id)+(1|super_region_name)+(1|region_name)"
  index<-8
  data_id<-4301
  data_transform<-"log"
  modtype<-"lmer"
  date<-gsub("-", "_", Sys.Date())
}



##USERNAME:paths


central<-paste0(j, "FILEPATH")


output_folder<-paste0(j, "FILEPATH", data_id, "_", date, "/")
output<-paste0(output_folder, "form_", sexchar, "_", index, ".csv")

path<-paste0(output_folder, sexchar, "_full_data.rds")


suppressMessages(F)
###################  SCRIPTS  #########################################
#####################################################

if(T){
setwd(paste0(j, 'FILEPATH'))

source('register_data.r')
source('setup.r')
source('model.r')
source('rake.r')
source('utility.r')
#source('graph.r')
#source('clean.r')


source(paste0(j, "FILEPATH/bind_covariates.R"))
source(paste0(central, "get_location_metadata.R"))
}


################### GET FULL DATA SET #########################################
######################################################



data<-readRDS(path)


##USERNAME: get saved formuli
forms<-readRDS(paste0(output_folder, "forms.rds"))



suppressMessages({
message(forms)

message(paste("class of forms:", class(forms), length(forms)))

message(paste("i:", index, class(index)))

message(forms[index])

message(class(forms[index]))



form<-forms[index]

message(paste0("pre-whitespace clean form:", form))
})
##USERNAME: get rid of white space in formula
#form<-gsub(" ", "", form)
if(index>length(forms)){ message("Formula index larger than length of formulas!" )}



################### DEFINE PRED.LM FUNCTION #########################################
######################################################

##USERNAME: this is from patty, should be same as used in STGPR.  Allows lm or lmer
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
######################################################
###############################################
##USERNAME: male and female formula lists need to be same length (legnth(mforms)==length(fforms) is TRUE)
  
  
message(paste("Formula:", form))

##USERNAME: create a ko0 column for all data
data[, ko0:=0]
  
  rmses<-list()
  for(ko in 0:kos){  ##USERNAME: ko==1 is the full dataset
    if(ko==0){
      message(paste(" Calculating in-sample RMSE"))
    }else{
      message(paste(" Calculating out-of-sample RMSE for holdout", ko))
    }
    
    
    test<-data[get(paste0("ko", ko))==1, ]
    test<-as.data.table(test)
    train<-data[get(paste0("ko", ko))==0, ]
    
    if(ko==0){
      test<-data  ##USERNAME: this is in sample rmse.  Same as train but includes all subnats
      train<-data
    }
    
    
    # subset to level 3 or higher (nationals) ##USERNAME: this is being done at the KO step instead now
    #train<-train[level<4  |  location_id %in% c(44533, 4749, 4640, 4639, 434, 84, 4636),] 
    
    
    
    covs<-list()
      message(paste("     Calculating RMSE for formula", index))
      
      if(modtype=="lmer"){
        mod<-lmer(as.formula(form), data=train)
      }
      
      if(modtype=="lm"){
        mod<-lm(as.formula(form), data=train)
      }
      #preds<-predict(mod, test, allow.new.levels=TRUE)  
      preds<-pred.lm(test, mod, predict_re=0)
      
      
      if(length(preds)!= nrow(test)){
        stop("Prediction not done for each row, there may be missing values in the data or a covariate!")
      }
      
      test.t<-cbind(test, preds)
      
      test.t[, preds:=transform_data(preds, data_transform, reverse=T)]  ##USERNAME: transform data back
      
      
      test.t[, sqrerr:=(data-preds)^2]
      rmse<-sqrt(mean(test.t$sqrerr, na.rm=T))
      
      
      covs<-as.character(as.formula(form))[3]  ##USERNAME:pulling out the covariates from the formula

    
    
    
    
    
    if(ko==0){
      is.rmse<-rmse
      
      
      if(modtype=="lmer"){
        fixd<-fixef(mod)
        fixd_se<-se.fixef(mod)
      }
      if(modtype=="lm"){
        fixd<-coef(mod)
        fixd_se<-coef(summary(mod))[, 2]
      }
      fixd<-fixd[!grepl("age_group_id", names(fixd))]  ##USERNAME: taking out age group betas
      
      fixd_se<-fixd_se[!grepl("age_group_id", names(fixd_se))]  ##USERNAME: taking out age group betas
      
      fixd<-data.table(cov=names(fixd), fixd, fixd_se)
      
      
      
      fixd.m<-melt(fixd, measure.vars=c("fixd", "fixd_se"))
      effects<-dcast(fixd.m, .~cov+variable)
      effects[, .:=NULL]
      
      message("   Calculating AIC")
      aic<-AIC(logLik(mod))
      
      
    }else{
      
      rmses[[ko]]<-rmse
    }
    
    
    
  } ##########################END KO LOOP###################
  
  
  if(length(rmses)==0){
    rmse<-"No holdouts done"
  }else{
    rmse<-unlist(rmses) %>% mean
  }
  
  
  out<-data.table(out_rmse=rmse, in_rmse=is.rmse, aic=aic, covs=covs, sex=sexchar)
  out.t<-cbind(out, effects)
  
  



write.csv(out.t, file=output, row.names=F)

message(paste0("Output saved to", output))

