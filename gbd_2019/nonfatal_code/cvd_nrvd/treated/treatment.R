
##Purpose: Model tx for nrvd

library(rstan)
library(data.table)
library(ggplot2)
library(boot)
library(ini)
library(shinystan)

date<-gsub("-", "_", Sys.Date())

################### ARGS AND PATHS #########################################
######################################################

decomp_step<-"4"
descr<-"Decomp step 4 initial run;"
upload_mes<-c("aort", "mitral", "other")
random_effect<-F
haqi_cov<-"Healthcare access and quality index" 
link<-"logit"
use_weights<-F

pred<-T
save_upload<-T
save_upload_2 <- T
best<-T

age_wts_path<-"FILEPATH"

stan_path<-"FILEPATH"

stan_path_random_int<-"FILEPATH"
stan_path_probit<-"FILEPATH"

input_folder<-"FILEPATH"
output_folder<-"FILEPATH"
betas_output<-"FILEPATH"

central<-"FILEPATH"


################### SCRIPTS #########################################
######################################################

source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_ids.R"))
source(paste0(central, "get_bundle_data.R"))
source(paste0(central, "save_results_epi.R"))

source("bind_covariates.R")
source("get_recent.R")
source("model_helper_functions.R")

source("db_tools.r")
source("setup.r")

################### UTILITY FUNCTIONS  #########################################
######################################################

extract_stan_matrix<-function(row_object, col_object, stan_par, stan_obj){
  if(!is.character(row_object) | !is.character(col_object)){stop("row_object and col_object must be names (character) of vectors")}
  rvect<-get(row_object)
  cvect<-get(col_object)

  name_rows<-paste0("haqi","_", rvect)
  name_cols<-paste0("age", "_", cvect)
  stan_matrix<-summary(stan_obj, pars=stan_par)$summary
  id<-row.names(stan_matrix)
  stan_matrix<-as.data.table(stan_matrix)
  stan_matrix[, id:=id]
  ## fill in
  invisible(lapply(1:length(name_rows), function(x){
    stan_matrix[grepl(paste0(x, ","), id), c(row_object):=rvect[x]]
  }))
  invisible(lapply(1:length(name_cols), function(x){
    stan_matrix[grepl(paste0(",", x), id), c(col_object):=cvect[x]]
  }))

  stan_matrix<-stan_matrix[, c("mean", "sd", row_object, col_object), with=F]
  setnames(stan_matrix, c("mean", "sd"), c("pred", "pred_sd"))
  return(stan_matrix)
}

################### GET DATA  #########################################
######################################################

lit<-get_recent(input_folder)
lit[, sex_id:=as.integer(ifelse(sex==3, "Both", ifelse(sex=="Male", 1, 2)))]
lit[, year_id:=as.integer(floor(year_start+year_end)/2)]
lit[, location_id:=as.integer(location_id)]
## get haqi
lit_and_cov<-bind_covariates(lit, cov_list=haqi_cov)
lit<-lit_and_cov[[1]]
haqi_cov_short<-lit_and_cov[[2]]
setnames(lit, haqi_cov_short, "haqi")

if(haqi_cov_short=="ldi"){
  lit[, haqi:=log(haqi)]
}


################### GET DATA  #########################################
######################################################


lit[, mid_age:=(age_start+age_end)/2]
lit[, me_factor:=as.numeric(as.factor(me_name))]

prior_data<-data.table(mid_age=mean(lit$mid_age), haqi=c(0.001), mean=c(0.001), standard_error=haqi_prior, prior=1, cv_hemo_stat="severe", me_factor=unique(lit$me_factor), is_outlier=0)
lit<-rbind(lit, prior_data,fill=T)
lit[is.na(prior), prior:=0]
lit[, severity_factor:=ifelse(cv_hemo_stat=="severe", 0, 1)]

lit_and_outliers<-lit
lit<-lit[is_outlier==0]

################### GET PREDICTION SQUARE  #########################################
######################################################

sqr<-make_square(location_set_id=35, year_start=1990, year_end=2019, by_sex=1, by_age=1,
                 covariates = NA, gbd_round_id=6)

## only keep estimation years
sqr<-sqr[year_id %in% c(seq(from=1990, to=2010, by=5), 2015, 2017, 2019)]

## get age mid points
## get age_group_ids and reformat
sqr[age_group_id==236, age_group_id:=1]
sqr[age_group_id %in% c(44, 33, 45, 46) , age_group_id:=235]

invisible(age_ids<-get_ids(table="age_group"))
suppressWarnings(age_ids[, age_start:=as.numeric(unlist(lapply(strsplit(age_ids$age_group_name, "to"), "[", 1)))])
suppressWarnings(age_ids[, age_end:=as.numeric(unlist(lapply(strsplit(age_ids$age_group_name, "to"), "[", 2)))])
age_ids[age_group_id==235, `:=` (age_start=95, age_end=99)]
age_ids[age_group_id==1, `:=` (age_start=0, age_end=4)]
age_ids[age_group_id==2, `:=` (age_start=0, age_end=28/365)]
age_ids[age_group_id==3, `:=` (age_start=29/365, age_end=154/365)]
age_ids[age_group_id==4, `:=` (age_start=155/365, age_end=364/365)]


## merge
sqr<-merge(sqr, age_ids, by=c("age_group_id"))
## drop 0-5
sqr<-sqr[age_group_id>1]
sqr[, mid_age:=(age_start+age_end)/2]

## get haqi for prediction
sqr<-bind_covariates(sqr, cov_list=haqi_cov)[[1]]
setnames(sqr, haqi_cov_short, "haqi")

if(haqi_cov_short=="ldi"){
  sqr[, haqi:=log(haqi)]
}

sqr[, severity_factor:=0]

################### STAN MODEL  #########################################
######################################################

message("Compiling stan model...")
if(link=="logit"){
  mod1<-stan_model(stan_path)
}
if(link=="probit"){
  mod1<-stan_model(stan_path_probit)
}
if(random_effect==T){
  mod2<-stan_model(stan_path_random_int)
  
}else{
  mod2<-stan_model(stan_path)
}
message("Done compiling")
haqi_pred<-quantile(sqr$haqi, probs=c(0.01, seq(from=0.05, to=0.9, by=0.05), 0.99))
age_pred<-seq(from=15, to=95, by=5)

mod_data<-list(
  option_vector=array(ifelse(use_weights, 1, 0)),
  n_lit=nrow(lit), lit_prop=lit$mean, haqi=lit$haqi, lit_mid_age=lit$mid_age,
  lit_severity=lit$severity_factor,
  n_valves=length(unique(lit$me_factor)),
  lit_v_type=lit$me_factor,
  ## plotting predictions
  n_temp_mid_age=length(age_pred), temp_mid_age=age_pred,
  n_temp_haqi=length(haqi_pred), temp_haqi=haqi_pred,
  haqi_prior=haqi_prior, weight_i=rep(1, times=nrow(lit))
)

## run fit
fit1<-sampling(mod2, mod_data, chains=2, iter=1000) #, control=list(adapt_delta=.9))


## get parameter draws for predictions
draws<-as.data.table(extract(fit1, pars=c("alpha", "betas"), permuted=T, inc_warmup=F))

draw_list<-list(
  alpha=as.matrix(draws[, names(draws) %like% "alpha", with=F]),
  beta=as.matrix(draws[, names(draws) %like% "betas", with=F])
)


################### PREDICTIONS  #########################################
######################################################


if(pred==T){
  if(random_effect==F & length(upload_mes)==2){upload_mes<-"aort"}
  for(me in upload_mes){

    ################### CONDITIONALS  #########################################
    ######################################################
    me_id<-ifelse(me=="aort", 18811, ifelse(me=="mitral", 18812, 18813))
    output_folder.t<-"FILEPATH"
    v_factor<-ifelse(me=="aort", 1, ifelse(me=="mitral", 2, 3))
    sqr[, me_factor:=v_factor]
    
    ## loop over year/sex
    for(year in unique(sqr$year_id)){
      for(s in unique(sqr$sex_id)){

        message("Predicting for year_id: ", year, "; sex_id: ", s)
        sqr.sub<-sqr[year_id==year & sex_id==s, ]
        
        
        ################### MAKE PREDICTIONS  #########################################
        ######################################################
        
        ## create prediction design matrix
        X_pred<-make_fixef_matrix(df=sqr.sub, fixefs=c("haqi", "mid_age", "severity_factor"))
        data_list<-list(X=X_pred)
        
        pred_math<-"inv_logit(alpha + X %*% beta)"
        
        start<-Sys.time()
        preds<-predict_draws(prediction_math=pred_math, draw_list=draw_list, data_list=data_list, return_draw=T)
        end<-Sys.time()
        print(end-start)
        
        setnames(preds, paste0("draw", 0:999),  paste0("draw_", 0:999))
        full_preds<-cbind(sqr.sub[, .(location_id, age_group_id)], preds)
        
        ################### UPLOAD PREDICTIONS  #########################################
        ######################################################
        
        message(" Saving...")
          if(save_upload==T){
             write.csv(full_preds, file="FILEPATH", row.names=F)
         # }
          if(random_effect==F & me=="aort"){
            write.csv(full_preds, file="FILEPATH", row.names=F)
            
          }
        }
      }
    }
    
    
    ################### SAVE RESULTS #########################################
    ######################################################
    
    if(save_upload_2==T){
      save_results_epi(input_dir=output_folder.t, input_file_pattern="{year_id}_{sex_id}_{measure_id}.csv",
                       modelable_entity_id=me_id, measure_id=18, mark_best=best, description=descr,
                       decomp_step=paste0("step", decomp_step))
      
      if(random_effect==F & me=="aort"){ ## save for mitral too
        save_results_epi(input_dir="FILEPATH",  input_file_pattern="{year_id}_{sex_id}_{measure_id}.csv",
                         modelable_entity_id=18812, measure_id=18, mark_best=best, description=descr,
                         decomp_step=paste0("step", decomp_step))
      }
    }
  }
}

