####################
##Author: USERNAME
##Date: 12/30/2017
##Purpose: Model tx for nrvd
##
########################

rm(list=ls())
os <- .Platform$OS.type

library(rstan)

library(data.table)
library(ggplot2)
library(boot)
library(ini)
library(shinystan)

date<-gsub("-", "_", Sys.Date())

################### ARGS AND PATHS #########################################
######################################################

descr<-"Fixed SE bug;"
upload_mes<-c("aort", "mitral", "other")
upload_mes<-c("mitral")
age_smooth<-2 ##USERNAME: this isn't used rn
haqi_prior<-1 ##USERNAME: this isn't used rn
random_effect<-F
haqi_cov<-"Healthcare access and quality index" #"Socio-demographic Index" ##"Healthcare access and quality index" "LDI (I$ per capita)"
link<-"logit"
use_weights<-F

pred<-F
save_upload<-F
best<-F


age_wts_path<-paste0("FILEPATH")

stan_path<-paste0("FILEPATH/nrvd_tx_model.stan")

##USERNAME: need an input folder if bypassing upload to epi
input_folder<-paste0("FILEPATH")
output_folder<-paste0("FILEPATH")
betas_output<-paste0("FILEPATH")

central<-paste0("FILEPATH")


################### SCRIPTS #########################################
######################################################

source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_ids.R"))
source(paste0(central, "get_epi_data.R"))
source(paste0(central, "save_results_epi.R"))
source(paste0("FILEPATH/utility/bind_covariates.R"))
source(paste0("FILEPATH/utility/get_recent.R"))

source(paste0("FILEPATH/db_tools.r"))
source(paste0("FILEPATH/setup.r"))

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
  ##USERNAME: fill in 
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

#lit<-get_epi_data(bundle_id=3146)
lit<-get_recent(input_folder)
lit[, sex_id:=as.integer(ifelse(sex==3, "Both", ifelse(sex=="Male", 1, 2)))]
lit[, year_id:=as.integer(floor(year_start+year_end)/2)]
lit[, location_id:=as.integer(location_id)]
##USERNAME: get haqi
lit_and_cov<-bind_covariates(lit, cov_list=haqi_cov)
lit<-lit_and_cov[[1]]
haqi_cov_short<-lit_and_cov[[2]]
setnames(lit, haqi_cov_short, "haqi")

if(haqi_cov_short=="ldi"){
  lit[, haqi:=log(haqi)]
}

plot_output<-paste0("FILEPATH/nrvd_tx_plots_", link, "_", haqi_cov_short, ".pdf")


################### GET DATA  #########################################
######################################################


lit[, mid_age:=(age_start+age_end)/2]

##USERNAME: create a factor for valve type
lit[, me_factor:=as.numeric(as.factor(me_name))]


prior_data<-data.table(mid_age=mean(lit$mid_age), haqi=c(0.001), mean=c(0.001), standard_error=haqi_prior, prior=1, cv_hemo_stat="severe", me_factor=unique(lit$me_factor), is_outlier=0)
lit<-rbind(lit, prior_data,fill=T)
lit[is.na(prior), prior:=0]
lit[, severity_factor:=ifelse(cv_hemo_stat=="severe", 0, 1)]

lit_and_outliers<-lit
lit<-lit[is_outlier==0]

################### GET PREDICTION SQUARE  #########################################
######################################################

locs<-get_location_metadata(location_set_id=35)
l_set_v_id<-unique(locs$location_set_version_id)

sqr<-make_square(location_set_version_id=l_set_v_id, year_start=1990, year_end=2017, by_sex=1, by_age=1,
                 covariates = NA)

##USERNAME: only keep estimation years
sqr<-sqr[year_id %in% c(seq(from=1990, to=2010, by=5), 2017)]

##USERNAME: get age mid points
##USERNAME: get age_group_ids and reformat
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


##USERNAME: merge
sqr<-merge(sqr, age_ids, by=c("age_group_id"))
##USERNAME: drop 0-5
sqr<-sqr[age_group_id>1]
sqr[, mid_age:=(age_start+age_end)/2]

##USERNAME: get haqi for prediction
sqr<-bind_covariates(sqr, cov_list=haqi_cov)[[1]]
setnames(sqr, haqi_cov_short, "haqi")

if(haqi_cov_short=="ldi"){
  sqr[, haqi:=log(haqi)]
}

sqr[, severity_factor:=0]

################### STAN MODEL  #########################################
######################################################
message("Compiling stan model...")

mod2<-stan_model(stan_path)

message("Done compiling")
##USERNAME: decide which levels of haqi and age midpoints to predict out
haqi_pred<-quantile(sqr$haqi, probs=c(0.01, seq(from=0.05, to=0.9, by=0.05), 0.99))
age_pred<-seq(from=15, to=95, by=5)


if(pred==T){
  if(random_effect==F & length(upload_mes)==2){upload_mes<-"aort"} ##USERNAME: if not running valve-specific models, just run models once and duplicate draws
  for(me in upload_mes){
    ################### CONDITIONALS  #########################################
    ######################################################
    me_id<-ifelse(me=="aort", 18811, ifelse(me=="mitral", 18812, 18813))
    output_folder.t<-paste0(output_folder, me, "/prop_tx_draws/")
    v_factor<-ifelse(me=="aort", 1, ifelse(me=="mitral", 2, 3))
    sqr[, me_factor:=v_factor]
    
    ##USERNAME: loop over year/sex
    for(year in unique(sqr$year_id)){
      for(s in unique(sqr$sex_id)){
      if(F){
        year<-1990
        s<-1
      }
        message("Modelling for year_id: ", year, "; sex_id: ", s)
        sqr.sub<-sqr[year_id==year & sex_id==s, ]
        
        ################### RUN MODEL  #########################################
        ######################################################
        
        mod_data<-list(##USERNAME: lit data (training)
          option_vector=array(ifelse(use_weights, 1, 0)),
                       n_lit=nrow(lit), lit_prop=lit$mean, haqi=lit$haqi, lit_mid_age=lit$mid_age,
                       lit_severity=lit$severity_factor,
                       n_valves=length(unique(lit$me_factor)),
                       lit_v_type=lit$me_factor,
                       ##USERNAME: plotting predictions
                       n_temp_mid_age=length(age_pred), temp_mid_age=age_pred,
                       n_temp_haqi=length(haqi_pred), temp_haqi=haqi_pred,
                       haqi_prior=haqi_prior, weight_i=rep(1, times=nrow(lit)),
                       ##USERNAME: full predictions
                       n_pred=nrow(sqr.sub), pred_haqi=sqr.sub$haqi, pred_mid_age=sqr.sub$mid_age,
                       pred_severity=sqr.sub$severity_factor,
                       pred_v_type=sqr.sub$me_factor
                       )
        
        fit1<-sampling(mod2, mod_data, chains=2, iter=1000) 
        

        
        ################### UPLOAD PREDICTIONS  #########################################
        ######################################################
        message(" Saving...")
        full_preds<-as.data.table(t(rstan::extract(fit1, pars="full_pred")[[1]]))
        setnames(full_preds, names(full_preds), paste0("draw_", 0:999))
        full_preds<-cbind(sqr.sub[, .(location_id, age_group_id)], full_preds)
        ##full_preds[, measure:=5] ##USERNAME: mark as prevalence
        if(save_upload==T){
          ##USERNAME: save

            write.csv(full_preds, file=paste0(output_folder.t, year, "_", s, "_18.csv"), row.names=F)

          if(random_effect==F & me=="aort"){##USERNAME: write for mitral too
            write.csv(full_preds, file=paste0(output_folder, "mitral/prop_tx_draws/", year, "_", s, "_18.csv"), row.names=F)
            
          }
        }
      }
    }
    
    
    ################### SAVE RESULTS #########################################
    ######################################################
    if(save_upload==T){
      save_results_epi(input_dir=output_folder.t, input_file_pattern="{year_id}_{sex_id}_{measure_id}.csv",
                       modelable_entity_id=me_id, measure_id=18, mark_best=best, description=descr)
      if(random_effect==F & me=="aort"){##USERNAME: save for mitral too
        save_results_epi(input_dir=paste0(output_folder, "mitral/prop_tx_draws/"),  input_file_pattern="{year_id}_{sex_id}_{measure_id}.csv",
                         modelable_entity_id=18812, measure_id=18, mark_best=best, description=descr)
      }
    }
  }
}else{
  
  ################### TEST W/O PREDS #########################################
  ######################################################
  
  message("Testing model w/o predictions..")
  year<-1990
  s<-1
  sqr.sub<-sqr[year_id==year & sex_id==s, ]
  sqr.sub[, me_factor:=1]
  sqr.sub<-sqr.sub[0,] ##USERNAME: drop all rows so don't predict

  
  
  mod_data<-list(##USERNAME: lit data (training)
    option_vector=array(as.integer(ifelse(use_weights, 2, 0))),
    n_lit=nrow(lit), lit_prop=lit$mean, haqi=lit$haqi, lit_mid_age=lit$mid_age,
    lit_severity=lit$severity_factor,
    n_valves=length(unique(lit$me_factor)),
    lit_v_type=lit$me_factor,
    ##USERNAME: plotting predictions
    n_temp_mid_age=length(age_pred), temp_mid_age=age_pred,
    n_temp_haqi=length(haqi_pred), temp_haqi=haqi_pred,
    haqi_prior=haqi_prior, weight_i=rep(1, times=nrow(lit)),
    ##USERNAME: full predictions
    n_pred=nrow(sqr.sub), pred_haqi=sqr.sub$haqi, pred_mid_age=sqr.sub$mid_age,
    pred_severity=sqr.sub$severity_factor,
    pred_severity=sqr.sub$severity_factor,
    pred_v_type=sqr.sub$me_factor
    
  )
  
  fit1<-sampling(mod2, mod_data, chains=2, iter=1000)
  


  ################### PLOT FIT  #########################################
  ######################################################
  
  
  lit_pred<-as.data.table(summary(fit1, pars="prop_est")$summary)
  lit_pred<-lit_pred[, .(mean, sd)]
  setnames(lit_pred, c("mean", "sd"), c("pred", "pred_se"))
  lit<-cbind(lit, lit_pred)

  
  ##USERNAME: clean and save betas
  alpha<-as.data.table(summary(fit1, pars="alpha")$summary)
  betas<-as.data.table(summary(fit1, pars="betas")$summary)
  betas<-rbind(alpha, betas)
  betas<-betas[, .(mean, sd, `2.5%`, `97.5%` )]
  betas[, `:=` (exp_mean=exp(mean), exp_lwr=exp(`2.5%`), exp_upr=exp(`97.5%`))]
  betas[, par_name:=c("Intercept", haqi_cov_short, "mid_age", "mod_severe")]
  betas[, mod_descr:=descr]
  write.csv(betas, file=betas_output, row.names=F)
  
  
  if(random_effect==T){
    aort_pred<-extract_stan_matrix("age_pred", "haqi_pred", "haqi_preds_aort", fit1)
    aort_pred[, valve:="aort"]
    mitral_pred<-extract_stan_matrix("age_pred", "haqi_pred", "haqi_preds_mitr", fit1)
    mitral_pred[, valve:="mitral"]
    all_pred<-extract_stan_matrix("age_pred", "haqi_pred", "haqi_preds_all", fit1)
    all_pred[, valve:="all"]
    
    preds<-rbind(aort_pred, mitral_pred, all_pred)
  }else{
    
    preds<-extract_stan_matrix("age_pred", "haqi_pred", "haqi_preds", fit1)
  }

  
  pdf(file=plot_output, width=8)
  
  if(random_effect==T){
  
    
  }else{
    p<-ggplot(data=preds, aes(x=haqi_pred))+
      geom_ribbon(aes(ymin=pred-2*pred_sd, ymax=pred+2*pred_sd, fill=factor(age_pred)), alpha=.2)+
      geom_line(aes(y=pred, color=factor(age_pred), group=factor(age_pred)))+
      geom_point(data=lit, aes(y=mean, x=haqi, shape=factor(prior), color=me_name))+
      ggtitle("Treatment model")+
      xlab("HAQ Index")+
      ylab("Proportion treated")+
      theme_bw()
    print(p)
    
    ##USERNAME: plot over age by haqi
    p<-ggplot(data=preds, aes(x=age_pred))+
      geom_ribbon(aes(ymin=pred-2*pred_sd, ymax=pred+2*pred_sd, fill=factor(haqi_pred)), alpha=.2)+
      geom_line(aes(y=pred, color=factor(haqi_pred), group=factor(haqi_pred)))+
      geom_point(data=lit, aes(y=mean, x=mid_age, shape=factor(prior)))+
      ggtitle("Treatment model over age")+
      xlab("Age")+
      ylab("Proportion treated")+
      theme_bw()
    print(p)
    
  
    ##USERNAME: plot adjusted mod+severe data points
    lit[severity_factor==1, new_mean:=inv.logit(logit(mean)/betas[3, mean])]
    lit[is.na(new_mean), new_mean:=mean]
    
    
    ##USERNAME: plot data w/o results and prior
    p<-ggplot()+
      geom_point(data=lit[prior!=1], aes(y=new_mean, x=haqi), size=2.5)+
      ggtitle("Treatment data")+
      xlab(haqi_cov)+
      ylab("Proportion treated")+
      ylim(0,1)+
      xlim(0, 100)+
      scale_size_discrete(name="Prior", labels=c("", "Prior"))+
      theme_bw()+
      theme(text = element_text(size=20))
    print(p)
    
    ##USERNAME: plot data w/o results, add in prior prior
    p<-ggplot()+
      geom_point(data=lit[prior!=1], aes(y=new_mean, x=haqi), size=2.5)+
      geom_point(data=lit[prior==1], aes(y=new_mean, x=haqi), size=2.5, color="black", shape=17)+
      ggtitle("Treatment data, with prior")+
      xlab(haqi_cov)+
      ylab("Proportion treated")+
      ylim(0,1)+
      xlim(0, 100)+
      scale_size_discrete(name="Prior", labels=c("", "Prior"))+
      theme_bw()+
      theme(text = element_text(size=20))
    print(p)
    
    ##USERNAME: plot adjusted data 
    p<-ggplot(data=preds[age_pred %in% c(min(age_pred), 85, median(age_pred))], aes(x=haqi_pred))+
      geom_ribbon(aes(ymin=pred-2*pred_sd, ymax=pred+2*pred_sd, fill=factor(age_pred)), alpha=.2)+
      geom_line(aes(y=pred, color=factor(age_pred)))+
      geom_point(data=lit[prior!=1], aes(y=new_mean, x=haqi), size=2.5)+
      geom_point(data=lit[prior==1], aes(y=new_mean, x=haqi), size=2.5)+
      ggtitle("Treatment model, with adjusted data")+
      xlab(haqi_cov)+
      ylab("Proportion treated")+
      scale_color_discrete(name="Age")+
      scale_fill_discrete(name="Age")+
      scale_size_discrete(name="Prior", labels=c("", "Prior"))+
      theme_bw()+
      theme(text = element_text(size=20))
    print(p)
    
    ##USERNAME: plot over age
    p<-ggplot(data=preds[haqi_pred %in% c(quantile(haqi_pred, probs = 0.1), median(haqi_pred), max(haqi_pred), 80)], aes(x=age_pred))+
      geom_ribbon(aes(ymin=pred-2*pred_sd, ymax=pred+2*pred_sd, fill=factor(round(haqi_pred))), alpha=.2)+
      geom_line(aes(y=pred, color=factor(round(haqi_pred))))+
      geom_point(data=lit[prior!=1], aes(y=new_mean, x=mid_age, size=haqi))+
      xlab("Age")+
      ylab("Proportion treated")+
      scale_color_discrete(name=haqi_cov)+
      scale_fill_discrete(name=haqi_cov)+
      theme_bw()+
      theme(text = element_text(size=20))
    print(p)
    
  }
  
  dev.off()

}
