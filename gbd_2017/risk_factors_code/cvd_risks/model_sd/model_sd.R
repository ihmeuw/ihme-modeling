################################
##author:USERNAME
##date: 4/10/2017
##purpose:    -Model SD
##
#######################################

rm(list=ls())
os <- .Platform$OS.type


date<-gsub("-", "_", Sys.Date())

library(data.table)
library(lme4)
library(MuMIn)
library(boot)
library(ggplot2)
library(rhdf5)
#library(arm, lib.loc=lib_path)
library(haven)
library(plyr)
library(dplyr)
library(stringr)
library(DBI)
library(RMySQL)
library(mvtnorm)
library(RColorBrewer)

################### ARGS AND PATHS #########################################
######################################################

me<-"ldl"
pool_oldage<-F
create_draws<-T
save_results<-T
descr<-"Using log(mean) transformed covariate"
best<-F
message("Create draws is ", create_draws)
convert_se<-F

country_covs<-c("Socio-demographic Index", "Alcohol (liters per capita)",
            "nuts seeds adjusted(g)",
            "Mean BMI", "Prevalence of obesity", "Healthcare access and quality index","Smoking Prevalence")

input_folder<-paste0("FILEPATH/", me, "_to_sd/")  
plot_output<-paste0("FILEPATH/", me, "_sd_model_", date, ".pdf") 
review_plot<-paste0("FILEPATH/", me,"_sd_results_plot.pdf")
output_folder<-paste0("FILEPATH/", me, "_", date, "/")

##USERNAME: central
age_wts<-paste0("FILEPATH/age_weights.csv")
usual_bp_path<-paste0("FILEPATH/2016_usual_bp_ratios.csv")
central<-paste0("FILEPATH")



################### CONDITIONALS #########################################
######################################################

if(me=="sbp"){
  me_dis<-"hypertension"
  meid<-2547
  sd_me_id<-15788
}

if(me=="chl"){
  me_dis<-"hypercholesterolemia"
  meid<-2546
}
if(me=="ldl"){
  me_dis<-"hypercholesterolemia"
  meid<-18822
  sd_me_id<-18823
}

################### UTILITY FUNCTION #########################################
######################################################
get_ci<-function(mod, interval=.95){
  ##USERNAME: pull out coefs
  coefs<-summary(mod)$coef
  variable<-row.names(coefs)
  coefs<-as.data.table(coefs)
  coefs<-cbind(variable, coefs)
  
  
  ##USERNAME: get model type names
  if(class(mod)=="lm"){
    se_colname<-"Std. Error"
    est_colname<-"Estimate"
  }
  
  setnames(coefs, c(est_colname, se_colname, grep("Pr(>|*|)", names(coefs), value=T)), c("coef", "se", "p_val"))
  coefs<-coefs[, .(variable, coef, se, p_val)]
  
  ##USERNAME: calculate upper and lower
  z<-abs(qnorm((1-interval)/2))
  coefs[, `:=` (lower=coef-z*se, upper=coef+z*se)]
  ##USERNAME: put in HR space
  coefs[, `:=` (exp_mean=exp(coef), exp_lower=exp(lower), exp_upper=exp(upper))]
  return(coefs)
}

################### SCRIPTS #########################################
######################################################

source(paste0("FILEPATH/utility/get_recent.R"))  ##USERNAME: my function
source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_model_results.R"))
source(paste0(central, "save_results_epi.R"))
source(paste0("FILEPATH/utility/bind_covariates.R"))


################### GET SD DATA #########################################
######################################################
message("Modelling sd for ", me)

locs<-get_location_metadata(location_set_id = 22)[,.(location_id, super_region_name, region_name)]
full<-get_recent(input_folder)


################### TAB DATA FOR REVIEW WEEK #########################################
######################################################

nrow(full)
data_counts<-unique(full[, .(nid, cv_lit)])

################### MODEL #########################################
######################################################
###################################################

################### CONSTRUCT FORMULAS #########################################
######################################################
cov_list<-c("sdi", "prev_overweight")

#full<-bind_covariates(df=full, cov_list=cov_list)


################### CONSTRUCT FORMULAS #########################################
######################################################
covs<-c("factor(age_group_id)", "factor(sex_id)", "factor(super_region_name)", "rt_mean", "(1|super_region_name)")

combos<-lapply(1:length(covs), function(x){
  as.data.table(combn(covs, m=x))
})


forms<-unlist(lapply(combos, function(x){
  lapply(x, function(z){
    paste(c("log(sd)"), paste("mean", paste0(z, collapse="+"), sep="+"), sep="~")  ##USERNAME: try out both sd and log(sd). always inlcude mean
  })
})) %>% unname



################### CREATE NEW VARS + DEFINE WEIGHTS #########################################
######################################################

##USERNAME: not currently applying these weights
full[, wt:=sample_size]
full[smaller_site_unit==1, wt:=wt*.9]  ##10% decrease in weight for smaller site units


full[, coef_var:=sd/mean]



##USERNAME: add mean^2 as a covariate
full[, rt_mean:=sqrt(mean)]
full[, log_mean:=log(mean)]

##USERNAME: remap age for predictions
if(pool_oldage==T){
  full[, age_group_orig:=age_group_id]
  full[age_group_id>20, age_group_id:=21]
}


################### TEST RANDOM FOREST #########################################
######################################################
if(F){
  
  rf_data<-copy(full)
  rf_data[, c("V1", "me_name", "location_id", "coef_var", "level", "location_id", "ihme_loc_id", "nid"):=NULL]
  
  rf_data_and_covs<-bind_covariates(df=rf_data, cov_list=country_covs)
  rf_data<-rf_data_and_covs[[1]]

  charcols<-sapply(rf_data, FUN=is.character)
  rf_data[, c(names(charcols)[charcols]):=lapply(.SD, as.factor), .SDcols=names(charcols)[charcols]]
  
  rfmod<-randomForest(sd~., data=rf_data[complete.cases(rf_data)], importance=T)
  
  varImpPlot(rfmod,
             sort = T,
             main="Variable Importance")
  
  preds<-predict(rfmod)
  rf_preds<-cbind(rf_data[complete.cases(rf_data)], preds)
}

################### LOOP OVER MODELS #########################################
######################################################

##sy; manually set a formula
forms<-c("log(sd)~mean+factor(age_group_id)+factor(sex_id)")
forms<-c("log(sd)~log_mean+factor(age_group_id)+factor(sex_id)")
#forms<-c("log(sd)~factor(age_group_id)+factor(sex_id)+(mean|age_group_id)+factor(region_name)")

modsums<-list()
for(i in 1:length(forms)){
  message("Modelling formula ", i, " of ", length(forms))
  if(F){
    i<-7
  }
  form<-forms[i]
  variates<-as.character(as.formula(form))[3]
  outcome<-as.character(as.formula(form))[2]
  ##USERNAME: only running simple lm currently
  # if(by_sex==T){
  #   
  # }
  # sex_list<-list()
  # for(s in c(1,2)){ ##USERNAME: loop over sex id
    if(grepl("\\|", form)==T){
      mod<-lmer(formula=as.formula(form), data=full, weights=wt)
      fit<-predict(mod)
    }else{
      #mod<-lm(formula=as.formula(form), data=full, weights=wt)
      mod<-lm(formula=as.formula(form), data=full) ##USERNAME: unweighted
      ##USERNAME: get in sample rmse
      preds<-predict.lm(mod, interval="confidence", weights=wt)
    }
    temp<-cbind(full, preds)
    
    
    if(outcome=="log(sd)"){
      temp[, fit:=exp(fit)]
    }
    
    temp[, diff:=sd-fit]
    is.rmse<-sqrt(mean(temp$diff^2))
    
    ##USERNAME: get betas
    if(grepl("\\|", form)==T){
      betas<-fixef(mod)
    }else{
      betas<-coef(mod)
    }
    
    
    p<-ggplot(data=temp, aes(x=mean, y=fit))+
      geom_point(aes(x=mean, y=sd), alpha=0.1)+
      geom_point(aes(color=factor(super_region_name)))+
      ggtitle(paste("Covariates:", variates, "\nis.rmse:", round(is.rmse, digits=3)))+
      facet_wrap(~age_group_id)+
      ylab(outcome)+
      theme_classic()
    
    resid<-ggplot(data=temp, aes(x=sd, y=fit))+
      geom_point(aes(color=factor(super_region_name)))+
      geom_abline(aes(slope=1, intercept=0))+
      ylim(0, max(temp$sd))+
      ylab("Residual")+
      xlab("Observed SD")+
      facet_wrap(~age_group_id)+
      theme_classic()+
      theme(legend.position="top", legend.key.width=unit(0.5, "cm"))
    
    
  #}
  if(grepl("\\|", form)==T){
    mod<-lmer(formula=as.formula(form), data=full, weights=wt)
    fit<-predict(mod)
  }else{
    #mod<-lm(formula=as.formula(form), data=full, weights=wt)
    mod<-lm(formula=as.formula(form), data=full) ##USERNAME: unweighted
    ##USERNAME: get in sample rmse
    preds<-predict.lm(mod, interval="confidence", weights=wt)
  }
  temp<-cbind(full, preds)
  

  if(outcome=="log(sd)"){
    temp[, fit:=exp(fit)]
  }
  
  temp[, diff:=sd-fit]
  is.rmse<-sqrt(mean(temp$diff^2))
  
  ##USERNAME: get betas
  if(grepl("\\|", form)==T){
    betas<-fixef(mod)
  }else{
    betas<-coef(mod)
  }
  
  
  p<-ggplot(data=temp, aes(x=mean, y=fit))+
    geom_point(aes(x=mean, y=sd), alpha=0.1)+
    geom_point(aes(color=factor(super_region_name)))+
    ggtitle(paste("Covariates:", variates, "\nis.rmse:", round(is.rmse, digits=3)))+
    facet_wrap(~age_group_id)+
    ylab(outcome)+
    theme_classic()
  
  resid<-ggplot(data=temp, aes(x=sd, y=fit))+
    geom_point(aes(color=factor(super_region_name)))+
    geom_abline(aes(slope=1, intercept=0))+
    ylim(0, max(temp$sd))+
    ylab("Residual")+
    xlab("Observed SD")+
    facet_wrap(~age_group_id)+
    theme_classic()+
    theme(legend.position="top", legend.key.width=unit(0.5, "cm"))
  
    
    
  modsums[[i]]<-list(is.rmse, form, betas, p, mod, resid)
  
  
  
}


##don't print modsums! will print all the plots out and take a long time
rmses<-unlist(lapply(modsums, "[", 1))

winform<-modsums[rmses==min(rmses)][[1]][2]
winplot<-modsums[rmses==min(rmses)][[1]][4]
winbetas<-modsums[rmses==min(rmses)][[1]][3]
winmod<-modsums[rmses==min(rmses)][[1]][5][[1]]
winresid<-modsums[rmses==min(rmses)][[1]][6]

lastform<-modsums[length(modsums)][[1]][2]
lastplot<-modsums[length(modsums)][[1]][4]
lastbetas<-modsums[length(modsums)][[1]][3]
lastmod<-modsums[length(modsums)][[1]][5][[1]]
lastresid<-modsums[length(modsums)][[1]][6]

if(best==T){
  ##USERNAME:save winning model for imputing variance
  save(list=c("winmod", "winform"), file=paste0(j, "temp/syadgir/", me, "_sd_mod.Rdata"))
}


################### PLOT FOR REVIEW WEEK #########################################
######################################################
if(F){
  ##USERNAME: gen new data and predict
  if(pool_oldage==T){
    remap_ages<-setdiff(unique(full$age_group_orig), unique(full$age_group_id))
    
    pred_age_grps<-c(unique(full$age_group_id), rep(21, times=length(remap_ages)-1))
  }else{
    pred_age_gprs<-unique(full$age_group_id)
  }
  new_data<-data.table(age_group_id=rep(pred_age_grps, times=2), 
                       sex_id=c(rep(1, times=length(pred_age_grps)), rep(2, times=length(pred_age_grps))),
                       mean=mean(full$mean))
  
  if(pool_oldage==T){new_data[, age_group_orig:=unique(full$age_group_orig)]}
  preds<-exp(predict.lm(winmod, interval="confidence", newdata = new_data))
  preds<-cbind(new_data, preds)
  
  ##USERNAME: remap age group id to age
  preds[, age_start:=5*age_group_id-25]
  preds[age_group_id==30, age_start:=80]
  preds[age_group_id==31, age_start:=85]
  preds[age_group_id==32, age_start:=90]
  preds[age_group_id==235, age_start:=95]
  preds[, sex_char:=ifelse(sex_id==1, "Male", "Female")]
  
  
  ##USERNAME:plot
  pdf(file=review_plot, width=11.5)
  p<-ggplot(data=preds[age_start>=15], aes(x=age_start, y=fit))+
    geom_line(aes(color=sex_char), size=1.5)+
    geom_ribbon(aes(ymin=lwr, ymax=upr, fill=sex_char), alpha=0.5)+
    facet_wrap(~sex_char)+
    guides(color="none", fill="none")+
    scale_color_brewer(palette="Set1")+
    xlab("Age")+
    ylab("Estimated SD")+
    theme_bw()+
    theme(text = element_text(size=17))
  print(p)
  dev.off()
  
}

################### APPLY REGRESSION #########################################
######################################################
message("Applying regression")

results<-get_model_results("epi", meid, location_set_id=22, sex_id=c(1,2))

results<-results[!age_group_id %in% c(22, 27)]
results<-merge(results, locs, by="location_id")
results<-results[super_region_name!=""]
results[, mean2:=mean^2]
results[, log_mean:=log(mean)]

preds<-predict.lm(winmod, newdata=results, interval="confidence")
preds<-as.data.table(preds)
preds[, fit:=exp(fit)]
results<-cbind(results, preds)

preds2<-exp(predict(lastmod, newdata=results))
results<-cbind(results, preds2)


pdf(plot_output)
print(winplot)
print(winresid)

print(lastplot)
print(lastresid)



################### AGE STANDARDIZE RESULTS #########################################
######################################################

message("Diagnosing results")  
##USERNAME: age weights for age-standardizing
wts<-fread(age_wts)[gbd_round_id==4, .(age_group_id, age_group_weight_value)]
wts<-wts[age_group_id %in% unique(results$age_group_id)]

##USERNAME: rescale weights
rescaler<-1/sum(wts$age_group_weight_value)
wts[, rescaled:=age_group_weight_value*rescaler]
wts[, age_group_weight_value:=NULL]



##USERNAME:age-standardize sd
results<-merge(results, wts, by=c("age_group_id"))
results[, wtd_sd:=rescaled*fit]
results[, wtd_sd2:=rescaled*preds2]
stand_results<-results[, .(stndrd_sd=sum(wtd_sd)), by=c("sex_id", "year_id", "location_id")]
stand_results2<-results[, .(stndrd_sd=sum(wtd_sd2)), by=c("sex_id", "year_id", "location_id")]



##USERNAME:age-standardize mean
results[, wtd_mean:=rescaled*mean]
stand_mean<-results[, .(stndrd_mean=sum(wtd_mean)), by=c("sex_id", "year_id", "location_id")]


stand_results<-merge(stand_results, stand_mean, by=c("sex_id", "year_id", "location_id"))
stand_results<-merge(stand_results, locs[, .(location_id, super_region_name)], by="location_id" )
stand_results2<-merge(stand_results2, stand_mean, by=c("sex_id", "year_id", "location_id"))
stand_results2<-merge(stand_results2, locs[, .(location_id, super_region_name)], by="location_id" )




for(sex in c(1,2)){
  stand_results.s<-stand_results[sex_id==sex  & year_id==2017, ]
  results.s<-results[sex_id==sex]
  p<-ggplot(data=stand_results.s, aes(x=stndrd_mean, y=stndrd_sd, color=super_region_name))+
    geom_point()+
    ggtitle(paste("Scatter of predicted SD vs mean, best mod, sex_id:", sex, ", age standardized, 2016"))+
    #facet_wrap(~age_group_id)+
    theme_classic()+
    theme(legend.position="top", legend.key.width=unit(0.5, "cm"))
  print(p)
  
  stand_results.s2<-stand_results2[sex_id==sex  & year_id==2017, ]
  results.s<-results[sex_id==sex]
  p<-ggplot(data=stand_results.s2, aes(x=stndrd_mean, y=stndrd_sd, color=super_region_name))+
    geom_point()+
    ggtitle(paste("Scatter of predicted SD vs mean, last, sex_id:", sex, ", age standardized, 2016"))+
    #facet_wrap(~age_group_id)+
    theme_classic()+
    theme(legend.position="top", legend.key.width=unit(0.5, "cm"))
  print(p)
  
  p<-ggplot(data=results.s, aes(x=mean))+
    geom_histogram(color="black", aes(fill=factor(super_region_name)))+
    ggtitle(paste("Histogram of predicted mean, sex_id:", sex))+
    facet_wrap(~age_group_id)+
    theme_classic()+
    theme(legend.position="top", legend.key.width=unit(0.5, "cm"))
  print(p)
  
  p<-ggplot(data=results.s, aes(x=fit))+
    geom_histogram(color="black", aes(fill=factor(super_region_name)))+
    ggtitle(paste("Histogram of predicted SD, best mod, sex_id:", sex))+
    facet_wrap(~age_group_id)+
    theme_classic()+
    theme(legend.position="top", legend.key.width=unit(0.5, "cm"))
  print(p)
  
  p<-ggplot(data=results.s, aes(x=preds2))+
    geom_histogram(color="black", aes(fill=factor(super_region_name)))+
    ggtitle(paste("Histogram of predicted SD, last mod, sex_id:", sex))+
    facet_wrap(~age_group_id)+
    theme_classic()+
    theme(legend.position="top", legend.key.width=unit(0.5, "cm"))
  print(p)
  
}
dev.off()



################### CREATE DRAWS AND SAVE RESULTS #########################################
######################################################

#results<-results[, .(age_group_id, location_id, year_id, sex_id, fit, measure_id)]
#results<-results[, .(age_group_id, location_id, year_id, sex_id, sd, measure_id)]
start<-Sys.time()
if(create_draws){
  dir.create(output_folder)
  
  usual_bp<-fread(usual_bp_path)
  setnames(results, "fit", "sd")
  
  
  n.sims<-1000
  for(loc in unique(results$location_id)){
    message("Getting draws for ", loc)
    results.l<-results[location_id==loc]
    #betas <- mvrnorm(1000, mu = coef(lastmod), Sigma = matrix(vcov(lastmod))) %>% as.data.table
    if (n.sims > 0) fe <- rmvnorm(n.sims, winmod$coefficients, vcov(winmod)) %>% t
    X <- model.matrix(formula(winmod), model.frame(winmod, data=results.l))[, rownames(fe)]
    pred <- exp(X %*% fe)
    #if (n.sims > 0) pred <- lapply(1:n.sims, function(p) as.numeric(pred[,p] >= runif(nrow(pred), 0, 1)))
    results.t<-results.l[, .(age_group_id, location_id, year_id, sex_id, measure_id)]
    results.t<-cbind(results.t, pred)
    setnames(results.t, paste0("V", 1:1000), paste0("draw_", 0:999))
    ##USERNAME:apply SD adjustment factors of me is sbp
    if(me=="sbp"){
      results.t<-merge(results.t, usual_bp, by="age_group_id") ##USERNAME: added this 1/9/2017
      results.t[, paste0("draw_", 0:999):=lapply(0:999, function(x){
        get(paste0("draw_", x))*ratio
      })]
      results.t[ ,ratio:=NULL]
    }
    write.csv(results.t, file=paste0(output_folder, loc, ".csv"), row.names=F)
    
  }
}
end<-Sys.time()
message("Creating SD draws took ", end-start)

if(save_results==T){
  save_results_epi(input_dir=output_folder, input_file_pattern="{location_id}.csv",
                   modelable_entity_id=sd_me_id, description=descr, mark_best=best, measure_id = 19)

}


