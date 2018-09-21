################################
##author:USERNAME
##date: 4/10/2017
##purpose:    -Model SD
##
## source("FILEPATH")
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

date<-gsub("-", "_", Sys.Date())


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
library(mvtnorm, lib.loc=lib_path)


################### ARGS AND PATHS #########################################
######################################################

me<-"sbp"
message("Modelling sd for ", me)
create_draws<-F
message("Create draws is ", create_draws)
convert_se<-F


input<-paste0(j, "FILEPATH/", me, "_to_sd/")  
plot_output<-paste0(j, "FILEPATH", me, "_sd_model_", date, ".pdf") 
output_folder<-paste0(j, "FILEPATH", me, "_", date, "/")

##USERNAME: central
age_wts<-paste0(j, "FILEPATH")
central<-paste0(j, "FILEPATH")



################### CONDITIONALS #########################################
######################################################

if(me=="sbp"){
  me_dis<-"hypertension"
  meid<-2547
}

if(me=="chl"){
  me_dis<-"hypercholesterolemia"
  meid<-2546
}




################### SCRIPTS #########################################
######################################################

source(paste0(j, "FILEPATH/get_recent.R")) 
source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_model_results.R"))
source(paste0(j, "FILEPATH/bind_covariates.R"))


################### GET SD DATA #########################################
######################################################

locs<-get_location_metadata(version_id=149)[,.(location_id, super_region_name, region_name)]
full<-get_recent(input)


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


################### LOOP OVER MODELS #########################################
######################################################

##USERNAME; manually set a formula
forms<-c("log(sd)~mean+factor(age_group_id)+factor(sex_id)")
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
    geom_point(aes(color=factor(super_region_name)))+
    geom_point(aes(x=mean, y=sd), alpha=0.1)+
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


##USERNAME:save winning model for imputing variance
save(winmod, file=paste0(j, "FILEPATH", me, "_sd_mod.Rdata"))


################### APPLY REGRESSION #########################################
######################################################
message("Applying regression")

results<-get_model_results("epi", meid, location_set_version_id=149)

results<-results[!age_group_id %in% c(22, 27)]
results<-merge(results, locs, by="location_id")
results<-results[super_region_name!=""]
results[, mean2:=mean^2]

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
  stand_results.s<-stand_results[sex_id==sex  & year_id==2016, ]
  results.s<-results[sex_id==sex]
  p<-ggplot(data=stand_results.s, aes(x=stndrd_mean, y=stndrd_sd, color=super_region_name))+
    geom_point()+
    ggtitle(paste("Scatter of predicted SD vs mean, best mod, sex_id:", sex, ", age standardized, 2016"))+
    #facet_wrap(~age_group_id)+
    theme_classic()+
    theme(legend.position="top", legend.key.width=unit(0.5, "cm"))
  print(p)
  
  stand_results.s2<-stand_results2[sex_id==sex  & year_id==2016, ]
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
if(create_draws){
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
    
    write.csv(results.t, file=paste0(output, loc, ".csv"), row.names=F)
    
  }
}




