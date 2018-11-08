####################
##Author: USERNAME
##Date: 9/14/2017
##Purpose: Clean NRVD lit data and prep for dismod
##
########################


rm(list=ls())
os <- .Platform$OS.type

date<-gsub("-", "_", Sys.Date())


library(ggplot2)
library(data.table)
library(openxlsx)
library(rstan)
library(lme4)
library(boot)
library(mvtnorm)

################### PATHS AND ARGS #########################################
################################################################

input_folder<-paste0("FILEPATH")


save<-F
by_age<-T ##USERNAME: to save draws by age

stan_path<-paste0("FILEPATH")
random_stan_path<-paste0("FILEPATH")
hosp_stan_path<-paste0("FILEPATH/asympt_model.stan")

if(by_age==T){
  output<-paste0("FILEPATH")
}else{
  output<-paste0("FILEPATH")
}

plot_output<-paste0("FILEPATH")

central<-paste0("FILEPATH")


################### SCRIPTS #########################################
################################################################

source(paste0(j, "FILEPATH/utility/get_recent.R"))  ##USERNAME: my function for getting most recent data
source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_population.R"))
source(paste0(central, "get_ids.R"))
source(paste0(central, "get_age_metadata.R"))

source("FILEPATH/utility/model_helper_functions.R")
source("FILEPATH/utility/expand_pops.R")

################### GET DATA #########################################
################################################################

##USERNAME: read in data
files<-list.files(input_folder, full.names=T)
files<-files[!grepl("\\~\\$", files)] ##USERNAME: drops any temp open files; this may break the stuffs

nrvd<-rbindlist(lapply(files, function(x){
  temp<-as.data.table(read.xlsx(x, sheet="extraction", na.strings="<NA>"))[-1]
  temp[, source_file:=x]
  return(temp)
}), fill=T)


nrvd<-nrvd[!is.na(me_name)]
nrvd[, "cv*":=NULL]


##USERNAME: format/clean some columns
nrvd[is.na(cv_tx), cv_tx:=3]
nrvd[is.na(cv_only_degen), cv_only_degen:=1]
nrvd[is.na(cv_symptomatic), cv_symptomatic:=3]
nrvd[, cv_preserved_lvef:=NULL] ##USERNAME: drop this col


num_cols<-c("mean", "lower", "upper", "standard_error", "cases", "sample_size", "age_start", "age_end", "year_start", "year_end")
for(col in num_cols){
  nrvd[, (col):=as.numeric(get(col))]
}


##USERNAME: if cases/sample sizes were given, create the mean
nrvd[is.na(mean), mean:=cases/sample_size]

##USERNAME: create age start and end if mid age 
nrvd[is.na(age_start) & !is.na(mean_age) & !is.na(age_sd), 
     `:=` (age_start=mean_age-1.5*age_sd, age_end=mean_age+1.5*age_sd)]


outliers<-nrvd[is_outlier==1]
nrvd<-nrvd[is_outlier!=1 | is.na(is_outlier)]
nrvd<-nrvd[measure %in% c("prevalence", "proportion")]

################### PLOT #########################################
################################################################

p<-ggplot(data=nrvd, aes(x=(age_start+age_end)/2, y=mean))+
  geom_point(aes(color=location_id, shape=sex))+
  facet_wrap(~cv_symptomatic)+
  theme_bw()
print(p)


################### TRY W/ MOD/SEV #########################################
################################################################

cols<-c("age_start", "age_end", "sex", "year_start", "year_end", "location_id", "urbanicity_type", "nid", "me_name", "measure", grep("cv_*", names(nrvd)[!grepl("cv_hemo_stat", names(nrvd))], value=T)) 

##USERNAME: cast on hemodynamic status
to_agg<-nrvd[cv_hemo_stat %in% c("moderate", "severe"), c(cols, "seq", "mean", "cases", "sample_size", "cv_hemo_stat"), with=F]
to_agg.d<-data.table::dcast(to_agg, formula(paste0(paste0(c(cols, "sample_size"), collapse="+"), "~cv_hemo_stat")), value.var=c("mean"), subset=.(measure %in% c("prevalence", "incidence")))

##USERNAME: add proporiton from moderate to prop from severe to combine
to_agg.d[, c("moderate, severe"):=rowSums(.SD, na.rm=F), .SDcols=c("moderate", "severe")]
to_agg.d$aggd<-ifelse(is.na(to_agg.d[["moderate, severe"]]), 0, 1)
to_agg.d[, mod_prop:=moderate/(moderate+severe)]
to_agg.d[, sample_size:=sample_size*`moderate, severe`]
##s
to_agg.d[sample_size<=10, is_outlier:=1]
to_agg.d[is.na(is_outlier), is_outlier:=0]
to_agg.d[!me_name %in% c("aortic", "mitral"), is_outlier:=1]


p<-ggplot(data=to_agg.d, aes(x=(age_start+age_end)/2, y=mod_prop, color=nid))+
  geom_point()+
  theme_bw()
print(p)



locs<-get_location_metadata(location_set_id = 35)[, .(location_id, location_name)]
to_agg.d[, location_id:=as.integer(location_id)]
to_agg.d<-merge(to_agg.d, locs, by="location_id")

pdf(file=plot_output<-paste0("FILEPATH/asympt_data_scatter.pdf"))
nrow(unique(to_agg.d[!is.na(mod_prop) & is_outlier==0, .(location_id, year_start)]))
nrow(unique(to_agg.d[!is.na(mod_prop) & is_outlier==0, .(nid)]))
nrow(unique(to_agg.d[!is.na(mod_prop) & is_outlier==0, ]))


##USERNAME: plot for review week
p<-ggplot(data=to_agg.d[!is.na(mod_prop) & is_outlier==0], aes(x=(age_start+age_end)/2, y=mod_prop, color=location_name))+
  geom_point(size=3)+
  xlab("Age")+
  ylab("Proportion of moderate NRVD")+
  guides(color=guide_legend("Location"))+
  ylim(0,1)+
  theme_bw()+
  theme(text = element_text(size=20))
print(p)
dev.off()

################### STAN MODEL #########################################
################################################################
mod3<-stan_model(hosp_stan_path)

mod_data<-to_agg.d[!is.na(mod_prop) & !mod_prop %in% c(0, 1) & !nid %in% c(322328, 352743)]#& is_outlier==0,] 352759,
mod_data<-mod_data[me_name %in% c("aortic", "mitral")]

data_list<-list(N=nrow(mod_data), prop_asympt_data=mod_data$mod_prop,
                G=length(unique(mod_data$nid)), g=as.numeric(as.factor(mod_data$nid)), group_prior=1)

response<-"logit_mod_prop"
random_effects<-"nid"
fixed_effects<-"mid_age"
mod_data[, mid_age:=(age_start+age_end)/2]
mod_data[, logit_mod_prop:=logit(mod_prop)]

################### STAN MODEL WITH WEIGHTS #########################################
################################################################

##USERNAME: make prediction matricies
mod_data[, sex_id:=ifelse(sex=="Male", 1, ifelse(sex=="Female", 2, 3))]
mod_data[, year_id:=as.integer(round((year_start+year_end)/2))]
mod_data2<-expand_pops(mod_data)
mod_data2[, weight:=population/total_pop]

form<-make_formula(response=response, fixefs=fixed_effects, ranefs=random_effects)
Z<-make_ranef_matrix(mod_data2, form)
n_s<-get_ranef_lvl_counts(mod_data2, form)

X<-make_fixef_matrix(mod_data2, fixefs = fixed_effects)

##USERNAME: data list for hosp cw model
data_list2<-list(
  option_vector = array(1),
  n_i = nrow(mod_data2), n_k = length(fixed_effects), n_l = length(random_effects), 
  n_s = array(n_s),
  
  y_i = mod_data2[[response]], weight_i =mod_data2$weight,
  X_ik = X, Z_is = Z,
  
  random_priors_l = array(rep(.5, times=length(random_effects))) ##USERNAME: setting group prior depends on number of groups. w/ few groups, need a stronger prior for convergence
)

fit5<-sampling(mod3, data_list2, chains=4, iter=5000, warmup=4000, pars=c("fixefs", "ranefs"), include=F, control=list(adapt_delta=.98))

draws3<-data.table(draws=inv.logit(extract(fit5, pars="alpha", permuted=F, inc_warmup=F)))
draws3[, group_prior:="General Bayesian random effect"]


################### PREDICTIONS #########################################
####################################################################

##USERNAME: get draws
epsilon_draws<-as.data.table(extract(fit5, pars="epsilon_s", permuted=T, inc_warmup=F))
beta_draws<-as.data.table(extract(fit5, pars="beta_k", permuted=T, inc_warmup=F))
alpha_draws<-as.data.table(extract(fit5, pars="alpha", permuted=T, inc_warmup=F))
draw_list<-list(alpha=alpha_draws, betas=beta_draws) 


##USERNAME: get prediction data (same as training data here)
pred_df<-data.table(mid_age=seq(from=0, to=100, by=5), nid=0)

Z_pred<-make_ranef_matrix(pred_df, form, training_matrix = Z)
X_pred<-make_fixef_matrix(pred_df, fixefs = fixed_effects)
data_list<-list(X=X_pred, Z=Z_pred)

prediction_math<-"inv.logit(alpha + X %*% betas ) #+ Z %*% epsilons)"

predictions<-predict_draws(prediction_math = prediction_math, draw_list=draw_list, data_list = data_list,
                           return_draws=F, upper_lower=T)
pred_df<-cbind(pred_df, predictions)


################### PLOT RESULTS #########################################
################################################################
pdf(plot_output)

##USERNAME plot age-effect
p<-ggplot(data=pred_df, aes(x=mid_age, y=pred))+
  geom_line(color="cornflowerblue", size=2)+
  geom_line(aes(y=old_pred), color="forestgreen", size=2)+
  geom_ribbon(aes(ymin=old_lower, ymax=old_upper), alpha=0.4, fill="forestgreen")+
  geom_ribbon(aes(ymin=lower, ymax=upper), alpha=0.4, fill="cornflowerblue")+
  geom_point(data=mod_data, aes(y=mod_prop, color=location_name))+
  geom_errorbarh(data=mod_data, aes(y=mod_prop, xmin=age_start, xmax=age_end, color=location_name))+
  ylim(c(0,1))+
  ylab("Proportion moderate")+
  xlab("Age")+
  theme_classic()
print(p)


dev.off()

################### SAVE RESULTS #########################################
################################################################
##USERNAME: not currently uploading..
if(save==T){
  message("Saving..")
  if(by_age==F){
    set.seed(30)
    draws<-t(as.data.table(sample(draws1$draws, 1000, replace=F)))
    names(draws)<-paste0("draw_", 0:999)
    
    write.csv(draws, file=output, row.names=F)
    
    me_ids<-c(19386, 19387) ## 19386 is aortic,  19387 is mitral
  }else{
    
    ##USERNAME: get draws
    beta_draws<-as.data.table(extract(fit5, pars="beta_k", permuted=T, inc_warmup=F))
    alpha_draws<-as.data.table(extract(fit5, pars="alpha", permuted=T, inc_warmup=F))
    draw_list<-list(alpha=alpha_draws, betas=beta_draws)
    
    
    ##USERNAME: get prediction data
    pred_df<-get_age_metadata(12)
    pred_df[, mid_age:=(age_group_years_start+age_group_years_end)/2]
    
    X_pred<-make_fixef_matrix(pred_df, fixefs = fixed_effects)
    data_list<-list(X=X_pred, Z=Z_pred)
    
    prediction_math<-"inv.logit( alpha + X %*% betas )"
    
    predictions<-predict_draws(prediction_math = prediction_math, draw_list=draw_list, data_list = data_list,
                               return_draws=T, upper_lower=F)
    ##USERNAME: sample 1k random draw columns from 4k that were saved
    draws<-sample(size=1000, c(0:3999), replace = F)
    predictions<-predictions[, paste0("draw", draws), with=F]
    setnames(predictions, paste0("draw", draws), paste0("draw", 0:999))
    
    draws<-cbind(pred_df[, .(age_group_id)], predictions)
    
    write.csv(draws, file=output, row.names=F)
  }
  message("Saved")
}


