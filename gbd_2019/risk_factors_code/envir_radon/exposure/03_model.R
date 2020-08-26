
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 06/20/2019
# Purpose: Model mean and SD of radon distributions
#          
# source("FILEPATH.R", echo=T)
#***************************************************************************

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  } else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  }


# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","ggplot2","lme4","mvtnorm")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

draw_nums <- 1:1000
draw_nums_gbd <- 0:999

date <- format(Sys.Date(), "%m%d%y")
crosswalk_date <- "101019"

decomp <- "step4"

years <- c(1990:2019)
ages <- c(2:20,30,31,32,235)
sexes <- c(1,2)

save_exposure <- T
save_sd <- T

desc <- "annual save"

# Directories -------------------------------------------------------------

home_dir <- ("FILEPATH")

#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

#get covariates
source(file.path(central_lib,"FILEPATH.R"))
#get populations
source(file.path(central_lib,"FILEPATH.R"))
#bundle functions
source(file.path(central_lib,"FILEPATH.R"))
#get model results
source(file.path(central_lib,"FILEPATH.R"))
#save results epi
source(file.path(central_lib,"FILEPATH.R"))

#locations
source(file.path(central_lib,"FILEPATH.R"))
locations <- get_location_metadata(location_set_id=22, gbd_round_id=6)
locs <- locations[is_estimate==1]
std_locs <- get_location_metadata(location_set_id=101, gbd_round_id=6)

#for reproducibility
set.seed(143)

# Open PDF to save plots
pdf(paste0(home_dir,"FILEPATH",decomp,"FILEPATH",date,".pdf"),height=8.5,width=11)


# Read in data ------------------------------------------------------------


dt <- fread(paste0(home_dir,"FILEPATH",decomp,"_",crosswalk_date,".csv"))


# Model mean ----------------------------------------------------------

# merge on location information for modelling and aggregation
dt <- merge(dt,locs[,.(location_id,region_name,region_id,super_region_name,super_region_id,level,path_to_top_parent,most_detailed)],by="location_id",all.x=T)

ggplot(data=dt,aes(x=lm,y=lsd,color=method))+geom_point()+ggtitle("Input Data LSD v LM")

ggplot(data=dt,aes(x=sample_size,y=lsd,color=super_region_name,shape=as.character(smaller_site_unit)))+
  geom_point()+
  ggtitle("Input Data LSD v sample size")+
  scale_x_log10()+
  scale_shape_manual(values=c("0"=19,"1"=1),breaks=c("0","1"),labels=c("representative","non-representative"))

# Note which are standard locations
dt[,std_loc:="Non-standard location"]
dt[location_id %in% std_locs$location_id, std_loc:="Standard location"]

#pull covariates
#average mean temperature over time
mean_temp <- get_covariate_estimates(covariate_id=71,decomp_step="iterative", gbd_round_id=6)
mean_temp <- mean_temp[,.(location_id,mean_value)]
mean_temp <- mean_temp[,mean(mean_value),by="location_id"]
setnames(mean_temp,"V1","cv_temp")

# merge covariate onto radon dt
dt<-merge(dt,mean_temp,by=c("location_id"),all.x=TRUE)

# inverse variance weight was not stable. Had to impute a lot for sample size weighting. Decided on inverse standard error weighting
dt[,weight:=1/lse]

if(decomp=="step2"){
  dt[,weight:=1]
}

# First model run only on standard locations. Fixed effect on temp and random effects on locations. Only standard locations should effect beta values
mod1 <- lmer(lm ~ cv_temp + (1 | super_region_id) + (1 | region_id) + (1 | location_id),
             data=dt, weights=weight, subset=std_loc=="Standard location")

# save version of this model
save(mod1,file=paste0(home_dir,"FILEPATH",decomp,"_",date,".Rdata"))

# plot SR effects to check fit
SR <- as.data.table(coefficients(mod1)$super_region_id)
SR[,super_region_id:=row.names(coefficients(mod1)$super_region_id) %>% as.numeric()]
SR <- merge(SR,locs[,.(super_region_id,super_region_name)],by="super_region_id")
setnames(SR,c("(Intercept)","cv_temp"),c("beta0","beta1"))

ggplot(data=dt,aes(x=cv_temp,y=lm,size=weight,color=region_name,shape=std_loc))+
  geom_abline(data=SR,aes(slope=beta1,intercept=beta0))+
  geom_point()+
  facet_wrap(~super_region_name)+
  scale_shape_manual(values=c("Non-standard location"=1,"Standard location"=19))+
  ggtitle("Super-Region effects")

# plot R effects to check fit
R <- as.data.table(coefficients(mod1)$region_id)
R[,region_id:=row.names(coefficients(mod1)$region_id) %>% as.numeric()]
R <- merge(R,locs[,.(region_id,region_name)],by="region_id")
setnames(R,c("(Intercept)","cv_temp"),c("beta0","beta1"))

ggplot(data=dt,aes(x=cv_temp,y=lm,size=weight,color=region_name,shape=std_loc))+
  geom_abline(data=R,aes(slope=beta1,intercept=beta0))+
  geom_point()+
  facet_wrap(~region_name)+
  scale_shape_manual(values=c("Non-standard location"=1,"Standard location"=19))+
  ggtitle("Region effects")

# Predict out first mod and calculate residual
dt[,pred1:=predict(mod1,newdata=dt,allow.new.levels=T)]
dt[,resid1:=lm-pred1]

# Second model run on residuals to add random effects for non-standard locations
mod2 <- lmer(resid1 ~ (1 | region_id) + (1 | location_id), data=dt, weights=weight) # model was very unstable when including super_region

# save version of this model
save(mod2,file=paste0(home_dir,"FILEPATH",decomp,"_",date,".Rdata"))

dt[,resid:=predict(mod2,newdata=dt)]
dt[,pred:=pred1+resid]

#Plots to check fit:

ggplot(data=dt, aes(x=lm,y=pred1,color=region_name,size=weight))+geom_point()+geom_abline(slope=1,intercept=0)+facet_wrap(~std_loc)+ggtitle("First model, standard locations only")
ggplot(data=dt, aes(x=lm,y=pred, color=region_name,size=weight))+geom_point()+geom_abline(slope=1,intercept=0)+facet_wrap(~std_loc)+ggtitle("Second model, adds random effects for non-standard locations")


# Generate draws of mean --------------------------------------------------

### prep prediction dataset with all locations
prep <- merge(locs[,.(location_id,location_name,region_id,region_name,super_region_id,super_region_name,path_to_top_parent,level,most_detailed)],mean_temp[,.(location_id,cv_temp)],by="location_id",all.x=TRUE)
prep[,pred1:=predict(mod1,newdata=prep,allow.new.levels=T)]
prep[,resid:=predict(mod2,newdata=prep,allow.new.levels=T)]
prep[,pred:=pred1+resid]

#function to generate draws from the model including estimating random effects for unknown locations/regions (inflating standard error)
gen_draws <- function(model,model_name,prep_dt){

  #pull out random effects
  ranef<-as.data.table(ranef(model,condVar=T))
  ranef[,grp:=as.numeric(levels(grp))[grp]]
  ranef$term <- NULL

  # add in random effects for those groups for which we did not have any estimates
  unknown_re <- data.table(mean_re=numeric(),sd_re=numeric())
  for(i in 1:length(ranef(model))){
    re<- names(ranef(model))[i]
    prep_dt <- merge(prep_dt,ranef[grpvar==re,c("grp","condval","condsd")],by.x=re,by.y="grp",all.x=T)
    setnames(prep_dt,c("condval","condsd"),c(paste0(re,"_re"),paste0(re,"_re_sd")))
    unknown_re <- rbind(unknown_re,data.table(mean_re = round(mean(ranef[grpvar==re,condval]),4),
                                              sd_re = round(sqrt(ranef[grpvar==re,mean(condsd^2)]+sd(ranef[grpvar==re,condval])^2),4)))
    prep_dt[is.na(get(paste0(re,"_re"))), paste0(re,c("_re","_re_sd")):=c(unknown_re[i,1],unknown_re[i,2])]
  }

  # coefficient matrix
  coeff <- as.data.table(as.list(fixef(model)))
  coefmat <-matrix(unlist(coeff),ncol=ncol(coeff),byrow=T,dimnames=list(c("coef"),names(coeff)))

  # covariance matrix
  vcovmat <- vcov(model)
  vcovmat <- matrix(vcovmat,ncol=ncol(coeff),byrow=T)

  lre_cols <- paste0("lre_",draw_nums)
  rre_cols <- paste0("rre_",draw_nums)
  srre_cols <- paste0("srre_",draw_nums)
  prep_dt[,(lre_cols):= transpose(lapply(1:nrow(prep_dt),function(x) rnorm(mean=prep_dt$location_id_re[x],sd=prep_dt$location_id_re_sd[x],n=1000)))]
  prep_dt[,(rre_cols):= transpose(lapply(1:nrow(prep_dt),function(x) rnorm(mean=prep_dt$region_id_re[x],sd=prep_dt$region_id_re_sd[x],n=1000)))]

  if("super_region_id" %in% names(ranef(model))){
    prep_dt[,(srre_cols):= transpose(lapply(1:nrow(prep_dt),function(x) rnorm(mean=prep_dt$super_region_id_re[x],sd=prep_dt$super_region_id_re_sd[x],n=1000)))]
  }

  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
  # transpose coefficient matrix
  betas <- t(betadraws)
  row.names(betas) <- names(coeff)
  # create draws based on fixed and random effects

  draw_cols <- paste0("draw_",model_name,"_", draw_nums_gbd)
  out <- copy(prep_dt)
  if("super_region_id" %in% names(ranef(model))){
    if(ncol(coeff)==1){
      invisible(
        out[, (draw_cols) := lapply(draw_nums, function(x) { betas["(Intercept)", x] + get(paste0("lre_",x)) + get(paste0("rre_",x)) + get(paste0("srre_",x)
        ) } )] )
    }else if(ncol(coeff)==2){
      invisible(
        out[, (draw_cols) := lapply(draw_nums, function(x) { betas["(Intercept)", x] +
            ( betas[names(coeff)[2], x] * get(names(coeff)[2]) ) +
            get(paste0("lre_",x)) + get(paste0("rre_",x)) + get(paste0("srre_",x)
            ) } )] )
    }else if(ncol(coeff)==3){
      invisible(
        out[, (draw_cols) := lapply(draw_nums, function(x) { betas["(Intercept)", x] +
            ( betas[names(coeff)[2], x] * get(names(coeff)[2]) ) +
            ( betas[names(coeff)[3], x] * get(names(coeff)[3]) ) +
            get(paste0("lre_",x)) + get(paste0("rre_",x)) + get(paste0("srre_",x)
            ) } )] )
    }
  }else{
    if(ncol(coeff)==1){
      invisible(
        out[, (draw_cols) := lapply(draw_nums, function(x) { betas["(Intercept)", x] + get(paste0("lre_",x)) + get(paste0("rre_",x)
        ) } )] )
    }else if(ncol(coeff)==2){
      invisible(
        out[, (draw_cols) := lapply(draw_nums, function(x) { betas["(Intercept)", x] +
            ( betas[names(coeff)[2], x] * get(names(coeff)[2]) ) +
            get(paste0("lre_",x)) + get(paste0("rre_",x)
            ) } )] )
    }else if(ncol(coeff)==3){
      invisible(
        out[, (draw_cols) := lapply(draw_nums, function(x) { betas["(Intercept)", x] +
            ( betas[names(coeff)[2], x] * get(names(coeff)[2]) ) +
            ( betas[names(coeff)[3], x] * get(names(coeff)[3]) ) +
            get(paste0("lre_",x)) + get(paste0("rre_",x)
            ) } )] )
    }
  }


  out <- out[, c("location_id","region_id","super_region_id","cv_temp",model_name,"pred","path_to_top_parent","level","most_detailed",draw_cols),with=F]

  return(out)

}

pred1 <- gen_draws(model=mod1,model_name="pred1",prep_dt=prep)
resid <- gen_draws(model=mod2,model_name="resid",prep_dt=prep)

draws_radon <- merge(pred1,resid,by=intersect(names(pred1),names(resid)))

# combine mod1 with smoothed residuals to get final predictions:

draw_cols <- paste0("draw_",draw_nums_gbd)
draws_radon[, (draw_cols) := lapply(draw_nums_gbd, function(x) {
  get(paste0("draw_pred1_",x))+get(paste0("draw_resid_",x))
})]

#save LM draws
write.csv(draws_radon,file=paste0(home_dir,"FILEPATH",decomp,"_",date,".csv"),row.names=F)

#drop pred and resid columns

draws_radon[,paste0("draw_pred1_",draw_nums_gbd):=NULL]
draws_radon[,paste0("draw_resid_",draw_nums_gbd):=NULL]

#calculate mean and CI
exp <- copy(draws_radon)
exp <- as.data.table(exp)
exp[, exp_lower := apply(.SD, 1, quantile, c(.025)), .SDcols=draw_cols]
exp[, exp_mean := apply(.SD, 1, mean), .SDcols=draw_cols]
exp[, exp_median := apply(.SD, 1, quantile, c(0.5)), .SDcols=draw_cols]
exp[, exp_upper := apply(.SD, 1, quantile, c(.975)), .SDcols=draw_cols]
exp[, conf_int := exp_upper-exp_lower]

ggplot(data=exp,aes(x=pred,y=exp_mean))+geom_point()+geom_abline()+ggtitle("Plot to check mean of draws against model prediction (LM)")

#save LM summaries
write.csv(exp[,.(location_id,exp_lower,exp_mean,exp_median,exp_upper,conf_int)],file=paste0(home_dir,"FILEPATH",decomp,"_",date,".csv"),row.names=F)

# Model standard deviation ------------------------------------------------

# model to estimate LSD
mod_sd <- lmer(lsd ~ lm + (1|region_id) + (1|location_id),     # model was very unstable when including super_region
             data=dt[method %in% c("AM & GM","AM & ASD","GM & GSD","Median & GSD","AM & SE") &
                       smaller_site_unit==0],weights=weight)

# save version of this model
save(mod_sd,file=paste0(home_dir,"FILEPATH",decomp,"_",date,".Rdata"))

#Plots to check fit
ggplot(data=dt[method %in% c("AM & GM","AM & ASD","GM & GSD","Median & GSD","AM & SE")],aes(x=lm,y=lsd,color=method,size=weight))+
  geom_abline(slope=fixef(mod_sd)[2],intercept=fixef(mod_sd)[1])+
  geom_point()+ggtitle("Model to predict LSD from LM")+
  facet_wrap(~smaller_site_unit)

dt[,pred_sd:=predict(mod_sd,newdata=dt,allow.new.levels=T)]

ggplot(data=dt, aes(x=lsd,y=pred_sd))+
  geom_point()+geom_abline(slope=1,intercept=0)+
  expand_limits(x=c(0,2),y=c(0,2))+
  ggtitle("predicted values versus data")


# Generate draws of lsd --------------------------------------------------

### prep prediction data
prep[,lm:=pred]
prep[,pred_sd:=predict(mod_sd,newdata=prep,allow.new.levels=T)]

pred_sd <- gen_draws(model=mod_sd,model_name="pred_sd",prep_dt=prep)
draws_radon <- merge(draws_radon,pred_sd,by=intersect(names(draws_radon),names(pred_sd)))

# save draws lsd
write.csv(draws_radon,file=paste0(home_dir,"FILEPATH",decomp,"_",date,".csv"),row.names=F)

#calculate mean and CI
sd <- copy(draws_radon)
sd <- as.data.table(sd)
draw_cols <- paste0("draw_pred_sd_", draw_nums_gbd)
sd[, sd_lower := apply(.SD, 1, quantile, c(.025)), .SDcols=draw_cols]
sd[, sd_mean := apply(.SD, 1, mean), .SDcols=draw_cols]
sd[, sd_median := apply(.SD, 1, quantile, c(0.5)), .SDcols=draw_cols]
sd[, sd_upper := apply(.SD, 1, quantile, c(.975)), .SDcols=draw_cols]
sd[, sd_conf_int := sd_upper-sd_lower]

ggplot(data=sd,aes(x=pred_sd,y=sd_median))+geom_point()+geom_abline()+ggtitle("Plot to check median of draws against model prediction (LSD)")
ggplot(data=sd,aes(x=pred_sd,y=sd_mean))+geom_point()+geom_abline()+ggtitle("Plot to check mean of draws against model prediction (LSD)")

# save summary lsd
write.csv(sd[,.(location_id,sd_lower,sd_mean,sd_median,sd_upper,sd_conf_int)],file=paste0(home_dir,"FILEPATH",decomp,"_",date,".csv"),row.names=F)


# Convert to AM and ASD ---------------------------------------------------

calc_am <- function(lm,lsd){exp(lm + 0.5*lsd^2)}
calc_asd <- function(lm,lsd){sqrt(exp(2*lm+lsd^2)*(exp(lsd^2)-1))}

draws_radon[,pred_am:=calc_am(lm=pred, lsd=pred_sd)]
draws_radon[,pred_asd:=calc_asd(lm=pred,lsd=pred_sd)]

draw_am_cols <- paste0("draw_am_",draw_nums_gbd)

draws_radon[, (draw_am_cols) := lapply(draw_nums_gbd, function(x) {
  calc_am(lm=get(paste0("draw_",x)),
          lsd=get(paste0("draw_pred_sd_",x)))
  })]

draw_asd_cols <- paste0("draw_asd_",draw_nums_gbd)

draws_radon[, (draw_asd_cols) := lapply(draw_nums_gbd, function(x) {
  calc_asd(lm=get(paste0("draw_",x)),
          lsd=get(paste0("draw_pred_sd_",x)))
})]

# save draws am & asd
write.csv(draws_radon[,c("location_id",paste0("draw_am_",draw_nums_gbd),paste0("draw_asd_",draw_nums_gbd)), with = F],file=paste0(home_dir,"FILEPATH",decomp,"_",date,".csv"),row.names=F)

#calculate mean and CI
exp <- copy(draws_radon)
exp <- as.data.table(exp)
exp[, am_lower := apply(.SD, 1, quantile, c(.025)), .SDcols=draw_am_cols]
exp[, am_mean := apply(.SD, 1, mean), .SDcols=draw_am_cols]
exp[, am_median := apply(.SD, 1, quantile, c(0.5)), .SDcols=draw_am_cols]
exp[, am_upper := apply(.SD, 1, quantile, c(.975)), .SDcols=draw_am_cols]
exp[, am_conf_int := am_upper-am_lower]

exp[, asd_lower := apply(.SD, 1, quantile, c(.025)), .SDcols=draw_asd_cols]
exp[, asd_mean := apply(.SD, 1, mean), .SDcols=draw_asd_cols]
exp[, asd_median := apply(.SD, 1, quantile, c(0.5)), .SDcols=draw_asd_cols]
exp[, asd_upper := apply(.SD, 1, quantile, c(.975)), .SDcols=draw_asd_cols]
exp[, asd_conf_int := asd_upper-asd_lower]

ggplot(data=exp,aes(x=pred_am,y=am_mean))+geom_point()+geom_abline(slope=1,intercept=0)+ggtitle("Plot to check mean of draws against model prediction (AM)")
ggplot(data=exp,aes(x=pred_am,y=am_median))+geom_point()+geom_abline(slope=1,intercept=0)+ggtitle("Plot to check median of draws against model prediction (AM)")
ggplot(data=exp,aes(x=pred_asd,y=asd_mean))+geom_point()+geom_abline(slope=1,intercept=0)+ggtitle("Plot to check mean of draws against model prediction (ASD)")
ggplot(data=exp,aes(x=pred_asd,y=asd_median))+geom_point()+geom_abline(slope=1,intercept=0)+ggtitle("Plot to check median of draws against model prediction (ASD)")

dt[,am:=calc_am(lm,lsd)]
dt[,asd:=calc_asd(lm,lsd)]

check <- merge(dt[,.(location_id,am,asd,weight,region_name)],exp[,.(location_id,am_median,asd_median)], by="location_id", all.x=T)

ggplot(check,aes(x=am,y=am_median,color=region_name))+geom_point()+geom_line(aes(y=am))+scale_x_log10()+scale_y_log10()+ggtitle("Plot to check median of draws against data (AM)")
ggplot(check,aes(x=asd,y=asd_median,color=region_name))+geom_point()+geom_line(aes(y=asd))+scale_x_log10()+scale_y_log10()+ggtitle("Plot to check median of draws against data (ASD)")

exp <- exp[,.(location_id,am_lower,am_mean,am_median,am_upper,am_conf_int,asd_lower,asd_mean,asd_median,asd_upper,asd_conf_int)]

# save summary am,asd
write.csv(exp,file=paste0(home_dir,"FILEPATH",decomp,"_",date,".csv"),row.names=F)

# Save and upload final outputs -------------------------------------------

save <- function(meas){
  
  dt <- draws_radon[,c("location_id",paste0("draw_",meas,"_",draw_nums_gbd)),with=F] %>% as.data.table
  
  setnames(dt,paste0("draw_",meas,"_",draw_nums_gbd),paste0("draw_",draw_nums_gbd))
  
  #model results saved only by location id. add columns for age, sex, measure. Duplicate rows for year id
  dt[,c("age_group_id","sex_id","measure_id"):=.(rep(0,nrow(dt)),rep(0,nrow(dt)),rep(19,nrow(dt)))]
  
  for(age in ages){
    dt_age <- copy(dt[age_group_id==0])
    dt_age[,age_group_id:=age]
    dt <- rbind(dt,dt_age)
  }
  
  dt <- dt[age_group_id %in% ages]
  
  for(sex in sexes){
    dt_sex <- copy(dt[sex_id==0])
    dt_sex[,sex_id:=sex]
    dt <- rbind(dt,dt_sex)
  }
  
  dt <- dt[sex_id %in% sexes]
  
  save_dir <- paste0(home_dir,"FILEPATH",meas,"_",decomp,"FILEPATH",date,"/")
  dir.create(save_dir)
  
  for(year in years){
    dt[,year_id:=year]
    write.csv(dt,paste0(save_dir,year,".csv"),row.names=F)
  }
  
}
  
if(save_exposure){
  save(meas="am")
}
if(save_sd){
  save(meas="asd")
}

# Save Results ------------------------------------------------------------

# Save exposure
if(save_exposure){
  save_results_epi(input_dir=file.path(home_dir,"upload"),
                 input_file_pattern=paste0("am_",decomp,"_all_draws_",date,"/{year_id}.csv"),
                 year_id=years,
                 modelable_entity_id=2535,
                 description=desc,
                 measure_id=19,
                 mark_best=TRUE,
                 decomp_step=decomp)
}

# Save sd
if(save_sd){
  save_results_epi(input_dir=file.path(home_dir,"upload"),
                 input_file_pattern=paste0("asd_",decomp,"_all_draws_",date,"/{year_id}.csv"),
                 year_id=years,
                 modelable_entity_id=18685,
                 description=desc,
                 measure_id=19,
                 mark_best=TRUE,
                 decomp_step=decomp)
  }