#-------------------Header------------------------------------------------
# Author: NAME
# Date: 02/22/2019
# Purpose: Prep radon extractions for MR-BRT
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
  } else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  }

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","openxlsx","ggplot2")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

# load the functions
repo_dir <- "FILEPATH"
source(file.path(repo_dir,"FILEPATH.R"))

version <- "linear" # new extractions--assuming linear

in_dir <- "FILEPATH"
out_dir <- file.path("FILEPATH",version)
results_dir <- file.path("FILEPATH",version)

dir.create(out_dir,recursive=T)
dir.create(results_dir,recursive=T)

#------------------PREP DATA------------------------------------------------------

dt <- read.xlsx(file.path(in_dir,"FILEPATH.xlsm"),
                sheet="extraction", startRow=4, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(dt) <- read.xlsx(file.path(in_dir,"FILEPATH.xlsm"),
                       sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names

dt <- dt[is.na(exclude)]

if(version==1){
  # isolate to continouos studies 
  dt <- dt[custom_select_rows=="continuous"]
}else if(version==2){
  dt <- dt[custom_select_rows %in% c("continuous","categorical_only")]
}else if(version=="linear"){
  dt <- dt[custom_select_rows %in% c("continuous","continuous_only","spline","categorical_only")]
}else if(version=="spline"){
  dt <- dt[custom_select_rows %in% c("categorical","continuous_only","spline","categorical_only")]
}

# calculate scaling factor for different units
dt[,exp_unit:=cohort_exp_unit_rr]
dt[is.na(exp_unit),exp_unit:=cc_exp_unit_rr]

dt[,exp_scale_factor:=1]
dt[exp_unit=="hBq/m^3", exp_scale_factor:=100]
dt[exp_unit=="pCi/L", exp_scale_factor:= 37] # http://www.icrpaedia.org/index.php/Radon:_Units_of_Measure

# For continouous effect size, determine the exposure level/range of the effect
dt[,exp_level_dr := cohort_exp_level_dr]
dt[is.na(exp_level_dr), exp_level_dr := cc_exp_level_dr]

# For categorical determine the exposure level/range of the effect

# first choice is to use the study given mean/median of the group
# otherwise we will use the midpoint. When there is no lower value given, replace with 0. When there is no upper value given, replace with 2 times the minimum of the upper category.
dt[custom_select_rows %in% c("categorical_only","spline","categorical") & is.na(exp_level_dr) & is.na(custom_unexp_level_lower), custom_unexp_level_lower:=0]
dt[custom_select_rows %in% c("categorical_only","spline","categorical") & is.na(exp_level_dr) & is.na(custom_exp_level_upper), custom_exp_level_upper:=2*custom_exp_level_lower]

dt[custom_select_rows %in% c("categorical_only","spline","categorical") & is.na(cc_unexp_level_rr),cc_unexp_level_rr:=(custom_unexp_level_lower+custom_unexp_level_upper)/2]
dt[custom_select_rows %in% c("categorical_only","spline","categorical") & is.na(cc_exp_level_rr),cc_exp_level_rr:=(custom_exp_level_lower+custom_exp_level_upper)/2]

dt[custom_select_rows %in% c("categorical_only","spline","categorical") & is.na(exp_level_dr), exp_level_dr:=cc_exp_level_rr-cc_unexp_level_rr] 

# Check
dt[,.(custom_select_rows,cc_exposed_def,cc_exp_level_rr,cc_unexposed_def,cc_unexp_level_rr,exp_level_dr,exp_unit,exp_scale_factor)]

# Scale effect size to the given range
dt[,scaled_effect_mean:=effect_size^(100/(exp_level_dr*exp_scale_factor))]
dt[,scaled_effect_lower:= lower ^ (100/(exp_level_dr*exp_scale_factor))]
dt[,scaled_effect_upper:= upper ^ (100/(exp_level_dr*exp_scale_factor))]

dt[,sample_size := cohort_sample_size_total]
dt[is.na(sample_size), sample_size := cc_cases+cc_control]

# calculate exposure value for each row for spline
dt[,exposure:=(cc_exp_level_rr+cc_unexp_level_rr)/2*exp_scale_factor]
# for continuous studies plot at median
dt[is.na(exposure),exposure:=custom_exp_50th*exp_scale_factor]

# Check
dt[,.(custom_select_rows,cc_exposed_def,cc_exp_level_rr,cc_unexposed_def,cc_unexp_level_rr,exposure,exp_level_dr,exp_unit,exp_scale_factor)]

# isolate to needed columns
dt <- dt[,c("nid","location_name","design","outcome_type","effect_size","lower","upper","CI_uncertainty_type_value","exp_unit",
            "exp_level_dr","exp_scale_factor","scaled_effect_mean","scaled_effect_lower","scaled_effect_upper","sample_size",
            "custom_select_rows",grep("cv_",names(dt),value = T),"exposure"),
         with=F]

# replace cv_selection_bias with 2 when missing
dt[is.na(cv_selection_bias),cv_selection_bias:=2]

# calculate log effect size and log se for MR-BRT
dt[,log_effect_size:=log(scaled_effect_mean)]
dt[,log_effect_size_se:= (log(scaled_effect_upper)-log(scaled_effect_lower))/
     (qnorm(0.5+(CI_uncertainty_type_value/200))*2)]

# predict unknown SE based on sample size
dt[,scaled_n:=1/sqrt(sample_size)]

SE_mod <- lm(data=dt,
             log_effect_size_se ~ 0 + scaled_n)
dt[,log_effect_size_se_predicted:=predict.lm(SE_mod,newdata = dt)]
dt

pred_dt <- data.table(sample_size = seq(50,1000000,10))
pred_dt[,scaled_n := 1/sqrt(sample_size)]
pred_dt[, log_effect_size_se:= predict.lm(SE_mod,
                                          newdata=pred_dt)]

pdf(file.path(out_dir,"FILEPATH.pdf"))

# plot to check fit for imputing SE
ggplot(data=dt,aes(x=sample_size,y=log_effect_size_se))+
  geom_point(aes(color=custom_select_rows))+
  geom_line(data=pred_dt,
            aes(x=sample_size,y=log_effect_size_se))+
  geom_vline(xintercept=dt[is.na(log_effect_size_se),sample_size],color="grey")+
  scale_x_log10()+
  scale_y_continuous(limits=c(0,2.5))+
  scale_color_manual(values=c("categorical_only"= "#af8dc3","categorical"= "#af8dc3","continuous"="#7fbf7b","continuous_only"="#7fbf7b","spline"="#f48b8b"))+
  theme_bw()

dev.off()

# replace missing SE with predicted
dt[is.na(log_effect_size_se),log_effect_size_se:=log_effect_size_se_predicted]

write.csv(dt,file.path(out_dir,"FILEPATH.csv"),row.names = F)

#------------------FIT 1----------------------------------------------------------

# fit 1, no priors, and no Z covariates
fit1 <- run_mr_brt(
  output_dir = out_dir,
  model_label = "no_cov",
  data = dt,
  mean_var = "log_effect_size",
  se_var = "log_effect_size_se",
  covs = list(),
  study_id = "nid",
  overwrite_previous = T)

fit1$model_coefs
exp(fit1$model_coefs$beta_soln)
plot_mr_brt(fit1)

pred1 <- predict_mr_brt(fit1,newdata=data.table(X_intercept=1,Z_intercept=1))
pred1$model_summaries$Y_mean %>% exp()
pred1$model_summaries$Y_mean_lo %>% exp()
pred1$model_summaries$Y_mean_hi %>% exp()
pred1$model_summaries$Y_negp

pred1$model_summaries$Y_mean_fe %>% exp()
pred1$model_summaries$Y_mean_lo_fe %>% exp()
pred1$model_summaries$Y_mean_hi_fe %>% exp()
pred1$model_summaries$Y_negp_fe

#------------------FIT 2----------------------------------------------------------

# Adding bias covariates & trim
cov_list <- list()
i <- 0
for(cv in c("cv_exposure_study","cv_selection_bias")){
  if(any(is.na(dt[,get(cv)]))){
    warning(paste("missingness in",cv))
  }else if(length(dt[,unique(get(cv))])>1){
    i <- i+1
    cov_list[[i]] <- cov_info(cv,"X")
    i <- i+1
    cov_list[[i]] <- cov_info(cv,"Z")
  }
}

fit2 <- run_mr_brt(
  output_dir = out_dir,
  model_label = "bias_cov",
  data = dt,
  mean_var = "log_effect_size",
  se_var = "log_effect_size_se",
  covs = cov_list,
  study_id = "nid",
  overwrite_previous = T,
  method = "trim_maxL",
  trim_pct = 0.1)

fit2$model_coefs
exp(fit2$model_coefs$beta_soln)
plot_mr_brt(fit2)

pred2 <- predict_mr_brt(fit2,newdata=expand.grid(cv_exposure_study=0,cv_selection_bias=0))
pred2$model_summaries$Y_mean %>% exp()
pred2$model_summaries$Y_mean_lo %>% exp()
pred2$model_summaries$Y_mean_hi %>% exp()
pred2$model_summaries$Y_negp

pred2$model_summaries$Y_mean_fe %>% exp()
pred2$model_summaries$Y_mean_lo_fe %>% exp()
pred2$model_summaries$Y_mean_hi_fe %>% exp()
pred2$model_summaries$Y_negp_fe

#------------------FIT 3----------------------------------------------------------

# fit 3: same as fit 2, adding priors on Beta, y0, and Z covariates #NOTE: THIS FIT WAS USED FOR GBD2019
cov_list <- list()
i <- 1
cov_list[[i]] <- cov_info("intercept","Z",gprior_mean=0.2,gprior_var=10e-2)
for(cv in c("cv_exposure_study","cv_selection_bias")){
  if(any(is.na(dt[,get(cv)]))){
    warning(paste("missingness in",cv))
  }else if(length(dt[,unique(get(cv))])>1){
    i <- i+1
    cov_list[[i]] <- cov_info(cv,"X",gprior_mean=0,gprior_var=10e-2)
    i <- i+1
    cov_list[[i]] <- cov_info(cv,"Z",gprior_mean=0,gprior_var=10e-2)
  }
}

fit3 <- run_mr_brt(
  output_dir = out_dir,
  model_label = "priors",
  data = dt,
  mean_var = "log_effect_size",
  se_var = "log_effect_size_se",
  covs = cov_list,
  study_id = "nid",
  overwrite_previous = T,
  method = "trim_maxL",
  trim_pct = 0.1,
  remove_z_intercept = T)

fit3$model_coefs
exp(fit3$model_coefs$beta_soln)
plot_mr_brt(fit3)

# pred3 <- predict_mr_brt(fit3,newdata=expand.grid(cv_exposure_study=0,cv_selection_bias=0),n_samples=1000,write_draws = T)
# pred3$model_summaries$Y_mean %>% exp()
# pred3$model_summaries$Y_mean_lo %>% exp()
# pred3$model_summaries$Y_mean_hi %>% exp()
# pred3$model_summaries$Y_negp
# 
# pred3$model_summaries$Y_mean_fe %>% exp()
# pred3$model_summaries$Y_mean_lo_fe %>% exp()
# pred3$model_summaries$Y_mean_hi_fe %>% exp()
# pred3$model_summaries$Y_negp_fe

mean <- fit3$model_coefs$beta_soln[1]
var <- fit3$model_coefs$beta_var[1]
se <- sqrt(var)

draws <- as.data.table(t(rnorm(1000,mean,se))) %>% exp
setnames(draws,paste0("V",1:1000),paste0("draw_",0:999))

write.csv(draws,
          file.path(results_dir,"FILEPATH.csv"),
          row.names=F)

summary <- data.table(mean=mean(as.matrix(draws)),
                        lower=quantile(draws,0.025) %>% as.numeric(),
                        upper=quantile(draws,0.975) %>% as.numeric(),
                        median=quantile(draws,0.5) %>% as.numeric())

write.csv(summary,
          file.path(results_dir,"FILEPATH.csv"),
          row.names=F)