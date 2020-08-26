
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 8/15/2019
# Purpose: Run MR-Brt Model for HAP Cataracts
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

packages <- c("data.table","magrittr","ggplot2")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

version <- 1
version <- 2

# Directories -------------------------------------------------------------
home_dir <- "FILEPATH"

in_dir <- file.path(home_dir,"FILEPATH")
out_dir <- file.path(home_dir,"FILEPATH",version)
dir.create(out_dir,recursive=T)

results_dir <- file.path(home_dir,"FILEPATH",version)
dir.create(results_dir,recursive=T)

# load the functions
repo_dir <- "FILEPATH"
source(file.path(repo_dir,"FILEPATH.R"))

# Format data -------------------------------------------------------------
data <- openxlsx::read.xlsx(paste0(in_dir,"FILEPATH.xlsm"), sheet="extraction", startRow=3, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(data) <- openxlsx::read.xlsx(paste0(in_dir,"FILEPATH.xlsm"), sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names
data <- data[!is.na(ier_source) & is_outlier==0]
data <- data[ier_cause == "cataract"]

data[,log_effect_size := log(mean)]
data[,log_effect_size_se := (log(upper)-log(lower))/(qnorm(0.975)*2)]

data[,c("nid","study","sex","year_start","year_end","mean","lower","upper","cases","sample_size",
        "cv_outcome_unblinded","hap_exposed_def","hap_unexposed_def"),with=F]

# create indicator for if estimate includes males
data[sex %in% c("Both","Male"),male:=1]
data[sex=="Female",male:=0]

# Run MR-BRT --------------------------------------------------------------
# Fit 1: check for significant effect on sex
fit1 <- run_mr_brt(
  output_dir = out_dir,
  model_label = "sex_effect",
  data = data,
  mean_var = "log_effect_size",
  se_var = "log_effect_size_se",
  covs = list(cov_info("male","X"),cov_info("male","Z")),
  study_id = "nid",
  overwrite_previous = T)

fit1$model_coefs #studies that include male have a slightly higher RR (non-significant)
exp(fit1$model_coefs$beta_soln)
plot_mr_brt(fit1)

pred1 <- predict_mr_brt(fit1,newdata=data.table(X_intercept=1,Z_intercept=1,male=c(0,1)))
pred1$model_summaries$Y_mean %>% exp()
pred1$model_summaries$Y_mean_lo %>% exp()
pred1$model_summaries$Y_mean_hi %>% exp()
pred1$model_summaries$Y_negp

pred1$model_summaries$Y_mean_fe %>% exp()
pred1$model_summaries$Y_mean_lo_fe %>% exp()
pred1$model_summaries$Y_mean_hi_fe %>% exp()
pred1$model_summaries$Y_negp_fe


# Fit 2: add bias covariates & trim
cov_list <- list()
i <- 0
for(cv in c("cv_outcome_unblinded")){
  if(any(is.na(data[,get(cv)]))){
    warning(paste("missingness in",cv))
  }else if(length(data[,unique(get(cv))])>1){
    i <- i+1
    cov_list[[i]] <- cov_info(cv,"X")
    i <- i+1
    cov_list[[i]] <- cov_info(cv,"Z")
  }
}

fit2 <- run_mr_brt(
  output_dir = out_dir,
  model_label = "bias_cov",
  data = data,
  mean_var = "log_effect_size",
  se_var = "log_effect_size_se",
  covs = cov_list,
  study_id = "nid",
  overwrite_previous = T)

fit2$model_coefs
exp(fit2$model_coefs$beta_soln)
plot_mr_brt(fit2)

pred2 <- predict_mr_brt(fit2,newdata=expand.grid(cv_outcome_unblinded=0))
pred2$model_summaries$Y_mean %>% exp()
pred2$model_summaries$Y_mean_lo %>% exp()
pred2$model_summaries$Y_mean_hi %>% exp()
pred2$model_summaries$Y_negp

pred2$model_summaries$Y_mean_fe %>% exp()
pred2$model_summaries$Y_mean_lo_fe %>% exp()
pred2$model_summaries$Y_mean_hi_fe %>% exp()
pred2$model_summaries$Y_negp_fe


# Fit 3: same as fit 2, add priors on Beta, y0, and Z covariates  
cov_list <- list()
i <- 1
cov_list[[i]] <- cov_info("intercept","Z",gprior_mean=0.2,gprior_var=10e-2)
for(cv in c("cv_outcome_unblinded")){
  if(any(is.na(data[,get(cv)]))){
    warning(paste("missingness in",cv))
  }else if(length(data[,unique(get(cv))])>1){
    i <- i+1
    cov_list[[i]] <- cov_info(cv,"X",gprior_mean=0,gprior_var=10e-2)
    i <- i+1
    cov_list[[i]] <- cov_info(cv,"Z",gprior_mean=0,gprior_var=10e-2)
  }
}

fit3 <- run_mr_brt(
  output_dir = out_dir,
  model_label = "priors",
  data = data,
  mean_var = "log_effect_size",
  se_var = "log_effect_size_se",
  covs = cov_list,
  study_id = "nid",
  overwrite_previous = T,
  remove_z_intercept = T)

fit3$model_coefs
exp(fit3$model_coefs$beta_soln)
plot_mr_brt(fit3)

pred3 <- predict_mr_brt(fit3,newdata=expand.grid(cv_outcome_unblinded=0))
pred3$model_summaries$Y_mean %>% exp()
pred3$model_summaries$Y_mean_lo %>% exp()
pred3$model_summaries$Y_mean_hi %>% exp()
pred3$model_summaries$Y_negp

pred3$model_summaries$Y_mean_fe %>% exp()
pred3$model_summaries$Y_mean_lo_fe %>% exp()
pred3$model_summaries$Y_mean_hi_fe %>% exp()
pred3$model_summaries$Y_negp_fe

mean <- fit3$model_coefs$beta_soln[1]
var <- fit3$model_coefs$beta_var[1]
se <- sqrt(var)

draws <- as.data.table(t(rnorm(1000,mean,se))) %>% exp
setnames(draws,paste0("V",1:1000),paste0("draw_",0:999))

# Save results and summarize-----------------------------------------------

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