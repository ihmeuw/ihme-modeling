
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 02/15/2019
# Purpose: Prep ozone RR data for MR-BRT 
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

packages <- c("data.table","magrittr")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

# load the functions
repo_dir <- "FILEPATH"
source(file.path(repo_dir,"FILEPATH.R"))

version <- 1

#------------------DIRECTORIES--------------------------------------------------
home_dir <- file.path("FILEPATH")
out_dir <- file.path(home_dir,"model",version)
dir.create(out_dir,showWarnings = F)

results_dir <- file.path(home_dir,"results",version)
dir.create(results_dir,showWarnings = F)

data <- read.xlsx(file.path(home_dir,"FILEPATH.xlsx")) %>% as.data.table
data <- data[exclude==0]

# convert all to ppb
# using US EPA conversion factor of 0.51 * micrograms/m^3 = ppb
data[unit=="microg/m^3",number:=number*.51]

#converting every RR to per 10 ppb
data[,rr_shift:=effect_size^(10/number)]
data[,rr_lower_shift:=lower^(10/number)]
data[,rr_upper_shift:=upper^(10/number)]

# if we have the upper and lower, we can just log those and convert to log SE
data[, log_effect_size_se := (log(rr_upper_shift)-log(rr_lower_shift))/(qnorm(0.975)*2)]

#take the log of the risk
data[,log_effect_size:=log(rr_shift)]

data[,.(cv_exposure_study,cv_confounding_uncontrolled,log_effect_size,log_effect_size_se)]

#fit 1, no priors, and no Z covariates #USED FOR GBD2019
fit1 <- run_mr_brt(
  output_dir = out_dir,
  model_label = "no_cov",
  data = data,
  mean_var = "log_effect_size",
  se_var = "log_effect_size_se",
  covs = list(),
  study_id = "nid",
  overwrite_previous = T)

fit1$model_coefs
exp(fit1$model_coefs$beta_soln)
plot_mr_brt(fit1)

# pred1 <- predict_mr_brt(fit1,newdata=data.table(X_intercept=1,Z_intercept=1))
# pred1$model_summaries$Y_mean %>% exp()
# pred1$model_summaries$Y_mean_lo %>% exp()
# pred1$model_summaries$Y_mean_hi %>% exp()
# pred1$model_summaries$Y_negp
# 
# pred1$model_summaries$Y_mean_fe %>% exp()
# pred1$model_summaries$Y_mean_lo_fe %>% exp()
# pred1$model_summaries$Y_mean_hi_fe %>% exp()
# pred1$model_summaries$Y_negp_fe

mean <- fit1$model_coefs$beta_soln[1]
var <- fit1$model_coefs$beta_var[1]
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

test <- get_draws("rei_id", 86, location_id=54, year_id=1990, source="rr", gbd_round_id=5, status="best")