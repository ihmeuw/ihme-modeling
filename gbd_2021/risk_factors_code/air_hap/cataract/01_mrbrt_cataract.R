# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

project <- "-P PROJECT "
sge.output.dir <- " -o FILEPATH -e FILEPATH "
#sge.output.dir <- "" # toggle to run with no output files

# load packages, install if missing
lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","ggplot2","openxlsx","metafor","pbapply")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

'%ni%' <- Negate("%in%")

# load covs or model?
run_covs <- F
save_covs <- F
load_covs <- F

draws_req <- 1000

model.version <- VERSION

#------------------Directories and shared functions-----------------------------
home_dir <- "FILEPATH"

in_dir <- file.path("FILEPATH")

out_dir <- file.path(home_dir,"FILEPATH",model.version,"/")
dir.create(out_dir,recursive=T)

results_dir <- file.path(home_dir,"FILEPATH",model.version,"/")
dir.create(results_dir,recursive=T)


# load shared functions
source("FILEPATH/get_outputs.R")
source("FILEPATH/get_covariate_estimates.R")

# load current locations list
source("FILEPATH/get_location_metadata.R")
location_set_version <- 35
locations <- get_location_metadata(location_set_version, gbd_round_id=7)

# load MR-BRT functions
library(mrbrt001, lib.loc="FILEPATH")

# get summary from last year
gbd19 <- fread("FILEPATH/summary.csv")
gbd19 <- log(gbd19)


#------------------Load data & prep covs----------------------------

df <- openxlsx::read.xlsx(paste0(in_dir,"FILEPATH.xlsm"), sheet="extraction", startRow=3, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(df) <- openxlsx::read.xlsx(paste0(in_dir,"FILEPATH.xlsm"), sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names
df <- df[!is.na(ier_source) & is_outlier==0]
df <- df[ier_cause == "cataract"]

df[,log_effect_size := log(mean)]
df[,log_effect_size_se := (log(upper)-log(lower))/(qnorm(0.975)*2)]

df[,c("nid","study","sex","year_start","year_end","mean","lower","upper","cases","sample_size",
        "cv_outcome_unblinded","hap_exposed_def","hap_unexposed_def"),with=F]

# create indicator for if estimate includes males
df[sex %in% c("Both","Male"),cv_male:=1]
df[sex=="Female",cv_male:=0]

# code cv_confounding_uncontrolled as a binary indicator variable (only for level 2)
df[,cv_confounding_uncontrolled_2:=ifelse(cv_counfounding.uncontroled==2,1,0)]

# create a dummy cov for linear exposure
df[,linear_exp:=1]


print("Done with data prep:)")


# -------------------------  Run a plain log-linear model to get beta_sd-----------------------------------------

data <- MRData()
data$load_df(
  data = df,  
  col_obs = "log_effect_size", 
  col_obs_se = "log_effect_size_se",
  col_study_id = "nid" 
)

model <- MRBRT(
  data = data,
  cov_models = list(LinearCovModel("intercept",use_re=T)),
  inlier_pct = 1
)

model$fit_model(inner_print_level = 5L, inner_max_iter = 500L)

# make predictions
df_preds <- data.frame(
  cv_male = 0)

dat_preds <- MRData()
dat_preds$load_df(
  data = df_preds,
  col_covs = list("cv_male")
)

df_preds$preds <- model$predict(dat_preds)
df_preds <- as.data.table(df_preds)

# make draws
samples <- model$sample_soln(sample_size = as.integer(draws_req))

draws <- model$create_draws(
  data = dat_preds,
  beta_samples = samples[[1]],
  gamma_samples = samples[[2]],
  random_study = T
)

df_preds$preds_lo <- apply(draws, 1, function(x) quantile(x, 0.025))
df_preds$preds_hi <- apply(draws , 1, function(x) quantile(x, 0.975))

# compare against GBD 2019
gbd19
View(df_preds)

# save beta_sd
beta_sd <- sd(samples[[1]])
saveRDS(beta_sd,paste0(out_dir,"beta_sd.RDS"))

# -------------------------   Covariate selection using log-linear model ------------------------------------------

if (run_covs){
  # prep data using MR-BRT format
  data_cov <- MRData()
  data_cov$load_df(
    data=df, col_obs="log_effect_size", col_obs_se="log_effect_size_se",
    col_covs=list("cv_outcome_unblinded","cv_confounding_uncontrolled_2","cv_male"),
    col_study_id="nid"
  )
  
  
  candidate_covs <- list("cv_outcome_unblinded","cv_confounding_uncontrolled_2","cv_male")
  
  
  covfinder <- CovFinder(
    data = data_cov,
    covs = as.list(candidate_covs),
    pre_selected_covs = list("intercept"),
    num_samples = 1000L,
    power_range = list(-4,4),
    power_step_size = 0.05,
    laplace_threshold = 1e-5,
    inlier_pct = 1,
    normalized_covs = T,
    beta_gprior_std = beta_sd,
    bias_zero=T
  )
  
  covfinder$select_covs(verbose=T)
  
  covs <- covfinder$selected_covs
  
  covs
  
  # save covs for later
  if(save_covs){
    saveRDS(covs, paste0(out_dir,"covs_DATE.RDS"))
  }
}

if(load_covs){
  covs <- readRDS(paste0(out_dir,"covs_DATE.RDS"))
}

print("Done with covariate selection:) ")

# -------------------------  Get effect size across all cataract studies  ------------------------------------------

data <- MRData()
data$load_df(
  data = df,  
  col_obs = "log_effect_size", 
  col_obs_se = "log_effect_size_se",
  col_covs = list(),
  col_study_id = "nid" 
)

model <- MRBRT(
  data = data,
  cov_models = list(LinearCovModel("intercept",use_re=T)),
  inlier_pct = 1
)

model$fit_model(inner_print_level = 5L, inner_max_iter = 500L)

# save model
saveRDS(model,paste0(out_dir,"model.RDS"))
py_save_object(object = model, filename = paste0(out_dir, "model.pkl"), pickle = "dill")

# make predictions
df_preds <- data.frame(
  cv_male = 0)

dat_preds <- MRData()
dat_preds$load_df(
  data = df_preds,
  col_covs = list("cv_male")
)

df_preds$preds <- model$predict(dat_preds)
df_preds <- as.data.table(df_preds)

# make draws
samples <- model$sample_soln(sample_size = as.integer(draws_req))

draws <- model$create_draws(
  data = dat_preds,
  beta_samples = samples[[1]],
  gamma_samples = samples[[2]],
  random_study = T
)

df_preds$preds_lo <- apply(draws, 1, function(x) quantile(x, 0.025))
df_preds$preds_hi <- apply(draws , 1, function(x) quantile(x, 0.975))

# compare against GBD 2019
gbd19
View(df_preds)

# save results
out <- as.data.table(draws)
setnames(out, paste0("draw_",0:999))

write.csv(out,
          file.path(results_dir,"draws.csv"),
          row.names=F)

summary <- data.table(mean=mean(as.matrix(out)),
                      lower=quantile(out,0.025) %>% as.numeric(),
                      upper=quantile(out,0.975) %>% as.numeric(),
                      median=quantile(out,0.5) %>% as.numeric())

write.csv(summary,
          file.path(results_dir,"summary.csv"),
          row.names=F)

# -------------------------  Get evidence score  ------------------------------------------
betas <- samples[[1]]
inclusion <- quantile(betas[,1],0.05)


#
# 06_publication_bias.R
#
#
library(mrbrt001, lib.loc = "FILEPATH")


### Running settings
ro_pair <- c("air_hap_cataract")
work_dir <- "FILEPATH"

setwd(work_dir)
source("FILEPATH/dichotomous_functions.R")

model_path <- paste0(out_dir,"model.pkl")


### Load model objects
model <- py_load_object(filename = model_path, pickle = "dill")


### Extract data
df <- extract_data_info(model)


### Detect publication bias
egger_model_all <- egger_regression(df$residual, df$residual_se)
egger_model <- egger_regression(df[!df$outlier,]$residual, df[!df$outlier,]$residual_se)
has_pub_bias <- egger_model$pval < 0.05


### Adjust for publication bias
if (has_pub_bias) {
  df_fill <- get_df_fill(df)
  num_fill <- nrow(df_fill)
} else {
  num_fill <- 0
}

# fill the data if needed and refit the model
if (num_fill > 0) {
  df <- rbind(df, df_fill)
  
  # refit the model
  data = MRData()
  data$load_df(
    data=df[!df$outlier,],
    col_obs='obs',
    col_obs_se='obs_se',
    col_covs=as.list(model$cov_names),
    col_study_id='study_id'
  )
  model_fill <- MRBRT(data, cov_models=model$cov_models)
  model_fill$fit_model()
} else {
  model_fill <- NULL
}


### Extract scores
uncertainty_info <- get_uncertainty_info(model)
if (is.null(model_fill)) {
  uncertainty_info_fill <- NULL
} else {
  uncertainty_info_fill <- get_uncertainty_info(model_fill)
}


### Output diagnostics
pdf(paste0("FILEPATH",model.version,"/",ro_pair,".pdf"),width = 11,height = 7)
title <- paste0(ro_pair, ": egger_mean=", round(egger_model$mean, 3),
                ", egger_sd=", round(egger_model$sd,3), ", egger_pval=", 
                round(egger_model$pval, 3))
plot_model(df, uncertainty_info, model, uncertainty_info_fill, model_fill, ro_pair)
dev.off()

plot_model_appendix(df, uncertainty_info, model, uncertainty_info_fill, model_fill, ro_pair)

summary <- summarize_model(ro_pair, model, model_fill, egger_model, egger_model_all, uncertainty_info)

draws <- get_draws(model)
draws <- t(draws) %>% as.data.table

setnames(draws, paste0("draw_",0:999))

# save draws
write.csv(draws,
          file.path(results_dir,"fisher_draws.csv"),
          row.names=F)

summary <- data.table(mean=mean(as.matrix(draws)),
                      lower=quantile(draws,0.025) %>% as.numeric(),
                      upper=quantile(draws,0.975) %>% as.numeric(),
                      median=quantile(draws,0.5) %>% as.numeric())

write.csv(summary,
          file.path(results_dir,"fisher_summary.csv"),
          row.names=F)
