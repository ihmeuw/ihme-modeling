#-------------------Header------------------------------------------------
# Purpose: Fit MR-BRT models to ozone RR data
#          
#------------------Set-up--------------------------------------------------

# clear memory
rm(list=ls())


# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/"
  h_root <- "/homes/USERNAME/"
} else {
  j_root <- "J:"
  h_root <- "H:"
}

project <- "-P proj_erf "
sge.output.dir <- " -o /ADDRESS -e ADDRESS "
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
load_covs <- T
no_covs <- F

draws_req <- 1000

version <- 5 

#------------------Directories and shared functions-----------------------------
# create out_dir and directories for each step
out_dir <- paste0("FILEPATH/",version)
dir.create(out_dir,recursive = T)

step1_dir <- paste0(out_dir,"/01_loglinear/")
dir.create(step1_dir,recursive = T)
beta_sd_dir <- paste0(step1_dir,"beta_sd/")
dir.create(beta_sd_dir,recursive = T)

step2_dir <- paste0(out_dir,"/02_covariates/")
dir.create(step2_dir,recursive = T)

step3_dir <- paste0(out_dir,"/03_loglinear_covs/")
dir.create(step3_dir,recursive = T)
slope_dir <- paste0(step3_dir,"slope/")
dir.create(slope_dir,recursive = T)

draws_dir <- paste0("FILEPATH",version,"/")
dir.create(draws_dir,recursive = T)

step5_dir <- paste0(out_dir,"/05_evidence_score/")
dir.create(step5_dir,recursive = T)

fisher_draws_dir <- paste0("FILEPATH/",version,"/")
dir.create(fisher_draws_dir,recursive = T)

# load shared functions
source("FILEPATH/get_outputs.R")
source("FILEPATH/get_covariate_estimates.R")

# load current locations list
source("FILEPATH/get_location_metadata.R")
location_set_version <- 35
locations <- get_location_metadata(location_set_version, gbd_round_id=7)

# load MR-BRT functions
library(mrbrt001, lib.loc="FILEPATH")

# make a dummy table of final GBD2019 results to use for plotting
mrbrt <- fread(paste0("/FILEPATH/summary.csv"))

mrbrt <- log(mrbrt)

gbd19 <- data.table(linear_exp = seq(0,40,length.out = 1000))
gbd19[,log_rr_mean:= mrbrt$mean*(linear_exp/10)]
gbd19[,log_rr_lo := mrbrt$lower*(linear_exp/10)]
gbd19[,log_rr_hi := mrbrt$upper*(linear_exp/10)]


# make a simple plotting function

# simple plotting function
make_plot <- function(dfpred, altvar, predmean, predlo, predhi){
  p <- ggplot() +
    
    # plot the datapoints
    geom_point(data=df, aes(x=10, y=log_effect_size, alpha = 1/log_effect_size_se), color = "black") +
    
    # plot gbd19 fit
    geom_ribbon(data = gbd19, aes(x = linear_exp, ymin = log_rr_lo, ymax = log_rr_hi, linetype = "GBD 2019"), fill = "black", alpha = 0.2) +
    geom_line(data = gbd19, aes(x = linear_exp, y = log_rr_mean, linetype = "GBD 2019")) +
    
    # plot the predicted fit
    geom_ribbon(data = dfpred, aes(x = altvar, ymin = predlo, ymax = predhi, linetype = "GBD 2020"), fill = "green", alpha = 1/5) +
    geom_line(data = dfpred, aes(x = altvar, y = predmean, linetype = "GBD 2020")) +
    
        # add lines to mark the 10th and 90th percentiles of input data exposure
    geom_vline(aes(xintercept=lower_domain),linetype="dotted",color="black") +
    geom_vline(aes(xintercept=upper_domain),linetype="dotted",color="black") +
    
    # add a line to mark log(0) - baseline
    geom_abline(slope = 0, intercept = 0, color = "blue") +
    
    # theme/formatting
    xlab("Ozone exposure (ppb)") +
    ylab("Log(RR)") +
    labs(linetype = "Fit") +
    # scale_fill_manual(values = c("GBD 2020" = "green", "GBD 2019" = "black")) +
    guides(alpha = FALSE) +
    theme_classic()
  
  print(p)
}


#------------------Load data & prep covs----------------------------

# load input data
df <- read.csv(file.path("FILEPATH/ozone_input_prepped.csv")) %>% as.data.table

# all of these RRs were scaled to 10 ppb
df[,a_0:=0]
df[,a_1:=0]
df[,b_0:=10]
df[,b_1:=10]


lower_domain = 0.7803
upper_domain = 10

print("Done with data prep:)")


# -------------------------  Run Log-linear model with exposure to get posterior standard deviation for beta ------------------------------------------

# load the data frame
data <-  MRData()

data$load_df(
  df,
  col_obs = "log_effect_size",
  col_obs_se = "log_se_weighted",
  col_covs = list("a_0","a_1","b_0","b_1"),
  col_study_id = "nid")


log_cov_model = LogCovModel(
  alt_cov=c("b_0","b_1"),
  ref_cov=c("a_0","a_1"),
  use_re=T,
  use_spline=F,
  use_re_mid_point=T
)

model <- MRBRT(
  data = data,
  cov_models = list(log_cov_model),
  inlier_pct = 0.9)

model$fit_model(inner_print_level = 5L, inner_max_iter = 500L)

# make predictions
df_preds <- data.frame(
  b_0 = seq(0, 40, length.out = 1000),
  b_1 = seq(0, 40, length.out = 1000),
  a_0 = rep(0,times=1000),
  a_1= rep(0,times=1000))

dat_preds <- MRData()
dat_preds$load_df(
  data = df_preds,
  col_covs = list("a_0","a_1","b_0","b_1")
)

df_preds$preds <- model$predict(dat_preds)
df_preds <- as.data.table(df_preds)

# make draws
samples <- model$sample_soln(sample_size = as.integer(draws_req))

draws <- model$create_draws(
  data = dat_preds,
  beta_samples = samples[[1]],
  gamma_samples = samples[[2]],
  random_study = F
)

df_preds$preds_lo <- apply(draws, 1, function(x) quantile(x, 0.025))
df_preds$preds_hi <- apply(draws , 1, function(x) quantile(x, 0.975))

# make simple plot
pdf(paste0(step1_dir,"ozone_loglinear.pdf"), width = 11, height = 8)
make_plot(df_preds, df_preds$b_0, df_preds$preds, df_preds$preds_lo, df_preds$preds_hi)
dev.off()

make_plot(df_preds, df_preds$b_0, df_preds$preds, df_preds$preds_lo, df_preds$preds_hi)

# get SD for betas
beta_sd <- sd(samples[[1]])
beta_sd
saveRDS(beta_sd, paste0(beta_sd_dir,"betasd.RDS"))
print("Done with step one:) ")

beta_sd <- readRDS(paste0(beta_sd_dir,"betasd.RDS"))

# -------------------------   Covariate selection using log-linear model ------------------------------------------

if (run_covs){
  # prep data using MR-BRT format
  data_cov <- MRData()
  data_cov$load_df(
    data=df, col_obs="log_effect_size", col_obs_se="log_se_weighted",
    col_covs=list("b_0",
                  "cv_confounding_uncontrolled_1","cv_confounding_uncontrolled_2","cv_exposure_study"),
    col_study_id="nid"
  )

  candidate_covs <- list("cv_confounding_uncontrolled_1","cv_confounding_uncontrolled_2","cv_exposure_study")

  covfinder <- CovFinder(
    data = data_cov,
    covs = as.list(candidate_covs),
    pre_selected_covs = list("b_0"),
    num_samples = 1000L,
    power_range = list(-4,4),
    power_step_size = 0.05,
    laplace_threshold = 1e-5,
    inlier_pct = 0.9,
    normalized_covs = T,
    beta_gprior_std = 0.1*beta_sd
  )

  covfinder$select_covs(verbose=T)

  covs <- covfinder$selected_covs

  covs <- c(covs,"a_0","a_1","b_1")
  covs

  # save covs for later
  if(save_covs){
    saveRDS(covs, paste0(step2_dir, "covariates.RDS"))
  }
}

if(load_covs){
  covs <- readRDS(paste0(step2_dir,"covariates.RDS"))
}

if(no_covs){
  covs <- c("a_0","a_1","b_0","b_1")
}

print("Done with step two:) ")

covs <- readRDS(paste0(step2_dir,"covariates.RDS"))

# -------------------------  Run Log-linear model to get slope prior for next stage  ------------------------------------------

# load the data frame
data <-  MRData()

data$load_df(
  df,  
  col_obs = "log_effect_size", 
  col_obs_se = "log_se_weighted",
  col_covs = as.list(covs),
  col_study_id = "nid")


log_cov_model = LogCovModel(
  alt_cov=c("b_0","b_1"),
  ref_cov=c("a_0","a_1"),
  use_re=T,
  use_spline=F,
  use_re_mid_point=T
)

model <- MRBRT(
  data = data,
  cov_models = list(log_cov_model),
  inlier_pct = 0.9)


model$fit_model(inner_print_level = 5L, inner_max_iter = 500L)

# make predictions
df_preds <- data.frame(
  b_0 = seq(0, 50, length.out = 1000),
  b_1 = seq(0, 50, length.out = 1000),
  a_0 = rep(0,times=1000),
  a_1= rep(0,times=1000))

dat_preds <- MRData()

dat_preds$load_df(
  data = df_preds, 
  col_covs = as.list(covs))


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

# make simple plot
pdf(paste0(step3_dir,"loglinear_covs.pdf"), width = 11, height = 8)
make_plot(df_preds, df_preds$b_0, df_preds$preds, df_preds$preds_lo, df_preds$preds_hi)
dev.off()

make_plot(df_preds, df_preds$b_0, df_preds$preds, df_preds$preds_lo, df_preds$preds_hi)

# save the model
saveRDS(model,paste0(step3_dir,"model.RDS"))
py_save_object(object = model, filename = paste0(step3_dir, "model.pkl"), pickle = "dill")

# get slope prior
slope <- mean(samples[[1]])
slope
saveRDS(slope, paste0(slope_dir,"slope.RDS"))

print("Done with step three:) ")

# Make new draws for PAF calculation
# make predictions
df_preds2 <- data.frame(
  b_0 = 10,
  b_1 = 10,
  a_0 = 0,
  a_1= 0)


dat_preds2 <- MRData()
dat_preds2$load_df(
  data = df_preds2,
  col_covs = as.list(covs)
)

df_preds2$preds <- model$predict(dat_preds2)
df_preds2 <- as.data.table(df_preds2)

# make draws
samples2 <- model$sample_soln(sample_size = as.integer(draws_req))

draws2 <- model$create_draws(
  data = dat_preds2,
  beta_samples = samples2[[1]],
  gamma_samples = samples2[[2]],
  random_study = T
)

# save draws for PAF calculation
out <- as.data.table(draws2)
setnames(out,paste0("draw_",0:999))
write.csv(out,paste0(draws_dir,"/draws.csv"),row.names = F)


# There isn't enough info to do a spline fit, so I stopped here.

# -------------------------Test for publication bias & get evidence score------------------

ro_pair <- "ozone_resp_copd"

work.dir <- paste0(h_root,"/air_pollution/air/rr/evidence_score") # this is where all the evidence score functions are saved
setwd(work.dir)
source("./src/utils/loglinear_functions.R")

ref_covs <- c("a_0","a_1")
alt_covs <- c("b_0","b_1")

### Load model objects
model <- py_load_object(filename = paste0(step3_dir,"model.pkl"), pickle = "dill")

data_info <- extract_data_info(model,
                               ref_covs = ref_covs,
                               alt_covs = alt_covs,
                               pred_exp_bounds = c(0,50),
                               num_points = 1000L)
data_info$ro_pair <- ro_pair
df <- data_info$df

### Detect publication bias
df_no_outlier <- df[!df$outlier,]
egger_model_all <- egger_regression(df$residual, df$residual_se)
egger_model <- egger_regression(df_no_outlier$residual, df_no_outlier$residual_se)
has_pub_bias <- egger_model$pval < 0.05

### Adjust for publication bias
if (has_pub_bias) {
  df_fill <- get_df_fill(df[!df$outlier,])
  num_fill <- nrow(df_fill)
} else {
  num_fill <- 0
}

# fill the data if needed and refit the model
if (num_fill > 0) {
  df <- rbind(df, df_fill)
  data_info$df <- df
  
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
uncertainty_info <- get_uncertainty_info(data_info, model)
if (is.null(model_fill)) {
  uncertainty_info_fill <- NULL
} else {
  uncertainty_info_fill <- get_uncertainty_info(data_info, model_fill)
}


### Output diagnostics
# figures
pdf(paste0(step5_dir,ro_pair,".pdf"),width = 11,height = 7)
title <- paste0(ro_pair, ": egger_mean=", round(egger_model$mean, 3),
                ", egger_sd=", round(egger_model$sd,3), ", egger_pval=", 
                round(egger_model$pval, 3))
plot_residual(df, title)

plot_model(data_info,
           uncertainty_info,
           model,
           uncertainty_info_fill,
           model_fill)

dev.off()

# summary
summary <- summarize_model(data_info,
                           uncertainty_info,
                           model,
                           egger_model,
                           egger_model_all,
                           uncertainty_info_fill,
                           model_fill)
summary

draws <- get_draws(data_info, model)

# save draws and summary
write.csv(draws,paste0(fisher_draws_dir,"draws.csv"),row.names = F)
draw_summary <- data.table(exposure = draws$exposure,
                           mean = apply(draws[,2:1001] , 1, function(x) mean(x)),
                           median = apply(draws[,2:1001] , 1, function(x) quantile(x, 0.5)),
                           lower = apply(draws[,2:1001] , 1, function(x) quantile(x, 0.025)),
                           upper = apply(draws[,2:1001] , 1, function(x) quantile(x, 0.975)))
write.csv(draw_summary,paste0(fisher_draws_dir,"draws_summary.csv"),row.names = F)

##### save draws for 10 ppb only for PAF calculation and upload
data_info <- extract_data_info(model,
                               ref_covs = ref_covs,
                               alt_covs = alt_covs,
                               pred_exp_bounds = c(10,10),
                               num_points = 1L)
data_info$ro_pair <- ro_pair
df <- data_info$df

draws <- get_draws(data_info, model)

# save draws and summary
write.csv(draws,paste0(fisher_draws_dir,"10ppb_draws.csv"),row.names = F)
draw_summary <- data.table(exposure = draws$exposure,
                           mean = apply(draws[,2:1001] , 1, function(x) mean(x)),
                           median = apply(draws[,2:1001] , 1, function(x) quantile(x, 0.5)),
                           lower = apply(draws[,2:1001] , 1, function(x) quantile(x, 0.025)),
                           upper = apply(draws[,2:1001] , 1, function(x) quantile(x, 0.975)))
write.csv(draw_summary,paste0(fisher_draws_dir,"10ppb_draws_summary.csv"),row.names = F)


### plot for capstone
plot_model_appendix(data_info,
                    uncertainty_info,
                    model,
                    uncertainty_info_fill,
                    model_fill)

plot_residual_appendix(df, title)


# -------------------------Upload to central drive-----------------------------------------

work.dir <- paste0(h_root,"/air_pollution/air/rr/evidence_score") # this is where all the evidence score functions are saved
setwd(work.dir)
source("./upload_loglinear.R")


pair_info <- list(
  air_ozone_resp_copd = list(
    rei_id = 88,
    cause_id = 509,
    risk_unit = "ppb",
    model_path = "FILEPATH/model.pkl"
  )
)


for (pair in names(pair_info)) {
  print(paste0("upload pair=", pair))
  results_folder <- file.path(ARCHIVE, pair)
  if (!dir.exists(results_folder)) {
    dir.create(results_folder)
  }
  do.call(upload_results, c(pair_info[[pair]], list(results_folder = results_folder)))
}

