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

packages <- c("data.table","magrittr","ggplot2","openxlsx","metafor")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

'%ni%' <- Negate("%in%")

#------------------Directories and shared functions-----------------------------
model.version <- VERSION

out_dir <- paste0("FILEPATH", model.version,"/")
dir.create(out_dir,recursive = T)
draws_dir <- paste0("FILEPATH",model.version,"/")
dir.create(draws_dir,recursive = T)


# load shared functions
source("FILEPATH/get_outputs.R")
source("FILEPATH/get_covariate_estimates.R")

# load current locations list
source("FILEPATH/get_location_metadata.R")
location_set_version <- 35
locations <- get_location_metadata(location_set_version, gbd_round_id=7)

# load MR-BRT functions
library(mrbrt001, lib.loc="FILEPATH")


# load covs or model?
run_covs <- F
save_covs <- F
load_covs <- T

draws_req <- 1000

#------------------Load data & prep covs----------------------------

# load the prepped data
df <- read.csv(paste0("FILEPATH",model.version,"/input_data.csv")) %>% as.data.table

setnames(df,c("NO2_conc_95","NO2_conc_5"),c("NO2_conc_alt","NO2_conc_ref")) # using this for the range
# set exposure names for the ratio model
df[,b_0:=NO2_conc_alt]
df[,b_1:=NO2_conc_alt]
df[,a_0:=NO2_conc_ref]
df[,a_1:=NO2_conc_ref]

# use this for covariate selection
df[,linear_exp:=NO2_conc_alt-NO2_conc_ref]

# make stand-in dataset to plot Khreis et al. (2017) results
# covert the study's reporting exposure increment of 4 ug/m3 to 1 ppb
khreis <- data.table(NO2_conc_alt = seq(0,100,length.out = 100))
khreis[,log_rr_mean:= log(1.05^(1/(4*0.5319)))*NO2_conc_alt]
khreis[,log_rr_lo := log(1.02^(1/(4*0.5319)))*NO2_conc_alt]
khreis[,log_rr_hi := log(1.07^(1/(4*0.5319)))*NO2_conc_alt]

# simple plotting function
make_plot <- function(dfpred, altvar, predmean, predlo, predhi){
  p <- ggplot()+
    
    # plot the predicted fit
    geom_ribbon(data = dfpred, aes(x = altvar, ymin = predlo, ymax = predhi, linetype = "GBD 2020"), fill = "green", alpha = 1/5) +
    geom_line(data = dfpred, aes(x = altvar, y = predmean, linetype = "GBD 2020")) +
    
    # plot Khreis et al. fit
    geom_ribbon(data = khreis[NO2_conc_alt<=70], aes(x = NO2_conc_alt, ymin = log_rr_lo, ymax = log_rr_hi, linetype = "Khreis et al."), fill = "black", alpha = 0.2) +
    geom_line(data = khreis[NO2_conc_alt<=70], aes(x = NO2_conc_alt, y = log_rr_mean, linetype = "Khreis et al.")) +
   
    # add lines to mark the 10th and 90th percentiles of input data exposure
    geom_vline(aes(xintercept=lower_domain),linetype="dotted",color="black") +
    geom_vline(aes(xintercept=upper_domain),linetype="dotted",color="black") +
    
    # add a line to mark log(0) - baseline
    geom_abline(slope = 0, intercept = 0, color = "blue") +
    
    # theme/formatting
    xlab("NO2 exposure (ppb)") +
    ylab("Log(RR)") +
    labs(linetype = "Fit") +
    scale_fill_manual(values = c("GBD 2020" = "green", "Khreis et al." = "black")) +
    guides(alpha = FALSE) +
    theme_classic()
  
  print(p)
}


# -------------------------  Run Log-linear model with exposure to get posterior standard deviation for beta ------------------------------------------
# load the data frame
data <-  MRData()

data$load_df(
  df,
  col_obs = "log_mean_rescaled",
  col_obs_se = "log_se_weighted",
  col_covs = list("b_0","b_1","a_0","a_1"),
  col_study_id = "nid")


lin_cov_model = LinearCovModel(
  alt_cov=c("b_0","b_1"),
  ref_cov=c("a_0","a_1"),
  use_re=T,
  use_spline=F,
  use_re_mid_point=T
)

model <- MRBRT(
  data = data,
  cov_models = list(lin_cov_model),
  inlier_pct = 0.9)

model$fit_model(inner_print_level = 5L, inner_max_iter = 500L)

# make predictions
df_preds <- data.frame(
  b_0 = seq(0, 100, length.out = 1000),
  b_1 = seq(0, 100, length.out = 1000),
  a_0 = rep(0,times=1000),
  a_1= rep(0,times=1000))

dat_preds <- MRData()
dat_preds$load_df(
  data = df_preds,
  col_covs = list("b_0","b_1","a_0","a_1")
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

# get SD for betas
beta_sd <- sd(samples[[1]])
beta_sd

saveRDS(beta_sd, file = paste0(out_dir,"beta_sd_prior.RDS"))

# beta_sd <- readRDS(paste0(out_dir,"beta_sd_prior.RDS"))


# -------------------------   Covariate selection using log-linear model ------------------------------------------

if (run_covs){
  # prep data using MR-BRT format
  data_cov <- MRData()
 
  data_cov$load_df(
    data=df, col_obs="log_mean_rescaled", col_obs_se="log_se_weighted",
    col_covs=list("linear_exp","cv_exp_subpopulation","cv_exp_exposure_population","cv_exp_PM_controlled","cv_exp_outcome_selfreport",
                  "cv_exp_confounding_uncontrolled_1","cv_exp_confounding_uncontrolled_2","cv_exp_selection_bias_1","cv_exp_selection_bias_2",
                  "cv_exp_exp_prenatal","cv_exp_exp_lifetime","cv_exp_exp_birth",
                  "cv_exp_exp_yo_incidence","cv_exp_duration_fup"),
    col_study_id="nid"
    
  )
  

  # for GBD 2020, we did not include exposure timeframe covs
  candidate_covs <- list("cv_exp_subpopulation","cv_exp_exposure_population","cv_exp_PM_controlled","cv_exp_outcome_selfreport",
                         "cv_exp_confounding_uncontrolled_1","cv_exp_confounding_uncontrolled_2","cv_exp_selection_bias_1","cv_exp_selection_bias_2",
                         "cv_exp_duration_fup")
  
  
  covfinder <- CovFinder(
    data = data_cov,
    covs = as.list(candidate_covs),
    pre_selected_covs = list("linear_exp"),
    num_samples = 1000L,
    power_range = list(-4,4),
    power_step_size = 0.05,
    laplace_threshold = 1e-5,
    inlier_pct = 0.9,
    normalized_covs = T,
    beta_gprior_std = 0.1*beta_sd,
    bias_zero = T
  )
  
  covfinder$select_covs(verbose=T)
  
  covs <- covfinder$selected_covs
  
  covs <- covs[covs%ni%"linear_exp"]
  covs <- c(covs,"a_0","a_1","b_0","b_1")
  
  covs
  
  # save covs for later
  if(save_covs){
    saveRDS(covs, paste0(out_dir,"/selected_covs.RDS"))
  }
}

if(load_covs){
  covs <- readRDS(paste0(out_dir,"/selected_covs.RDS"))
}

# use this when running without covs
# covs <- "linear_exp"

# check distribution of the covs
# hist(df$cv_retrospective_cohort,breaks = 2,main="Retrospective cohort",xlab="cv_retrospective_cohort")


# -------------------------  Get effect size with covs for NO2 exposure:)  ------------------------------------------

data <- MRData()
data$load_df(
  data = df,  
  col_obs = "log_mean_rescaled", 
  col_obs_se = "log_se_weighted",
  col_covs = as.list(covs),
  col_study_id = "nid" 
  )

lin_cov_model = LinearCovModel(
  alt_cov=c("b_0","b_1"),
  ref_cov=c("a_0","a_1"),
  use_re=T,
  use_spline=F,
  use_re_mid_point=T
)

model <- MRBRT(
  data = data,
  cov_models = list(lin_cov_model,
    LinearCovModel(covs[1],use_re=F,prior_beta_gaussian=array(c(0, 0.1*beta_sd))),
    LinearCovModel(covs[2],use_re=F,prior_beta_gaussian=array(c(0, 0.1*beta_sd))),
    LinearCovModel(covs[3],use_re=F,prior_beta_gaussian=array(c(0, 0.1*beta_sd)))),
  inlier_pct = 0.9
  )

model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

# make predictions
df_preds <- data.frame(
  b_0 = c(seq(0, 100, length.out = 1000)),
  b_1 = c(seq(0, 100, length.out = 1000)),
  a_0 = 0,
  a_1 = 0,
  cv_exp_confounding_uncontrolled_2 = 0,
  cv_exp_selection_bias_2 = 0,
  cv_exp_outcome_selfreport = 0)

dat_preds <- MRData()
dat_preds$load_df(
  data = df_preds,
  col_covs = as.list(covs)
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

# make simple plot

input_exp <- c(df$NO2_conc_alt,df$NO2_conc_ref)
lower_domain = quantile(input_exp,0.15)
upper_domain = quantile(input_exp,0.85)

pdf(paste0(out_dir,"/linearfit_covs.pdf"), width = 11, height = 8)
make_plot(df_preds[b_0<=70],df_preds[b_0<=70,b_0],
          df_preds[b_0<=70,preds],df_preds[b_0<=70,preds_lo],df_preds[b_0<=70,preds_hi])
dev.off()

make_plot(df_preds,df_preds[,b_0],
          df_preds[,preds],df_preds[,preds_lo],df_preds[,preds_hi])


# save model
saveRDS(model,paste0(out_dir,"model.RDS"))
py_save_object(object = model, filename = paste0(out_dir, "model.pkl"), pickle = "dill")

# save draws for PAF calculation (using the draws predicted for exposures)
out <- as.data.table(draws)
setnames(out,paste0("draw_",0:999))
write.csv(out,paste0(draws_dir,"draws.csv"),row.names = F)


# Make new draws to check effect size at 5 ppb
# make predictions
df_preds2 <- data.frame(
  b_0 = 5,
  b_1 = 5,
  a_0 =0,
  a_1 = 0,
  cv_exp_outcome_selfreport = 0,
  cv_exp_confounding_uncontrolled_2 = 0,
  cv_exp_selection_bias_2 = 0)

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
  beta_samples = samples[[1]],
  gamma_samples = samples[[2]],
  random_study = T
)

# check the effect size at 5 ppb
temp <- exp(draws2)
mean(temp)
quantile(temp,0.025)
quantile(temp,0.975)


# -------------------------Test for publication bias & get evidence score------------------

ro_pair <- "no2_asthma"
fisher_dir <- paste0("FILEPATH", model.version,"/")
dir.create(fisher_dir,recursive = T)

work.dir <- paste0(h_root,"FILEPATH") # this is where all the evidence score functions are saved
setwd(work.dir)
source("FILEPATH/loglinear_functions.R")

model_path <- paste0(out_dir,"model.RDS")
ref_covs <- c("a_0","a_1")
alt_covs <- c("b_0","b_1")


### Load model objects
model <- py_load_object(filename = paste0(out_dir,"model.pkl"), pickle = "dill")

data_info <- extract_data_info(model,
                               ref_covs = ref_covs,
                               alt_covs = alt_covs,
                               pred_exp_bounds = c(0,100),
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
pdf(paste0("FILEPATH",model.version,"/",ro_pair,".pdf"),width = 11,height = 7)
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
write.csv(draws,paste0(fisher_dir,"draws.csv"),row.names = F)
draw_summary <- data.table(exposure = draws$exposure,
                           mean = apply(draws[,2:1001] , 1, function(x) mean(x)),
                           median = apply(draws[,2:1001] , 1, function(x) quantile(x, 0.5)),
                           lower = apply(draws[,2:1001] , 1, function(x) quantile(x, 0.025)),
                           upper = apply(draws[,2:1001] , 1, function(x) quantile(x, 0.975)))
write.csv(draw_summary,paste0(fisher_dir,"draws_summary.csv"),row.names = F)

# -------------------------Upload to central drive-----------------------------------------

work.dir <- paste0(h_root,"FILEPATH") # this is where all the evidence score functions are saved
setwd(work.dir)
source("./upload_loglinear.R")


pair_info <- list(
  air_no2_resp_asthma = list(
    rei_id = 404,
    cause_id = 515,
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
