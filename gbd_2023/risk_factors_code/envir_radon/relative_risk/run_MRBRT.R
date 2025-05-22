#-------------------Header------------------------------------------------

# Purpose: Fit MR-BRT models to radon RR data

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
sge.output.dir <- " -o ADDRESS -e ADDRESS "

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
run_covs <- T
save_covs <- T
load_covs <- F
no_covs <- T

draws_req <- 1000

version <- 9 

#------------------Directories and shared functions-----------------------------
# create out_dir and directories for each step
out_dir <- paste0("FILEPATH/",version,"/")
step1_dir <- paste0(out_dir,"/01_loglinear/")
beta_sd_dir <- paste0(step1_dir,"beta_sd/")
step2_dir <- paste0(out_dir,"/02_covariates/")
step3_dir <- paste0(out_dir,"/03_loglinear_covs/")
slope_dir <- paste0(step3_dir,"slope/")
step4_dir <- paste0(out_dir,"/04_spline/")
step5_dir <- paste0(out_dir,"/05_evidence_score/")
draws_dir <- paste0("FILEPATH/",version,"/")
fisher_draws_dir <- paste0("FILEPATH/",version,"/")

# Output directory for each stage
DIRECTORIES <- c(out_dir,step1_dir,beta_sd_dir,step2_dir,step3_dir,slope_dir,step4_dir,step5_dir,draws_dir,fisher_draws_dir) 

for (dir in DIRECTORIES) {
  if (!dir.exists(dir)) {
    dir.create(dir,recursive = T)
  } else {
    warning("Directory '", dir, "' already exists")
  }
}


# load shared functions
source("FILEPATH/get_outputs.R")
source("FILEPATH/get_covariate_estimates.R")

# load current locations list
source("FILEPATH/get_location_metadata.R")
location_set_version <- 35
locations <- get_location_metadata(location_set_version, gbd_round_id=7)

# load MR-BRT functions
library(mrbrt001, lib.loc="/ihme/code/mscm/R/packages/")

# make a dummy table of final GBD2019 results to use for plotting
mrbrt <- fread(paste0("FILEPATH/summary.csv"))

mrbrt <- log(mrbrt)

gbd19 <- data.table(linear_exp = seq(0,500,length.out = 1000))
gbd19[,log_rr_mean:= mrbrt$mean*(linear_exp/100)]
gbd19[,log_rr_lo := mrbrt$lower*(linear_exp/100)]
gbd19[,log_rr_hi := mrbrt$upper*(linear_exp/100)]


# make a simple plotting function

# simple plotting function
make_plot <- function(dfpred, altvar, predmean, predlo, predhi){
  p <- ggplot() +
  
    # plot the datapoints
    geom_point(data=df, aes(x=exposure, y=log_effect_size, alpha = 1/log_effect_size_se), color = "black") +

    # plot gbd19 fit
    geom_ribbon(data = gbd19, aes(x = linear_exp, ymin = log_rr_lo, ymax = log_rr_hi, linetype = "GBD 2019"), fill = "black", alpha = 0.2) +
    geom_line(data = gbd19, aes(x = linear_exp, y = log_rr_mean, linetype = "GBD 2019")) +
    
    # plot the predicted fit
    geom_ribbon(data = dfpred, aes(x = altvar, ymin = predlo, ymax = predhi, linetype = "GBD 2020"), fill = "green", alpha = 1/5) +
    geom_line(data = dfpred, aes(x = altvar, y = predmean, linetype = "GBD 2020")) +
    
    # # add line to mark the effect size per 100 Bq/m^3
    # geom_hline(aes(yintercept = mrbrt$mean),color = "gray") +
    # geom_hline(aes(yintercept = mean(predmean)), color = "red") +
    
    # add lines to mark the 10th and 90th percentiles of input data exposure
    geom_vline(aes(xintercept=lower_domain),linetype="dotted",color="black") +
    geom_vline(aes(xintercept=upper_domain),linetype="dotted",color="black") +
    
    # add a line to mark log(0) - baseline
    geom_abline(slope = 0, intercept = 0, color = "blue") +
    
    # theme/formatting
    xlab("Radon exposure (Bq/m^3)") +
    ylab("Log(RR)") +
    labs(linetype = "Fit") +
    # scale_fill_manual(values = c("GBD 2020" = "green", "GBD 2019" = "black")) +
    guides(alpha = FALSE) +
    theme_classic()
  
  print(p)
}


#------------------Load data & prep covs----------------------------

# load input data
df <- read.csv(file.path("/ihme/erf/GBD2020/envir_radon/rr/model/",version,"/input_data.csv")) %>% as.data.table
df[,linear_exp:=100] # because we scaled them all to 100

# all of these RRs were scaled to 10 ppb
df[,a_0:=0]
df[,a_1:=0]
df[,b_0:=100]
df[,b_1:=100]


print("Done with data prep:)")



beta_sd <- readRDS(paste0(beta_sd_dir,"radon_betasd.RDS"))


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

if (no_covs){

  model <- MRBRT(
    data = data,
    cov_models = list(log_cov_model),
    inlier_pct = 0.9)
  
} else {
  
  
}
  
model$fit_model(inner_print_level = 5L, inner_max_iter = 500L)
  

# make predictions
  
if (no_covs){
    
  df_preds <- data.frame(
      b_0 = seq(0, 500, length.out = 1000),
      b_1 = seq(0, 500, length.out = 1000),
      a_0 = rep(0,times=1000),
      a_1= rep(0,times=1000))
  
  } else {
    

  }
  
  
dat_preds <- MRData()
  
if (no_covs){
  
  dat_preds$load_df(
    data = df_preds, 
    col_covs = as.list(covs))
  
} else {
  
  
}
  
  
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
make_plot(df_preds, df_preds$linear_exp, df_preds$preds, df_preds$preds_lo, df_preds$preds_hi)
dev.off()

# save model
saveRDS(model,paste0(step3_dir,"model.RDS"))
py_save_object(object = model, filename = paste0(step3_dir, "model.pkl"), pickle = "dill")

# get slope prior
slope <- mean(samples[[1]])
slope
saveRDS(slope, paste0(slope_dir,"slope.RDS"))


df_preds2 <- data.frame(
  b_0 = 100,
  b_1 = 100,
  a_0 = 0,
  a_1 = 0)


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


print("Done with step three:) ")


# -------------------------Test for publication bias & get evidence score------------------

ro_pair <- "radon_neo_lung"

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
                               pred_exp_bounds = c(0,500),
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

##### save draws for 100 ppb only for PAF calculation and upload
data_info <- extract_data_info(model,
                               ref_covs = ref_covs,
                               alt_covs = alt_covs,
                               pred_exp_bounds = c(100,100),
                               num_points = 1L)
data_info$ro_pair <- ro_pair
df <- data_info$df

draws <- get_draws(data_info, model)

# save draws and summary
write.csv(draws,paste0(fisher_draws_dir,"100ppb_draws.csv"),row.names = F)
draw_summary <- data.table(exposure = draws$exposure,
                           mean = apply(draws[,2:1001] , 1, function(x) mean(x)),
                           median = apply(draws[,2:1001] , 1, function(x) quantile(x, 0.5)),
                           lower = apply(draws[,2:1001] , 1, function(x) quantile(x, 0.025)),
                           upper = apply(draws[,2:1001] , 1, function(x) quantile(x, 0.975)))
write.csv(draw_summary,paste0(fisher_draws_dir,"100ppb_draws_summary.csv"),row.names = F)


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
  envir_radon_neo_lung = list(
    rei_id = 90,
    cause_id = 426,
    risk_unit = "becquerels per cubic meter",
    model_path = "/ihme/erf/GBD2020/envir_radon/rr/model/9/03_loglinear_covs/model.pkl"
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


