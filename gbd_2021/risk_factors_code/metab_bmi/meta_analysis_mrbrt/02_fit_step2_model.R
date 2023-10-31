# STEP 2: Fit log linear model

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
work_dir <- "FILEPATH"

library(dplyr)
source(paste0(code_dir, "FILEPATH")) # Loads functions like add_ui and get_knots
library(ggplot2)
library(data.table)
source("FILEPATH")
source(paste0(code_dir, 'FILEPATH'))
cause_ids <- get_ids("cause")

# Set up arguments
args <- commandArgs(trailingOnly = TRUE)

cause_num <- as.numeric(args[1])
save_dir <- args[2]
fit_model <- args[3]

# Read in data
df <- fread(paste0(save_dir, "FILEPATH"))
df[, a_mid := (a_0 + a_1)/2][, b_mid := (b_0 + b_1)/2][, linear := b_mid - a_mid]
if(fit_model){
  
  set.seed(3197)
  
  dat2 <- MRData()
  
  dat2$load_df(
    data = df,
    col_obs = "obs",
    col_obs_se = "obs_se",
    col_study_id = "study_id",
    col_covs = as.list("signal")
  )
  
  cov_model <- list(LinearCovModel(
    alt_cov = "signal",
    use_re = TRUE,
    prior_beta_uniform=array(c(1.0, 1.0))
  ))
  
  mod2 <- MRBRT(data = dat2,
                cov_models = cov_model,
                inlier_pct = 1.0)
  
  mod2$fit_model(inner_print_level=5L, inner_max_iter=200L, outer_step_size=200L, outer_max_iter=100L)
  print(paste0("Saving to ", save_dir, "FILEPATH"))
  py_save_object(object = mod2,
                 filename = paste0(save_dir, "FILEPATH"),
                 pickle = "dill")
  print("success")
}