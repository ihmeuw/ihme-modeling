#--------------------------------------------------------------
# Name: NAME (USERNAME)
# Date: 6 July 2022
# Project: GBD nonfatal COVID
# Purpose: pull model coefficients with SD
#--------------------------------------------------------------

# setup -------------------------------------------------------

# clear workspace
rm(list=ls())
setwd("FILEPATH")

# map drives
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH'
  h_root <- '~/'
} else {
  j_root <- 'FILEPATH/'
  h_root <- 'FILEPATH/'
}


# load packages
pacman::p_load(data.table, ggplot2, DBI, openxlsx, gtools)
library(reticulate) 
use_python("FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")
library(plyr)
library(msm)

folder <- "FILEPATH"
outputfolder <- "FILEPATH"

version <- 81
i <- 0



get_spline_posteriors <- function(model_object) {
  stdevs_tmp <- apply(X = core$other_sampling$sample_simple_lme_beta(
    sample_size = 1000L, model = model_object
  ), MARGIN = 2, FUN = sd)
  
  list(
    betas = model_object$beta_soln,
    stdevs = stdevs_tmp
  )
}


folders <- c(paste0(outputfolder, "duration_v", version, "/mod1_offset0.pkl"),
                paste0(outputfolder, "duration_v", version, "/mod1_all.pkl"),
                paste0(outputfolder, "any_v", version, "/mod1_h_all.pkl"),
                paste0(outputfolder, "any_v", version, "/mod1_c_all.pkl"),
                paste0(outputfolder, "fat_v", version, "/mod1_h_all.pkl"),
                paste0(outputfolder, "fat_v", version, "/mod1_c_all.pkl"),
                paste0(outputfolder, "rsp_v", version, "/mod1_h_all.pkl"),
                paste0(outputfolder, "rsp_v", version, "/mod1_c_all.pkl"),
                paste0(outputfolder, "cog_v", version, "/mod1_h_all.pkl"),
                paste0(outputfolder, "cog_v", version, "/mod1_c_all.pkl"),
                paste0(outputfolder, "cog_rsp_v", version, "/mod1.pkl"),
                paste0(outputfolder, "fat_cog_v", version, "/mod1.pkl"),
                paste0(outputfolder, "fat_rsp_v", version, "/mod1.pkl"),
                paste0(outputfolder, "fat_cog_rsp_v", version, "/mod1.pkl"),
                paste0(outputfolder, "mild_cog_v", version, "/mod1.pkl"),
                paste0(outputfolder, "mod_cog_v", version, "/mod1.pkl"),
                paste0(outputfolder, "mild_rsp_v", version, "/mod1.pkl"),
                paste0(outputfolder, "mod_rsp_v", version, "/mod1.pkl"),
                paste0(outputfolder, "sev_rsp_v", version, "/mod1.pkl"))


for (file in folders) {
  
#  file <- paste0(outputfolder, "duration_v", version, "/mod1_all.pkl")
    
  fit <- py_load_object(filename = file, pickle = "dill")
  fit$cov_names
  coeffs <- get_spline_posteriors(fit)
  print(get_spline_posteriors(fit))
  save <- data.table(rbind(fit$cov_names, coeffs$betas, coeffs$stdevs))
  x <- c("covariate", "mean", "sd")
  save <- cbind(save, x)
  write.csv(save, paste0(file, "_coeffs_sd.csv"), row.names = FALSE)
}







