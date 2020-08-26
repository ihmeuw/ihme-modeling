####################################################################
## The goal here is to crosswalk rotavirus proportion data to a
## level where no vaccine coverage is the reference. This will be part
## of a proposed update to rotavirus attributable fraction where
## the attribution will be based on a probe approach to determine
## the reduction in attribution due to vaccine in an explicit way
####################################################################
## Source fuctions, import required information. ##
# Prepare your needed functions and information
library(metafor)
library(msm)
library(plyr)
library(boot)
library(data.table)
library(openxlsx)
library(ggplot2)
library(scales)
library(viridis)
library(MuMIn, lib.loc="filepath")

source() # filepaths to central functions for data upload/download
source() # filepaths to central functions for MR-BRT
source("/filepath/bundle_crosswalk_collapse.R")

  locs <- read.csv("filepath")
  loc_pull <- locs$location_id[locs$level>=3]
  ages <- c(2:20,30:32, 235)

#####################################
## Get rotavirus vaccine coverage, SDI ##
  rcov <- get_covariate_estimates(covariate_id=1075, location_id=loc_pull, year_id=1990:2019, gbd_round_id=6, decomp_step="step4")
  setnames(rcov, c("mean_value","lower_value","upper_value"), c("rota_cov","rota_cov_lower","rota_cov_upper"))

  sdi <- get_covariate_estimates(covariate_id=881, location_id=loc_pull, year_id=1990:2019, gbd_round_id=6, decomp_step="step4")
  setnames(sdi, c("mean_value","lower_value","upper_value"), c("sdi","sdi_lower","sdi_upper"))

#############################################################################################
## Load in the file for the predicted values of rotavirus VE.
## These estimates come from an MR-BRT analysis of RCTs, CCs, and Before/after studies
## and in that analysis, SDI was chosen as the best predictor in MR_BRT Lasso
#############################################################################################
rota_ve <- read.csv("filepath")

#########################################################
## Pull in rotavirus data after crosswalking in MR-BRT ##
  # Adjusted for sensitivity/specificity
    df <- read.xlsx("filepath")

  # Unadjusted for sensitivity/specificity
    #df <- read.xlsx("filepath")

## OR, pull in rotavirus data after crosswalking and adjusting for PCR!
  #df <- read.xlsx("filepath")

  df <- subset(df, !is.na(nid))

# Some have cases > sample size 
  df$case_hold <- df$cases
  df$ss_hold <- df$sample_size

  df$track <- ifelse(df$case_hold > df$ss_hold, 1, 0)
  df$cases <- ifelse(df$track == 1, df$ss_hold, df$cases)
  df$sample_size <- ifelse(df$track == 1, df$case_hold, df$sample_size)

  df$year_id <- floor((df$year_start + df$year_end)/2)
  df$year_id <- ifelse(df$year_id < 1990, 1990, df$year_id)

  df <- join(df, rcov[,c("location_id","year_id","rota_cov","rota_cov_lower","rota_cov_upper")], by=c("location_id","year_id"))
  df <- join(df, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))

  df <- join(df, rota_ve[,c("location_id","year_id","pred_ve","pred_se","pred_lb","pred_ub")], by=c("location_id","year_id"))
  
  # Drop locations that don't match ("Western Europe", "Southern sub-Saharan Africa")
  df <- subset(df, !is.na(pred_ve))
  
## Great! Now determine the impact of the vaccine ##
  df$rota_se <- (df$rota_cov_upper - df$rota_cov_lower) / 2 / qnorm(0.975)
  df$impact <- 1 - df$rota_cov * df$pred_ve
  df$se_impact <- sqrt(with(df, rota_se^2 * pred_se^2 + rota_se^2*pred_ve^2 + rota_cov^2*pred_se^2))

  df$mean_adj_ve <- df$mean / df$impact
  df$impact_mean <- 1/df$impact
  df$se_adj_ve <- sqrt(with(df, standard_error^2 * se_impact^2 + standard_error^2*impact_mean^2 + mean^2*se_impact^2))

  # If the age_start < 5 and vaccine coverage > 0, some children
  # will receive the vaccine and those data should be adjusted
  # as if there was no vaccine.

  df$mean_record <- df$mean
  df$se_record <- df$standard_error
  df$mean <- ifelse(df$age_start < 5, df$mean_adj_ve, df$mean)
  df$standard_error <- ifelse(df$age_start < 5 & df$rota_cov > 0, df$se_adj_ve, df$standard_error)

  # Bound this 0-1?
  # Make sure they don't exceed 1.
    df$mean <- ifelse(df$mean >= 1, 0.99, df$mean)
    df$standard_error <- ifelse(df$standard_error > 1, 0.99, df$standard_error)
    df$cases <- ""

  ggplot(df, aes(x=mean_record, y=mean_adj_ve, col=rota_cov)) + geom_point() + theme_bw() +
    scale_x_continuous("Reported proportion", labels=percent) + scale_y_continuous("Adjusted proportion (vaccine impact)", labels=percent) +
    scale_color_viridis("Vaccine coverage")

  ggplot(df, aes(x=mean_record, y=mean, col=rota_cov)) + geom_point(size=2, alpha=0.5) + theme_bw() +
    scale_x_continuous("Reported proportion", labels=percent) + scale_y_continuous("Adjusted proportion (vaccine impact)", labels=percent) +
    scale_color_viridis("Vaccine coverage")
  ggplot(df, aes(x=se_record, y=standard_error, col=rota_cov)) + geom_point(size = 2) + theme_bw() +
    scale_x_continuous("Reported standard_error", labels=percent) + scale_y_continuous("Adjusted standard_error (vaccine impact)", labels=percent) +
    scale_color_viridis("Vaccine coverage")

## Remove some columns, save!

  df <- subset(df, !is.na(mean))
  df <- df[,-which(names(df) %in% c("rota_cov_lower","rota_cov_upper","dsev_lower","dsev_upper","dsev_match"))]
  df[is.na(df)] <- ""
  write.xlsx(df, "filepath", sheetName="extraction")

  ## Save a version for step 4
  write.xlsx(df[df$gbd_2019_new == 1, ], "filepath", sheetName="extraction")

###############################################
  ## Step 2 ##
  ## We need to get a single bundle_version_id in iterative for all crosswalk versions. ##
  s <- save_bundle_version(bundle_id = 12, decomp_step="step2")
  save_crosswalk_version(bundle_version_id = 11954, data_filepath = "filepath",
                         description = "Step2, Rotavirus post-crosswalking, & Se/Sp adjusted: VE (mid-year, all estimates, U5)")

###############################################
  ## Step 4 ##
  ## We need to get a single bundle_version_id in iterative for all crosswalk versions.##
  s <- save_bundle_version(bundle_id = 12, decomp_step="step4")
  save_crosswalk_version(bundle_version_id = s$bundle_version_id, data_filepath = "filepath",
                         description = "Step4, Rotavirus post-crosswalking, & Se/Sp adjusted: VE (mid-year, all estimates, U5)")
