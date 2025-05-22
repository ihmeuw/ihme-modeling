###################################################################
## Pull in data and adjust survey data
###################################################################

## set up
rm(list = ls())

pacman::p_load(plyr, openxlsx, ggplot2, metafor, msm, lme4, scales, data.table, boot, readxl, magrittr)
# Source all GBD shared functions at once
invisible(sapply(list.files("/FILEPATH/", full.names = T), source))

# Pull in crosswalk packages
Sys.setenv("RETICULATE_PYTHON" = "/FILEPATH") 
library(reticulate)
reticulate::use_python("/FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")

# Set-up variables
date <- gsub("-", "_", Sys.Date())
user <- Sys.info()["user"]
release <- 16
bundle_version_id <- 49439

# Save directory
save_dir <- paste0("/FILEPATH/", date, "/")
dir.create(paste0(save_dir), recursive = TRUE)

period <- fread(paste0(save_dir,"/FILEPATH",bundle_version_id,"_", date, ".csv"))
survey_plot_mrbrt <- fread(paste0(save_dir,"/FILEPATH",bundle_version_id,"_", date, ".csv"))

# Append new data
table(period$had_fever_survey)

table(is.na(period$had_fever_survey))
table(period$had_fever_survey)
table(period$cv_diag_selfreport)

period$cv_diag_selfreport[is.na(period$cv_diag_selfreport)] <- 0
period <- subset(period, cv_diag_selfreport==1)
period <- subset(period, !is.na(had_fever_survey))

period$mean_original <- period$mean
period$standard_error_original <- period$standard_error

period$log_mean <- log(period$mean)
period$log_mean_original <- period$log_mean
period$cv_had_fever <- period$had_fever_survey
period$cv_no_fever <- (1-period$had_fever_survey)
period$indicator <- with(period, ifelse(cv_diag_valid_good==1 & cv_had_fever==1, "Reference", ifelse(cv_diag_valid_good==1 & cv_had_fever==0,"Chest only",
                                                                                                     ifelse(cv_diag_valid_good==0 & cv_had_fever==1,"Difficulty and fever",
                                                                                                            "Difficulty only"))))



pcomp <- join(period, survey_plot_mrbrt, by=c("age_start"))

table(is.na(pcomp$indicator))
table(pcomp$indicator)
table(is.na(pcomp$cv_had_fever))
table(pcomp$cv_had_fever)
table(is.na(pcomp$cv_diag_valid_good))
table(pcomp$cv_diag_valid_good)
table(is.na(pcomp$cv_diag_valid_poor))
table(pcomp$cv_diag_valid_poor)

pcomp[is.na(cv_diag_valid_poor), cv_diag_valid_poor := 0]

#########################################################
## ADJUST MEANS BY THE SURVEY RATIOS (CROSSWALK)
#########################################################


pcomp$log_mean <- ifelse(pcomp$cv_diag_valid_good==1, ifelse(pcomp$cv_had_fever==0, pcomp$log_mean + pcomp$log_chest_ratio, pcomp$log_mean), pcomp$log_mean)
pcomp$log_mean <- ifelse(pcomp$cv_diag_valid_good==0, ifelse(pcomp$cv_had_fever==1, pcomp$log_mean + pcomp$log_diff_fever_ratio, pcomp$log_mean), pcomp$log_mean)
pcomp$log_mean <- ifelse(pcomp$cv_diag_valid_good==0, ifelse(pcomp$cv_had_fever==0, pcomp$log_mean + pcomp$log_diff_ratio, pcomp$log_mean), pcomp$log_mean)

## Get the linear standard errors ##
log_standard_error <- sapply(1:nrow(pcomp), function(i) {
  ratio_i <- pcomp[i, "mean"]
  ratio_se_i <- pcomp[i, "standard_error"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})
se_xw_lri2 <- sqrt(log_standard_error^2 + pcomp$log_chest_se^2)
se_xw_lri3 <- sqrt(log_standard_error^2 + pcomp$log_diff_fever_se^2)
se_xw_lri4 <- sqrt(log_standard_error^2 + pcomp$log_diff_se^2)

final_se <- ifelse(pcomp$indicator=="Reference", pcomp$standard_error, ifelse(pcomp$indicator=="Chest only", se_xw_lri2, ifelse(pcomp$indicator=="Difficulty and fever", se_xw_lri3, se_xw_lri4)))
final_se <- sapply(1:nrow(pcomp), function(i) {
  ratio_i <- pcomp[i, "log_mean"]
  ratio_se_i <- final_se[i]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})

pcomp$mean <- exp(pcomp$log_mean)
pcomp$standard_error <- ifelse(pcomp$indicator=="Reference", pcomp$standard_error, final_se)


pcomp$cases <- pcomp$mean * pcomp$sample_size
table(pcomp$cases < 0)

###### Save! ######
write.csv(pcomp, paste0(save_dir,"/FILEPATH", bundle_version_id,"_", date,".csv"))

# Move to step 4