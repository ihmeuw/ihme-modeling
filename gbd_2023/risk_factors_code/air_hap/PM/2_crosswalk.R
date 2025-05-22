# Purpose: Generate mini-crosswalk ratios for female/male/child mapping exposures
#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
}

# load packages, install if missing

lib.loc <- paste0(h_root, "R/", R.Version()$platform,"/", R.Version()$major, ".", R.Version()$minor)
dir.create(lib.loc, recursive = T, showWarnings = F)
.libPaths(c(lib.loc, .libPaths()))

packages <- c("data.table", "magrittr", "lme4", "ggplot2", "mvtnorm")

for(p in packages){
  if(p %in% rownames(installed.packages()) == F){
    install.packages(p)
  }
  library(p, character.only = T)
}

library(data.table)
library(magrittr)
library(lme4)


in_date <- "082124"
date <- format(Sys.Date(), "%m%d%y")
disagg <- T

#------------------DIRECTORIES--------------------------------------------------

home_dir <- file.path("FILEPATH")
in_dataset <- paste0(home_dir, "FILEPATH/lmer_input_", in_date, ".csv")

# out
dir.create(paste0(home_dir, "FILEPATH", date), recursive=T)
dir.create(paste0(home_dir, "FILEPATH", date), recursive=T)

# central functions
source(file.path(central_lib, "FILEPATH/get_location_metadata.R"))
locs <- get_location_metadata(location_set_id=35, release_id = 16)
source(file.path(central_lib,"FILEPATH/get_covariate_estimates.R"))

# ---------------------- Scale female to male and child based on published studies --------------------------------

crosswalk <- fread(file.path(home_dir, "FILEPATH/personal_exp_factor_091620.csv"))
crosswalk <- crosswalk[!is.na(female_pm)]
crosswalk <- merge(crosswalk,locs[, .(location_id, location_name = location_ascii_name)], by="location_name", all.x = T)

crosswalk[location_name == "Republic of the Gambia", location_name := "Gambia"]
crosswalk[location_name == "Gambia", location_id:=206]

# Subtract off ambient
air_pm <- get_covariate_estimates(106,
                                  location_id = c(unique(crosswalk$location_id)),
                                  year_id = c(unique(crosswalk$year_id)),
                                  release_id = 16)

setnames(air_pm, "mean_value", "air_pm")
crosswalk <- merge(crosswalk, air_pm[, .(location_id,year_id,air_pm)], by=c("location_id", "year_id"), all.x = T)

# remove the duplicate rows made because "South Asia" is both a R & SR
crosswalk <- crosswalk[location_id != 158]

# Use published outdoor measurement for PM2.5 studies only (not including the PURE-HAP studies)
crosswalk[,outdoor := as.numeric(outdoor)]
crosswalk[Pollutant == "PM2.5" & Sno != 509,outdoor := air_pm]

crosswalk <- crosswalk[, .(location_id,year_id,location_name,Pollutant,kitchen_pm,kitchen_n,
                          female_pm,female_n,group,pm,n,outdoor)]

crosswalk[, kitchen_pm := kitchen_pm-outdoor]
crosswalk[, female_pm := female_pm-outdoor]
crosswalk[, pm := pm-outdoor]

# if the value is negative, replace with 1 (a ratio of - PM doesn't really mean anything)
crosswalk[kitchen_pm<1, kitchen_pm:=1]
crosswalk[female_pm<1, female_pm:=1]
crosswalk[pm<1, pm:=1]

# indicator whether or not it is PM2.5
crosswalk[, other_pm_size:=1]
crosswalk[Pollutant=="PM2.5", other_pm_size:=0]

# weight by total number of measurements
crosswalk[, weight:=female_n+n]

# Calculate ratio
crosswalk[, ratio:=pm/female_pm]

crosswalk_mod <- lm(data=crosswalk, log(ratio) ~ group + 0, weights=weight)
summary(crosswalk_mod)
exp(coefficients(crosswalk_mod))

  # coefficient matrix
  coeff_cw <- as.data.table(as.list(coefficients(crosswalk_mod)))
  coefmat <-matrix(unlist(coeff_cw), ncol=ncol(coeff_cw), byrow=T, dimnames=list(c("coef"), names(coeff_cw)))

  # covariance matrix
  vcovmat <- vcov(crosswalk_mod)
  vcovmat <- matrix(vcovmat, ncol=ncol(coeff_cw), byrow=T)

  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
  # transpose coefficient matrix
  betas <- t(betadraws)
  row.names(betas) <- names(coeff_cw)

# save the betas so we can use them for the overall solid mapping model
  saveRDS(betas,paste0(home_dir, "FILEPATH/crosswalk_betas.RDS"))


# ----------------------------------- summarize ratios --------------------------------------------------
betas_sum <- t(betas)
betas_sum <- exp(betas_sum)
betas_sum <- as.data.table(betas_sum)

ratio_summary <- data.table(type = c("child", "male"),
                            mean = c(mean(betas_sum$groupchild), mean(betas_sum$groupmale)),
                            median = c(quantile(betas_sum$groupchild,.5), quantile(betas_sum$groupmale, .5)),
                            lower = c(quantile(betas_sum$groupchild,.025), quantile(betas_sum$groupmale, 0.025)),
                            upper = c(quantile(betas_sum$groupchild,0.975), quantile(betas_sum$groupmale, 0.975)))
ratio_summary