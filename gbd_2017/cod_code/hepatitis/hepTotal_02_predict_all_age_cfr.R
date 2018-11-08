## ******************************************************************************
##
## Purpose: Estimate all-age CFR for every loc/year/sex based on HAQI
## Input:   - CFR model equation
##          - GBD 2017 HAQI covariate values
## Output:  All-age CFR for each loc/year/sex (one csv per loc)
##
## ******************************************************************************

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "/FILEPATH/"
  h <- "/FILEPATH/"
} else {
  j <- "J:"
  h <- "H:"
}

library(data.table)
library(plyr)
library(ggplot2)

source(paste0(j, "FILEPATH/interpolate.R"))
source(paste0(j, "FILEPATH/get_ids.R"))
source(paste0(j, "FILEPATH/get_population.R"))
source(paste0(j, "FILEPATH/get_model_results.R"))
source(paste0(j, "FILEPATH/get_covariate_estimates.R"))

#pull in location_id from bash command
args <- commandArgs(trailingOnly = TRUE)
loc <- args[1]

#------------------------------------------------------------------------------------
# 1. Pull HAQI for every year
#------------------------------------------------------------------------------------

haqi <- get_covariate_estimates(covariate_id=1099, location_id = loc)
haqi$sex_id <- NULL
haqi$age_group_id <- NULL
haqi$age_group_name <- NULL
setnames(haqi, 'mean_value', 'haqi')

#------------------------------------------------------------------------------------
# 2. Predict CFR using HAQI for every year
#------------------------------------------------------------------------------------

load(paste0(j, 'FILEPATH/model.rds'))
predictions <- unique(haqi[, .(location_id, year_id, haqi)])
haqi_inputs <- predictions[, list(haqi)]

predictions$logodds <- predict(model, newdata = haqi_inputs)
predictions[, odds := exp(logodds)]
predictions[, cfr := odds / (odds + 1)]

modelled_cfr <- predictions[, c('location_id', 'year_id', 'cfr')]

#duplicate each row to create a row for every sex (HAQI is same across sexes)
modelled_cfr[, sex_id := 1]
modelled_cfr_2 <- copy(modelled_cfr)
modelled_cfr_2[, sex_id := 2]
modelled_cfr <- rbind(modelled_cfr, modelled_cfr_2)

#save all-age CFR for every year and sex for future use
output_dir <- paste0(j, "FILEPATH/")

write.csv(modelled_cfr[, c('year_id', 'sex_id', 'cfr'), with=FALSE],
          paste0(output_dir, loc, ".csv"),
          row.names = FALSE)

