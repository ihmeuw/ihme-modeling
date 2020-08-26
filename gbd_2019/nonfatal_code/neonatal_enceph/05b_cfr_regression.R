## ******************************************************************************
##
## Purpose: Pull CFR and HAQi for regression
##
## ******************************************************************************
rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH/j/"
  h <- paste0("FILEPATH", Sys.getenv("USER"),"/")
}

pacman::p_load(data.table, openxlsx)
source("FILEPATH/get_covariate_estimates.R")

#Prep a data file to contain HAQI and CFR data
prep_file <- fread('FILEPATH/neonatal_enceph_cfr_prepped.csv')
prep_file <- prep_file[year >= 1980]

#pull haqi to use as covariate
haqi <- get_covariate_estimates(1099, decomp_step='step4')[,.(location_id,year_id,mean_value)]
setnames(haqi,"mean_value","haqi")
setnames(haqi, 'year_id', 'year')

update_file <- merge(prep_file, haqi, by = c('location_id', 'year'), all.x = TRUE)
nrow(update_file[is.na(haqi)])
update_file <- update_file[!is.na(haqi)]

update_file <- update_file[, -c('NMR', 'ln_NMR')]

write.csv(update_file, file = 'FILEPATH/neonatal_enceph_cfr_prepped_haqi.csv',
          row.names = FALSE, na = '')
