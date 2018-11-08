# Author: 
# Date: 10/27/17
# Purpose: 1) Prep VR data for graphing purposes

rm(list=ls())
library(data.table); library(haven); library(readr); library(readstata13); library(assertable); library(plyr); library(DBI); library(mortdb, lib = "FILEPATH"); library(mortcore, lib = "FILEPATH")
library(ggplot2)
if (Sys.info()[1] == "Linux") {
  root <- "/home/j" 
  user <- Sys.getenv("USER")
  version_id <- as.character(commandArgs(trailingOnly = T)[1])
} else {
  root <- "J:"
  user <- Sys.getenv("USERNAME")
  code_dir <- paste0("")
}

main_dir <- paste0("", version_id, "FILEPATH")
deaths <- data.table(read_dta(paste0(main_dir, "d00_compiled_deaths.dta")))

deaths <- deaths[, list(ihme_loc_id, year, sex, source_type)]

deaths[, n := seq(.N), by = c('ihme_loc_id', 'year', 'source_type')]
deaths[, maximum := max(n), by = c('ihme_loc_id', 'year', 'source_type')]
vr_both_sex_only <- deaths[maximum != 3, list(ihme_loc_id, year, source_type)]

write_csv(vr_both_sex_only, paste0(main_dir, "vr_both_sex_only.csv"))

# DONE