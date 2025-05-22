rm(list = ls())
library(data.table)
library(readr)
library(boot)
library(assertable)
library(RMySQL)
library(ggplot2)
library(ggrepel)
library(mortdb)
library(mortcore)

if (interactive()) {
  user <- "USERNAME"
  version_id <- "Run id"
} else {
  user <- "USERNAME"
  version_id <- as.numeric(commandArgs(trailingOnly = T)[1])
}

output_dir <- "FILEPATH"
input_dir <- "FILEPATH"
asfr <- fread("FILEPATH/compiled_gpr_results.csv")
age_map <- fread("FILEPATH/age_map.csv")
loc_map <- fread("FILEPATH/loc_map.csv")

setnames(age_map, "age_group_name_short", "age")

asfr[, year_id := floor(year)]
asfr <- asfr[age %in% c(seq(10, 50, 5))]

asfr <- merge(asfr, age_map[age_group_id %in% 7:15, .(age_group_id, age)], by = "age")
asfr <- merge(asfr, loc_map[, .(ihme_loc_id, location_id)], by = "ihme_loc_id")

asfr[, age := NULL]
asfr[, ihme_loc_id := NULL]
asfr[, year := NULL]

write_csv(asfr, "FILEPATH/asfr_for_upload.csv")

upload_results("FILEPATH/asfr_for_upload.csv", "asfr", "estimate", version_id, send_slack = T)
