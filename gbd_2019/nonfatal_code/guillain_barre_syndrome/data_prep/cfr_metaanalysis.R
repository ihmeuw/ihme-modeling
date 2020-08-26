###########################################################
### Author: USER
### Date: 10/27/2017
### Project: Survival Rate Adjustment
### Purpose: GBD 2017 Nonfatal Estimation
###########################################################

## SET-UP
rm(list=ls())

if (Sys.info()[1] == "Linux"){
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else if (Sys.info()[1] == "Windows"){
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

pacman::p_load(data.table, ggplot2, readr, boot)
library("meta", lib.loc = paste0(j_root, "FILEPATH"))
library("openxlsx", lib.loc = paste0(j_root, "FILEPATH"))

## SET OBJECTS
bundle <- 278
functions_dir <- paste0(j_root, "FILEPATH")
date <- gsub("-", "_", Sys.Date())

## SOURCE CENTRAL FUNCTIONS
source(paste0(functions_dir, "get_epi_data.R"))
source(paste0(functions_dir, "upload_epi_data.R"))

## USER FUNCTIONS
get_gbs_data <- function(hospital = F, marketscan = F){
  dt <- get_epi_data(bundle_id = bundle)
  if (hospital == T){
    dt <- dt[extractor == "USER"]
    dt <- dt[source_type == "Facility - inpatient"]
  } else if (marketscan == T){
    dt <- dt[extractor == "USER"]
    dt <- dt[source_type == "Facility - other/unknown"]
  }
  dt <- dt[measure == "incidence"]
  dt <- dt[!grepl("cfr", note_modeler)]
  dt <- dt[!grepl("case fatality", note_modeler)]
  return(dt)
}

adjustment <- function(data){
  dt <- copy(data)
  dt[, var_mean := standard_error^2]
  dt[, var_sr := sr_pooled_se^2]
  dt[, var := var_mean * var_sr + var_sr * mean^2 + var_mean * sr_pooled^2]
  dt[, standard_error := sqrt(var)]
  dt[, mean := mean * sr_pooled]
  dt[, c("var_mean", "var_sr", "var") := NULL]
  dt[, uncertainty_type := "Standard error"]
  dt[, note_modeler := paste0(note_modeler, " | SR adjustment by ", round(sr_pooled,3))]
  return(dt)
}

upload_data <- function(data, hospital = F, marketscan = F){
  dt <- copy(data)
  if (hospital == T){
    write.xlsx(dt, paste0(j_root, "FILEPATH", bundle, "FILEPATH", date, ".xlsx"), 
               sheetName = "extraction")
    upload_epi_data(bundle_id = bundle, filepath = paste0(j_root, "FILEPATH", bundle, 
                                                          "FILEPATH", date, ".xlsx"))
  } else if (marketscan == T){
    write.xlsx(dt, paste0(j_root, "FILEPATH", bundle, "FILEPATH", date, ".xlsx"), 
               sheetName = "extraction")
    upload_epi_data(bundle_id = bundle, filepath = paste0(j_root, "FILEPATH", bundle, 
                                                          "FILEPATH", date, ".xlsx"))
  }
}


## GET CFR RATE 
cfr <- get_epi_data(bundle_id = 1379)
cfr[, survived := cases - deaths]
cfr[, `:=` (survived = round(survived, 0),
            cases = round(cases, 0))]
meta <- metaprop(data = cfr, event = survived, n = cases,
                 studylab = location_name, comb.random = T, level = .95)
random <- summary(meta)$random
sr_pooled <- inv.logit(random$TE)
sr_pooled_se <- sr_pooled * (1-sr_pooled) * random$seTE 

## SAVE RESULT
pdf(paste0(j_root, "FILEPATH", date, ".pdf"), width = 12)
forest(meta, studlab = paste0(cfr$location_name, ": ", cfr$year_start), weight.study = "random", comb.fixed = F)
dev.off()

## GET DATA AND CORRECT - HOSPITAL
hospital <- get_gbs_data(hospital = T)
hospital <- adjustment(hospital)
upload_data(hospital, hospital = T)

## GET DATA AND CORRECT - MARKETSCAN
marketscan <- get_gbs_data(marketscan = T)
marketscan <- adjustment(marketscan)
upload_data(marketscan, marketscan = T)