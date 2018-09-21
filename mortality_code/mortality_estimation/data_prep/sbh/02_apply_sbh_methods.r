#####################################################################################
## Description: Apply summary birth history methods to prepped dataset
#####################################################################################

## Load functions
  rm(list=ls())
  library(foreign); library(lme4.0); library(arm)
  setwd("strFunctionDirectory")
  source("apply_mac.r"); source("apply_tfbc.r"); source("apply_map.r"); source("apply_tfbp.r")
  source("gather_data.r"); source("output_to_gbd_envelopes.r")

## Apply methods
  dir <- "strSurveyDirectory"		## CHANGE TO YOUR DIRECTORY
  grouping.categories = c("iso3", "svdate")	
  all.women <- TRUE		## SET TO TRUE IF YOU HAVE AN ALL-WOMEN SURVEY AND FALSE IF YOU HAVE AN EVER-MARRIED SURVEY 
  subset <- NULL
  uncertainty <- FALSE
  n.sims <- 1
  filetag <- ""

  ## APPLY THE METHODS YOU ARE ABLE TO (i.e. REMOVE ANY METHODS YOU KNOW YOU CAN'T APPLY)
  apply_mac(dir, grouping.categories, all.women, subset, filetag, uncertainty, n.sims)
  apply_map(dir, grouping.categories, all.women, subset, filetag, uncertainty, n.sims)
  apply_tfbc(dir, grouping.categories, all.women, subset, filetag, uncertainty, n.sims)
  apply_tfbp(dir, grouping.categories, all.women, subset, filetag, uncertainty, n.sims)
  
## Gather data
  gather_data(dir, grouping.categories, n.sims, filetag)
  
## Save the data
  survey.file.name <- "survey"					## SET WHAT YOU WANT THE SURVEY LABELLED IN THE FILE
  survey.database.name <- "survey"			## SET WHAT YOU WANT THE FILE NAMED

  output_to_gbd_envelopes(dir, survey.file.name, survey.database.name)
