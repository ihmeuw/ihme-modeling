#####################################################################################
## Description: Apply summary birth history methods to prepped dataset and output the data for child GPR 
#####################################################################################


## Load functions
rm(list=ls())
library(foreign); require(lme4.0); require(arm)

codedir <<- "strFunctionDirectory"
setwd(codedir)
source("apply_mac.r"); source("apply_tfbc.r"); source("apply_map.r"); source("apply_tfbp.r")
source("gather_data.r"); source("output_to_gbd_envelopes.r")

dir <- "FILEPATH"
## Load dataset that distinguishes ever-married and all women surveys

sample <- read.dta("FILEPATH/survey_women_sample_types.dta", sep="")

## APPLY THE METHODS YOU ARE ABLE TO (i.e. REMOVE ANY METHODS YOU KNOW YOU CAN'T APPLY)
apply_mac(dir, grouping.categories, all.women, subset, filetag, uncertainty, n.sims)
apply_map(dir, grouping.categories, all.women, subset, filetag, uncertainty, n.sims)
apply_tfbc(dir, grouping.categories, all.women, subset, filetag, uncertainty, n.sims)
apply_tfbp(dir, grouping.categories, all.women, subset, filetag, uncertainty, n.sims)


if(unique(sample$sample)[1] == "ever-married"){ 
  ## Apply methods to ever married samples
  grouping.categories = c("iso3", "svdate")
  all.women <- FALSE
  subset <- sample[sample$sample == "ever-married", grouping.categories]
  uncertainty <- FALSE
  n.sims <- 1
  filetag <- "_ever_married"
  apply_methods(dir, grouping.categories, all.women, subset, filetag, uncertainty, n.sims)
  
} else if(unique(sample$sample)[1] == "all_women"){ 
  ## Apply methods to all women samples
  grouping.categories = c("iso3", "svdate")
  all.women <- TRUE
  subset <- sample[sample$sample == "all", grouping.categories]
  uncertainty <- FALSE
  n.sims <- 1
  filetag <- "_all_women"
  apply_methods(dir, grouping.categories, all.women, subset, filetag, uncertainty, n.sims)
  
} else if (unique(sample$sample)[1] == "all"){ #both
  ## Apply methods to ever married samples
  grouping.categories = c("iso3", "svdate")
  all.women <- FALSE
  subset <- sample[sample$sample == "all", grouping.categories]
  uncertainty <- FALSE
  n.sims <- 1
  filetag <- "_all"
  apply_methods(dir, grouping.categories, all.women, subset, filetag, uncertainty, n.sims)
  
} else if (length(unique(sample$sample)) > 1){ #both
  ## Apply methods to ever married samples
  grouping.categories = c("iso3", "svdate")
  all.women <- FALSE
  subset <- sample[sample$sample == "ever-married", grouping.categories]
  uncertainty <- FALSE
  n.sims <- 1
  filetag <- "_ever_married"
  apply_methods(dir, grouping.categories, all.women, subset, filetag, uncertainty, n.sims)
  
  ## Apply methods to all women samples
  grouping.categories = c("iso3", "svdate")
  all.women <- TRUE
  subset <- sample[sample$sample == "all", grouping.categories]
  uncertainty <- FALSE
  n.sims <- 1
  filetag <- "_all_women"
  apply_methods(dir, grouping.categories, all.women, subset, filetag, uncertainty, n.sims)
  
  ## Gather data
  filetag <- c("_ever_married", "_all_women")
}

gather_data(dir, grouping.categories, n.sims, filetag)

## Output to GBD Envelopes
survey.file.name <- "strSurveyName"	
survey.database.name <- "strSurveyName"	
output_to_gbd_envelopes(dir, survey.file.name, survey.database.name, nid)