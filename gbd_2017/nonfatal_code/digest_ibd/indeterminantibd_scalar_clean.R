###################################################################################################################
## Author: 
## Based on: ic_scalar.R 
## Description: Generate redistribution scalar of indeterminate colitis 
## Input: meta-analysis of IBD with proproption due to IC
## Output:  values to input into indibd_scalar_apply.R
## Notes: this file must be run locally, no meta package on cluster, original stata script 
###################################################################################################################

rm(list=ls())

##working environment 

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j<- "J:/"
  h<-"H:/"
} else {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
  shell <- "FILEPATH"
  scratch <- "FILEPATH"
}

##load libraries

library(data.table)
library(meta)

##generate scalar 
dt <- fread(file.path(j, "FILEPATH"))
dt <- dt[, se_ic_prop:=ic_proportion*(1-ic_proportion)/(sqrt(total_ibd_cases))]
ic_meta <- metagen(dt$ic_proportion, dt$se_ic_prop, comb.random=gs("comb.random"))

##record these and transfer values to indibd_scalar_apply.R

print(ic_meta$TE.random)
print(ic_meta$lower.random)
print(ic_meta$upper.random)
