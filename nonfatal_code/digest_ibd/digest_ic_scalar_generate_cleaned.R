###################################################################################################################
## Author: USERNAME
## Description: Generate redistribution scalar of indeterminate colitis 
## Output:  values to input into ic_scalar_apply.R
## Notes: this file must be run locally, no meta package on cluster, 
###################################################################################################################

rm(list=ls())

##working environment 

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- FILEPATH
  j<- FILEPATH
  h<- FILEPATH
} else {
  lib_path <- FILEPATH
  j<- FILEPATH
  h<- FILEPATH
  shell <- FILEPATH
  scratch <- FILEPATH
}

##load libraries

library(data.table)
library(meta)

##generate scalar using proportion of indeterminate colitis and generated standard errors 

dt <- fread(FILEPATH))
dt <- dt[, se_ic_prop:= sqrt(ic_proportion*(1-ic_proportion)/total_ibd_cases)]
ic_meta <- metagen(dt$ic_proportion, dt$se_ic_prop, comb.random=gs("comb.random"))

##record these and transfer values to ic_scalar_apply.R

print(ic_meta$TE.random)
print(ic_meta$lower.random)
print(ic_meta$upper.random)
