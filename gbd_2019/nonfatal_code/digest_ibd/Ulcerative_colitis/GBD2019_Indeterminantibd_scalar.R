###################################################################################################################
## This script generates redistribution scalar of indeterminate colitis 
## Input: meta-analysis of IBD with proproption due to IC
## Output:  values to input into GBD2019_cause_indibd_scalar_apply.R
###################################################################################################################

##load libraries
library(data.table)
library(meta)

##generate scalar 
dt <- fread(file.path("FILEPATH TO EXTRACTED DATA"))
dt <- dt[, se_ic_prop:=ic_proportion*(1-ic_proportion)/(sqrt(total_ibd_cases))]
ic_meta <- metagen(dt$ic_proportion, dt$se_ic_prop, comb.random=gs("comb.random"))

##record these and transfer values to GBD2019_cause_indibd_scalar_apply.R
print(ic_meta$TE.random)
print(ic_meta$lower.random)
print(ic_meta$upper.random)

