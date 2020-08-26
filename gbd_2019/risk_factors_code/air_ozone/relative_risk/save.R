
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 9/4/2019
# Purpose: Save new MR-BeRT RR for ozone
#          
# source("FILEPATH.R", echo=T)
#***************************************************************************

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
} else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
}


# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

desc_rr <- "MR-BeRT model, no bias covariates or priors"
decomp <- "step4"

# Directories -------------------------------------------------------------

source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))

cause <- 509
ages <- c(10:20,30:32,235)
sexes <- c(1,2)
years <- c(1990:2019)
location <- 1
morb <- 0
mort <- 1
param <- "per unit"


results_dir <- file.path("FILEPATH")
save_dir <- file.path("FILEPATH")
dir.create(save_dir)

dt <- fread(file.path(results_dir,"FILEPATH.csv"))
dt[,merge:=1]

out <- expand.grid(cause_id=cause, age_group_id=ages, year_id=years, location_id=location, sex_id=sexes, mortality=mort, morbidity=morb, parameter=param, merge=1)

out <- merge(out,dt,"merge") %>% as.data.table

out[,merge:=NULL]
# modeled per 10 units. Convert to per 1
out[,paste0("draw_",0:999):=lapply(.SD,function(x){x^(1/10)}),.SDcols=paste0("draw_",0:999)]

write.csv(out,file.path(save_dir,"FILEPATH.csv"),row.names=F)

save_results_risk(input_dir = save_dir,
                  input_file_pattern = "FILEPATH.csv",
                  modelable_entity_id = 9021,
                  description = desc_rr,
                  risk_type = "rr",
                  year_id=1990:2019,
                  decomp_step=decomp,
                  mark_best = T)