
#-------------------Header------------------------------------------------
# Author: 
# Purpose: 
#
#          
#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
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

desc_rr <- "GBD2023 MR-BRT model -- reran GBD 2021 code"
decomp <- "iterative"

# Directories -------------------------------------------------------------

source(file.path(central_lib,"current/r/save_results_risk.R"))
source(file.path(central_lib,"current/r/get_draws.R"))
source(file.path(central_lib,"current/r/get_age_metadata.R"))

model.version <- 22
cause <- 515 # asthma
# ages <- c(2:3,6:7, 63:65, 388, 389, 238, 34) # new age groups for GBD2020
ages <- c(2:3,6:8,388, 389, 238, 34) # CC functions only support GBD most-detailed ages for GBD 2020
sexes <- c(1,2)
years <- c(1990:2023) # c(1990:2022)
location <- 1
morb <- 1
mort <- 0
param <- "per unit"



results_dir <- file.path("FILEPATH")
save_dir <- file.path("FILEPATH")
dir.create(save_dir,recursive = T)

dt <- fread(file.path(results_dir,"draws.csv"))
dt <- dt[11,] # arbitrary row (close to 1)
exp <- dt$exposure
dt[,exposure:=NULL]
dt <- exp(dt) # exponentiate because these are in log space!
setnames(dt, paste0("draw_",1:1000), paste0("draw_",0:999))
dt[,merge:=1]

out <- expand.grid(cause_id=cause, age_group_id=ages, year_id=years, location_id=location, sex_id=sexes, mortality=mort, morbidity=morb, parameter=param, merge=1)

out <- merge(out,dt,"merge") %>% as.data.table

out[,merge:=NULL]
# modeled per 10 units. Convert to per 1
out[,paste0("draw_",0:999):=lapply(.SD,function(x){x^(1/exp)}),.SDcols=paste0("draw_",0:999)]

write.csv(out,file.path(save_dir,"all_draws.csv"),row.names=F)

save_results_risk(input_dir = save_dir,
                  input_file_pattern = "all_draws.csv",
                  modelable_entity_id = 26281,
                  description = desc_rr,
                  risk_type = "rr",
                  year_id=1990:2023, # 1990:2022
                  # decomp_step=decomp,
                  release_id=16,
                  mark_best = T)

