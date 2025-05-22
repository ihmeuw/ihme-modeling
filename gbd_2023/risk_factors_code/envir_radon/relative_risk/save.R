
#-------------------Header------------------------------------------------
# Purpose: Save new MR-BRT RR for radon

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/"
  h_root <- "~/"
  central_lib <- "/ihme/cc_resources/libraries/"
  } else {
  j_root <- "J:/"
  h_root <- "H:/"
  central_lib <- "K:/libraries/"
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

desc_rr <- "GBD2020 new MRBRT model: adding Fisher's Information boost"
decomp <- "iterative"

model.version <- 6
model.version <- 8

# Directories -------------------------------------------------------------

source(file.path(central_lib,"current/r/save_results_risk.R"))
source(file.path(central_lib,"current/r/get_draws.R"))

cause <- 426
ages <- c(2:3,388,389,238, 34, 6:20,30,31,32,235)
sexes <- c(1,2)
years <- c(1990:2022)
location <- 1
morb <- 1
mort <- 1
param <- "per unit"

draws_dir <- file.path("FILEPATH/",model.version)
save_dir <- file.path("FILEPATH/",model.version)
dir.create(save_dir,recursive = T)

dt <- fread(file.path(draws_dir,"100ppb_draws.csv"))
dt[,exposure:=NULL] # it is for 100 ppb
dt <- exp(dt) # exponentiate because these are in log space!
setnames(dt, paste0("draw_",1:1000), paste0("draw_",0:999))
dt[,merge:=1]

out <- expand.grid(cause_id=cause, age_group_id=ages, year_id=years, location_id=location, sex_id=sexes, mortality=mort, morbidity=morb, parameter=param, merge=1)

out <- merge(out,dt,"merge") %>% as.data.table

out[,merge:=NULL]
out[,parameter:="per unit"] # now we need to add this because the CC RR upload code changed

# modeled as per 100 units. Convert to per 1
out[,paste0("draw_",0:999):=lapply(.SD,function(x){x^(1/100)}),.SDcols=paste0("draw_",0:999)]

write.csv(out,file.path(save_dir,"all_draws.csv"),row.names=F)

save_results_risk(input_dir = save_dir,
                  input_file_pattern = "all_draws.csv",
                  modelable_entity_id = 9022,
                  description = desc_rr,
                  risk_type = "rr",
                  measure_id=c(3,4),
                  year_id=years,
                  sex_id=sexes,
                  n_draws=1000,
                  decomp_step=decomp,
                  gbd_round_id=7,
                  mark_best = T)
