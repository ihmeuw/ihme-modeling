
#-------------------Header------------------------------------------------
# Author: Sarah Wozniak (03/16/21)
# Purpose: RR max calculation for ozone
#          

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

packages <- c("data.table","magrittr","fst","Hmisc","stringr","pbapply")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}


years <- c(1990,1995,2000,2005,2010,2015,2019,2020)
version <- 22 # best PAF version

source(paste0(central_lib,"current/r/get_population.R"))
source(paste0(central_lib,"current/r/get_age_metadata.R"))
source(paste0(central_lib,"/current/r/get_location_metadata.R"))
 
locs <- get_location_metadata(35,gbd_round_id = 7)

pops <- get_population(age_group_id=157, # 25 plus
                       location_id=locs[most_detailed==1,location_id],
                       year_id=years,
                       sex_id=3,
                       gbd_round_id=7,
                       decomp_step="iterative")

# Directories -------------------------------------------------------------

exp_dir <- file.path(j_root, "WORK/05_risk/risks/air_ozone/products/exp", version)

files <- list.files(paste0(exp_dir,"/summary"),full.names=T)

exposure <- pblapply(files,fread,cl=4) %>% rbindlist()
exposure <- exposure[year_id%in%years]

# get means of exposure by aggregating by population
exposure <- merge(exposure,pops,by=c("location_id","year_id")) # there is only one age group

# note that on 04/25/22 we changed the RR max to the 95th percentile to align with GBD decision
max <- wtd.quantile(exposure$exposure_mean,0.95,weights = exposure$population)

# get RR value for this max
rr <- fread(paste0("FILEPATH/10ppb_draws.csv"))
rr[,exposure:=NULL]
setnames(rr,paste0("draw_",1:1000),paste0("draw_",0:999))
rr <- exp(rr)
rr.draws <- as.vector(unlist(rr[1]))

rr.draws <- rr.draws^(max/10)


# add in identifying information
out <- data.table(cause_id = 509,
                  rei = 88,
                  age_group_id = rep(c(10:20, 30:32, 235),each=2),
                  sex_id = c(1,2),
                  merge = 1)
rr <- data.table(rr = rr.draws,
                 draw = seq(1,1000),
                 merge = 1)

out <- merge(out,rr,by="merge",allow.cartesian=T)
out[,draw:=as.integer(draw)] 
setorder(out,draw)

# format and save
out[,merge:=NULL]
setcolorder(out,neworder = c("cause_id","rei","draw","rr","age_group_id","sex_id"))

mean(out$rr) # check the RRmax
quantile(out$rr,0.025)
quantile(out$rr,0.975)

write.csv(out,paste0("FILEPATH","air_ozone","_gbd20_95th.csv"),row.names=F)

# # compare to previous (this is for next year)
# temp <- fread("")
# mean(temp$rr)
# quantile(temp$rr,0.025)
# quantile(temp$rr,0.975)
