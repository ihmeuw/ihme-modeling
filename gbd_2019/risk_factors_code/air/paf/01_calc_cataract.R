#----HEADER-------------------------------------------------------------------------------------------------------------
# Author: NAME
# Date: 5/7/18
# Project: RF: air_pm
# Purpose: Calculate PAFs from air PM for a given country year for cataracts
# source("FILEPATH.R", echo=T)
#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  
  arg <- commandArgs()[-(1:5)]  # First args are for unix use only
  
  if (length(arg)==0) {
    #toggle for targeted run on cluster
    arg <- c(46, #output version
             1000, #draws required
             1990, #year
             102800)  #hap st-gpr run id
  }
  
} else {
  
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  
  arg <- c(35, #output version
           1000, #draws required
           1990, #year
           102)  #hap st-gpr run id
}

pacman::p_load(data.table, fst, magrittr, ggplot2, gdata, ini)

#----Directories and shared functions----------------------------------------------------------------------------

# set working directories
home.dir <- file.path("FILEPATH")
setwd(home.dir)

# set project values
location_set_id <- 22

# Set parameters from input args
output.version <- arg[1]
draws.required <- as.numeric(arg[2])
year <- as.numeric(arg[3])
run_id <- arg[4]
silly.cols <- paste0("draw_", 0:(draws.required-1))
paf.cols <- paste0("draw_", 0:(draws.required-1)) #must be saved in 0-999 format
rr.cols <- paste0("rr_", 0:(draws.required-1))

#create age sex groups
ages <- c(9:20,30:32,235) #20+
sexes <- c(1,2)    #females & males, change for GBD2017
cause <- 671  #cataract
cause_name <- "sense_cataract"
measure <- 3   #yld

# outdir
hap.stgpr.dir <- paste0("FILEPATH",run_id,"FILEPATH")

# REIs
reis <- c("air_pmhap","air_pm","air_hap")

# PAFs
for(rei in reis){
  dir.create(file.path(home.dir,rei,"FILEPATH",output.version), recursive = T, showWarnings = F)
  dir.create(file.path(home.dir,rei,"FILEPATH",output.version), recursive = T, showWarnings = F)
}

dir.create(file.path(home.dir,"FILEPATH",output.version),recursive=T,showWarnings = F)

# PAF functions
paf.function.dir <- file.path(h_root, 'FILEPATH')
file.path(paf.function.dir, "FILEPATH.R") %>% source

# general functions
central.function.dir <- file.path(h_root, "FILEPATH")
# this pulls the general misc helper functions
file.path(central.function.dir, "FILEPATH.R") %>% source

# shared functions
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))

#----Prep data----------------------------------------------------------------------------------------------------------
locs <- get_location_metadata(location_set_id)[most_detailed==1,location_id]

hap.exp <- lapply(paste0(hap.stgpr.dir,locs,".csv"),fread) %>% rbindlist(use.names=T)
hap.exp <- hap.exp[year_id==year]

hap.prop.cols <- paste0("hap_prop_", 1:draws.required)
setnames(hap.exp, silly.cols, hap.prop.cols)

hap.exp <- hap.exp[,c("location_id","year_id",hap.prop.cols),with=F]

#----Generate draws/predictions ----------------------------------------------------------------------------------------

# # generate draws (or load if already saved)
# set.seed(5)
# log_rr <- log(rr)
# log_rr_sd <- (log(rr_upper)-log(rr_lower))/(2*1.96)
# rr.draws <- exp(rnorm(draws.required,mean=log_rr,sd=log_rr_sd)) #assumes log_normal distribution
rr.draws <- fread("FILEPATH.csv") %>% unlist()
                         
# generate PAF predictions based on draws of proportion exposed and RR
hap.exp[, (paf.cols) := lapply(1:draws.required,
                         function(draw) {
                           ((get(hap.prop.cols[draw])*(rr.draws[draw]-1))/(get(hap.prop.cols[draw])*(rr.draws[draw]-1)+1))
                         })]

hap.exp[,(hap.prop.cols):=NULL]

hap.exp[, index := seq_len(.N)] # add index
hap.exp[, draw_lower := quantile(.SD, c(.025)), .SDcols=paf.cols, by="index"]
hap.exp[, draw_mean := rowMeans(.SD), .SDcols=paf.cols, by="index"]
hap.exp[, draw_upper := quantile(.SD, c(.975)), .SDcols=paf.cols, by="index"]

# save summary version of PAF
summary <- hap.exp[, c("location_id",
                       "year_id",
                       "draw_lower",
                       "draw_mean",
                       "draw_upper"),
                   with=F]
write.csv(summary, paste0(home.dir,"FILEPATH",output.version,"FILEPATH",year,".csv"), row.names=F)

hap.exp[,index:=NULL]
hap.exp[,merge:=1]

# set risks to be air_hap and air because air_hap does not get aggregated up
groups <- CJ(risk = c("air_pmhap","air_hap"),
             measure_id = measure,
             age_group_id = ages,
             sex_id = sexes,
             acause = cause_name,
             cause_id=cause,
             merge=1)

out.paf <- merge(hap.exp,groups,by="merge",allow.cartesian=T)

out.paf <- out.paf[, c("risk",
                       "measure_id",
                       "age_group_id",
                       "sex_id",
                       "location_id",
                       "year_id",
                       "acause",
                       "cause_id",
                       "draw_mean",
                       paf.cols),
                   with=F]

#----Save PAFs ----------------------------------------------------------------------------------------------------------

saveSex <- function(this.sex, this.cause, this.measure, this.risk, this.location, this.year, file){
  
  out <- copy(file)
  
  out <- out[sex_id==this.sex 
             & cause_id==this.cause 
             & measure_id==this.measure
             & risk==this.risk 
             & location_id==this.location
             & year_id== this.year] #subset to the relevant rows
  out <- out[,risk:=NULL]
  
  write.csv(out, paste0(home.dir,"/",this.risk,"FILEPATH",output.version,"/", 
                        this.measure, "_", this.cause, "_", this.location, "_",
                        this.year, "_", this.sex, ".csv"),
            row.names=F)
}

# save a file for each sex, cause, yll/yld, air/air_pm/air_hap, location, year
save <- unique(out.paf[,.(sex_id,cause_id,measure_id,risk,location_id,year_id)])
save[,saveSex(sex_id,cause_id,measure_id,risk,location_id,year_id,file=out.paf),by=1:nrow(save)]