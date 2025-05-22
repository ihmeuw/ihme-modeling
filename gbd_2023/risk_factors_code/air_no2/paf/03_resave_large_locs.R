
#-------------------Header------------------------------------------------
# Author: 
# Purpose: Resave large locs - population weight to aggregate

#------------------Set-up--------------------------------------------------

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

# get arguments
args <- commandArgs(trailingOnly=T)

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","fst","parallel","raster","pbapply","tictoc")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

# set working directories
home.dir <- file.path("FILEPATH")
setwd(home.dir)

# set project values
location_set_id <- 35

source(file.path(central_lib,"FILEPATH/get_location_metadata.R"))
locs <- get_location_metadata(35)

# Set parameters from input args
this.loc <- args[1]
this.year <- args[2]
grid.version <- args[3]
output.version <- args[4]
draws.required <- args[5]
draws.required <- as.numeric(draws.required)

cores.provided = 20

# Directories & functions -------------------------------------------------------------

paf.sum.out <- file.path("FILEPATH",output.version)
exp.sum.out <- file.path("FILEPATH",output.version)
rr.sum.out <- file.path("FILEPATH",output.version)
paf.draws.out <- file.path("FILEPATH",output.version)
status.dir <- file.path("FILEPATH",output.version)
tracker.dir <- paste0("FILEPATH")

draw.cols <- paste0("draw_",0:(draws.required-1))

# Read in data ----------------------------------------------------------------

# isolate needed files
files <- list.files(paf.draws.out)
labels <- lapply(files, function(x) {unlist(strsplit(x,"_"))[1]}) %>% unlist
files <- files[labels==this.loc]
files <- files[grepl(this.year,files)]

# double check that we have the correct number of chunks
# if we don't, print warning and stop the job
tracker <- fread(tracker.dir)
if(tracker[location_id==this.loc & year_id==this.year,nfiles] != length(files)) {
  warning(paste0(this.loc,", ",this.year))
  warning(paste0("Have ", length(files), " input files"))
  warning(paste0("Need ", tracker[location_id==this.loc & year_id==this.year,nfiles], " input files"))
  warning("We do not have the correct number of input PAF files!")
  quit(save="no", status=1)
}

# read in the data and rbind
exp <- lapply(paste0(paf.draws.out,"/",files),fread) %>% rbindlist

# make sure we have the correct age group 
# TODO after this run remove this line
exp[,age:=158]

# Population weight to aggregate ----------------------------------------------

exp <- exp[,lapply(.SD,weighted.mean,w=population),.SDcols=c("rr","no2","paf"), by=c("draw","year_id","cause","acause","age","risk")]
exp[,location_id:=this.loc]


# Summarize -------------------------------------------------------------------

# generate summary files
lower <- function(x){quantile(x,p=.025)}
upper <- function(x){quantile(x,p=.975)}

summary <- melt(exp,id.vars=c("draw","location_id","year_id","cause","acause","age","risk"))
summary <- summary[,.(mean=mean(value),lower=lower(value),upper=upper(value)),by=c("location_id","year_id","cause","acause","age","risk","variable")]

# summary file of exposures
write.csv(summary[variable=="no2"],
          paste0(exp.sum.out,"/",this.loc,"_",this.year,".csv"),row.names=F)
# summary file of RR
write.csv(summary[variable=="rr"],
          paste0(rr.sum.out,"/",this.loc,"_",this.year,".csv"),row.names=F)
# summary file of PAF
write.csv(summary[variable=="paf"],
          paste0(paf.sum.out,"/",this.loc,"_",this.year,".csv"),row.names=F)


# Save Draws --------------------------------------------------------------
exp <- exp[,.(location_id,year_id,cause,acause,age,risk,draw,paf)]  

# reshape wide by draw
exp[,draw:=paste0("draw_",draw)]
exp <- dcast.data.table(exp, location_id + year_id + cause + acause + age + risk ~ draw, value.var="paf")

setnames(exp,paste0("draw_",1:draws.required),draw.cols)  

# replace cause with cause_id
setnames(exp,"cause","cause_id")
exp[, measure_id := 3] # ylds

# keep only the variables we need
keep_cols <- c("measure_id","age","location_id","year_id","acause","cause_id", draw.cols)
exp <- exp[,..keep_cols]


# merge on age_ids (0 to 19 only, because it's childhood asthma)
age_ids <- c(2:3,6:8, 388, 389, 238, 34) # adding in those last 4 for the new <5 age groups
age_ids <- data.table(age_group_id=age_ids, merge=1)

exp[,merge:=1]

exp <- merge(exp,age_ids,by="merge",allow.cartesian=T)

# create a copy for males
exp[,sex_id:=2]
exp_male <- copy(exp)
exp_male[,sex_id:=1]
exp <- rbind(exp,exp_male,use.names=T)

exp <- exp[, c("measure_id",
               "age_group_id",
               "sex_id",
               "location_id",
               "year_id",
               "acause",
               "cause_id",
               draw.cols),
           with=F]

write.csv(exp, paste0(paf.draws.out,"/",this.loc,"_",this.year,".csv"),row.names=F)

# write empty csv to show the job finished.
write.csv(data.table(),paste0(status.dir,"/",this.loc,"_",this.year,".csv"),row.names=F)

