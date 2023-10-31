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

# load packages, install if missing
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
exp.grid.version <- args[3]
rr.version <- args[4]
output.version <- args[5]
draws.required <- args[6]
draws.required <- as.numeric(draws.required)

cores.provided = 20

print(paste0(this.loc,", ", this.year))

# Generate tmrel distribution
set.seed(143)
tmrel.draws <- runif(draws.required,4.545,6.190)

# Directories & functions -------------------------------------------------------------

no2.grid.in <- file.path(home.dir,"FILEPATH",exp.grid.version,"FILEPATH")
mrbrt.in <- file.path(home.dir,"FILEPATH",rr.version)
paf.sum.out <- file.path(home.dir,"FILEPATH",output.version)
exp.sum.out <- file.path(home.dir,"FILEPATH",output.version)
rr.sum.out <- file.path(home.dir,"FILEPATH",output.version)
paf.draws.out <- file.path(home.dir,"FILEPATH",output.version)
status.dir <- file.path(home.dir,"FILEPATH",output.version)

dir.create(paf.sum.out, recursive = T, showWarnings = F)
dir.create(exp.sum.out, recursive = T, showWarnings = F)
dir.create(rr.sum.out, recursive = T, showWarnings = F)
dir.create(paf.draws.out, recursive = T, showWarnings = F)
dir.create(status.dir, recursive = T, showWarnings = F)

draw.cols <- paste0("draw_",0:(draws.required-1))

# Read in data ----------------------------------------------------------------

# read in no2 grid (copy 2019 to 2020-2022 for now, until I extrapolate to 2020)
if(this.year %in% c(2021,2022)){
  no2 <- read.csv(paste0(no2.grid.in,"/",this.loc,"_2019.csv")) %>% as.data.table
} else {
  no2 <- read.csv(paste0(no2.grid.in,"/",this.loc,"_",this.year,".csv")) %>% as.data.table
}
no2 <- no2[,c("latitude","longitude","population","air_no2"),with=F] 

# drop zero population cells to try to speed up computation time
no2 <- no2[population>0]

# make a note of the summed population for the gridcells for chunked locations
# this is so we can population-weight the chunks at the end
if(grepl("_",this.loc)){
  pop_out <- sum(no2$population)
}

# generate 1000 draws of exposure using MAE estimates
if(grepl("_",this.loc)){
  l <- unlist(strsplit(this.loc, "_"))[1] # need to do this because some loc ids have chunk labels
  loc <- locs[location_id==l]
} else {
  loc <- locs[location_id==this.loc]
}

uncertainty <- fread("FILEPATH/uncertainty.csv")
uncertainty <- uncertainty[location_id==loc$region_id,mae]

# to preserve uncertainty estimates, we need to make sure that all gridcells are correlated for their deviation draws
# note: right now we are assuming total correlation (being very conservative because we have no way of knowing how correlation varies in spacetime)
set.seed(153) # set seed so we have the same distribution for all locations
zscore_draws <- rnorm(n=draws.required,mean=0,sd=1)

exp_draws <- matrix(data = sapply(zscore_draws,function(x){x*uncertainty + no2$air_no2}),
                    nrow = nrow(no2),
                    ncol = draws.required) %>% as.data.table

exp_draws <- setNames(exp_draws,draw.cols)

exp <- cbind(no2[,.(latitude,longitude,population)],exp_draws)

rm(no2) # remove no2 to try to save memory


# melt for easier manipulation
exp <- melt(exp,measure.vars=draw.cols,value.name="no2",variable.name="draw",value.factor=T)
exp[,draw:=as.numeric(draw)]

# add in tmrel
exp <- exp[,tmrel:=tmrel.draws[draw]]

# read in RR data
mrbrt <- fread(file.path(mrbrt.in,"draws.csv"))
mrbrt[,exposure:=NULL]
setnames(mrbrt,paste0("draw_",1:1000),paste0("draw_",0:999))
mrbrt <- exp(mrbrt) # don't forget to do this because it was modeled in log space
mrbrt <- mrbrt[,..draw.cols]
mrbrt[,exposure_spline:=seq(0,100,length.out = 1000)]


# make long by draw
mrbrt <- melt.data.table(mrbrt,measure.vars=draw.cols, variable.name="draw", variable.factor=T, value.name="mrbrt")
mrbrt[,draw:=as.numeric(draw)]

# Calculate RR
exp_cats <- c("no2","tmrel")

for(exp_cat in exp_cats){
  
  # merge on mrbrt predictions based on closest available exposure datapoint predicted for mr_brt & draw
  # make a join column because exp loses its exposure column (& we want to keep no2 and tmrel)
  mrbrt[,join_exp:=exposure_spline]
  exp[,join_exp:=get(exp_cat)]
  
  # set the key variables on which to join (the roll is performed on the last one)
  setkeyv(mrbrt,c("draw","join_exp"))
  setkeyv(exp,c("draw","join_exp"))
  
  # rolling join by nearest exposure
  exp <- mrbrt[exp, roll = "nearest"]

  # change name
  setnames(exp,"mrbrt",paste0("rr_",exp_cat))
  
  # remove the unnecessary columns
  exp[,exposure_spline:=NULL]
  exp[,join_exp:=NULL]
  
}
setcolorder(exp,c("draw","latitude","longitude","population","no2","tmrel","rr_no2","rr_tmrel"))


rm(mrbrt)

# Calculate RR & PAF-----------------------------------------------------------

# get RR for each row
# we make the RR for each NO2 exposure draw relative to its TMREL draw
exp[,rr:=ifelse(no2>tmrel,
                (rr_no2/rr_tmrel), 
                1)]

# population weight no2 exposure & RR
exp <- exp[,lapply(.SD,weighted.mean,w=population),.SDcols=c("rr","no2"), by=c("draw")]

# calculate PAF for each row
exp[,paf:=(rr-1)/(rr)]

# add in identifiers
exp[,location_id:=this.loc]
exp[,year_id:=this.year]
exp[,cause:=515] # asthma
exp[,acause:="resp_asthma"]
exp[,age:=158] # <20 (because it's childhood asthma)
exp[,risk:="air_no2"]

# here, we don't want to save the chunked locations in the final format for save_results_epi
# we still have to pop-weight the chunks to aggregate
if(grepl("_",this.loc)){
  
  exp[,population:=pop_out]

  write.csv(exp, paste0(paf.draws.out,"/",this.loc,"_",this.year,".csv"),row.names=F)
  
  # write empty csv to show the job finished
  write.csv(data.table(),paste0(status.dir,"/",this.loc,"_",this.year,".csv"),row.names=F)
  
  quit(save="no",status=0) # we end the job early if it is a chunked location
  
}

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
age_ids <- c(2:3,6:8, 388, 389, 238, 34)
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
  

## end:) ##

