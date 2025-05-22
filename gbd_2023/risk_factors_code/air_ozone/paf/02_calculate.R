#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Run calculations for ozone (PAF/EXP)


#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/"
  h_root <- "/homes/USERNAME/"
  arg <- tail(commandArgs(),6)  # First args are for unix use only
  if (length(arg)!=6) {
    #toggle for targeted run on cluster
    # arg <- c(570, "12", 20, 1000, 5)
  }
} else {
  j_root <- "J:"
  h_root <- "H:"
  arg <- c("AND", 1, 9, 1000, 4)
}


# Set parameters
country <- arg[1]
exp.grid.version <- arg[2]
output.version <- arg[3]
draws.required <- as.numeric(arg[4])
cores.provided <- as.numeric(arg[5])
model.version <- as.numeric(arg[6])
years <- c(1990:2022)

# load packages, install if missing
pacman::p_load(data.table, ggplot2, grid, parallel, magrittr, RColorBrewer, reshape2, ini,fst)

# load functions
file.path("FILEPATH/get_population.R") %>% source
file.path("FILEPATH/get_location_metadata.R") %>% source

"%ni%" <- Negate("%in%") # create a reverse %in% operator

# IN
# clean environment with RR and TMRED draws (done separately to preserve covariance across parallel)
# file.path(j_root, "WORK/05_risk/risks/air_ozone/data/",output.version,"clean.Rdata") %>% load(envir = globalenv())
in.grid.dir <- file.path("FILEPATH/gridded",exp.grid.version)

# OUT
out.paf.dir <- file.path(j_root, "FILEPATH", output.version)
out.exp.dir <- file.path(j_root, "FILEPATH", output.version)
out.tmp <- file.path("FILEPATH", output.version)
out.rr.dir <- file.path(j_root, "FILEPATH", output.version)

#----PREP------------------------------------------------------------------------------------------------------------------------
# Generate TMREL
# Update for GBD 2017: 0 and 5% of (Aug-Sept) ozone from updated ACS II study, NID 259393
tmred <- data.frame(tmred=runif(draws.required, 29.1, 35.7))

# Load RR
# GBD2019 MR-BRT model with no bias covariates or priors

rr <- fread(paste0("FILEPATH/",model.version,"/10ppb_draws.csv"))
rr[,exposure:=NULL]
setnames(rr,paste0("draw_",1:1000),paste0("draw_",0:999))
rr <- exp(rr)
rr.draws <- as.vector(unlist(rr[1]))

#----PREP---------------------------------------------------------------------------------------------------------------------

yearWrapper <- function(this.year) {
  
  message("Working on the year ", this.year)
  
  exp <- as.data.table(read.fst(paste0(in.grid.dir,"/loc_",country,"_",this.year,".fst")))
  
  names(exp)<- tolower(names(exp))
  
  # Prep gridded exposure dataset
  setkeyv(exp, c('longitude', 'latitude')) #make sure there are no duplicate grids
  exp <- unique(exp)
  exp <- exp[!is.na(exp$ozone) 
             & !is.na(exp$ozone_var)
             & !is.infinite(exp$ozone) 
             & !is.infinite(exp$pop), ] # Get rid of missings and infinites
  exp[,":="(pop=as.numeric(pop),ozone=as.numeric(ozone),ozone_var=as.numeric(ozone_var))]
  exp[exp$ozone <= 0, "ozone"] <- 0.1 # Set ozone values of 0 or smaller to be 0.1 (This will have a PAF of 0, so we don't want to drop.) #TODO combine these 2 steps?
  
  # For countries with 0 population, set all pop to be an arbitrary value of 0.1
  if(sum(exp$pop,na.rm = T)==0){
    exp[,pop:=0.1]
  }
  
  # otherwise, set any NA pop to zero
  exp[is.na(pop),pop:=0]
  
  #Calculate 3 year mean
  
  #extrapolated years just take mean and variance from extrapolation
  if(this.year %in% c(2018,2019,2020,2021,2022)){
    exp[,smooth_mean:=ozone]
    exp[,smooth_var:=ozone_var]
  }else if(this.year==1990){  #1990 and 2017 we can only take 2 year mean
    
    exp_future <-as.data.table(read.fst(paste0(in.grid.dir,"/loc_",country,"_",this.year+1,".fst")))
    
    exp <- merge(exp,exp_future[,.(id,ozone,ozone_var)],by="id",suffixes=c("","_future"))
    
    #Generate 2-year rolling mean
    exp[,smooth_mean:=mean(c(ozone,ozone_future)),by="id"]
    exp[,smooth_var:= 1/4 * (ozone_var+ozone_var_future+
                              2*sqrt(ozone_var*ozone_var_future))]
    
  }else if(this.year==2017){
    
    exp_prior <-as.data.table(read.fst(paste0(in.grid.dir,"/loc_",country,"_",this.year-1,".fst")))
    
    exp <- merge(exp,exp_prior[,.(id,ozone,ozone_var)],by="id",suffixes=c("","_prior"))
    
    #Generate 2-year rolling mean
    exp[,smooth_mean:=mean(c(ozone,ozone_prior)),by="id"]
    exp[,smooth_var:=1/9 * (ozone_var+ozone_var_prior+
                              2*sqrt(ozone_var*ozone_var_prior))]
    
  }else{

    exp_prior <-as.data.table(read.fst(paste0(in.grid.dir,"/loc_",country,"_",this.year-1,".fst")))
    exp_future <-as.data.table(read.fst(paste0(in.grid.dir,"/loc_",country,"_",this.year+1,".fst")))
    
    exp <- merge(exp,exp_prior[,.(id,ozone,ozone_var)],by="id",suffixes=c("","_prior"))
    exp <- merge(exp,exp_future[,.(id,ozone,ozone_var)],by="id",suffixes=c("","_future"))
    
    #Generate 3-year rolling mean
    exp[,smooth_mean:=mean(c(ozone,ozone_prior,ozone_future)),by="id"]
    exp[,smooth_var:=1/9 * (ozone_var+ozone_var_prior+ozone_var_future+
                       2*sqrt(ozone_var*ozone_var_prior)+
                       2*sqrt(ozone_var*ozone_var_future)+
                       2*sqrt(ozone_var_prior*ozone_var_future))]
   
  }
  
#generate SD
exp[, sd := sqrt(smooth_var)]

#generate draws of exposure using sd and mean
ozone.draw.colnames <- paste0("ozone_",1: draws.required)

#sample draws of the ozone exposure using your predefined SD

exp[, c(ozone.draw.colnames) := rnorm(draws.required, smooth_mean, sd) %>% as.list, by=list(rownames(exp))]

#----CALC PAFS-------------------------------------------------------------------------------------------------------------------
# generate RR using draws of ozone, RR, and TMRED with formula rr = base.RR ^ ((exp-tmred)/10) because rr is in terms of 10 ppb ozone
RR <- lapply(1:draws.required,
             function(draw.number)
               ifelse(exp[, ozone.draw.colnames[draw.number], with=FALSE] > tmred[draw.number,],
                      rr.draws[draw.number]^((exp[[ozone.draw.colnames[draw.number]]]-tmred[draw.number,])/10),
                      1)) # if exposure <= TMRED, there is no elevated risk

# new aggregation formula created by Mehrdad to address the issue that population at the grid level

out.paf <- lapply(1:draws.required,
                  function(draw.number)
                    (sum((RR[[draw.number]] - 1)*exp[,pop]) / sum(RR[[draw.number]]*exp[,pop]))) %>% as.data.table

#----FORMAT/SAVE-----------------------------------------------------------------------------------------------------------------
# Set up variables
# Currently we only estimate one cause/age group for ozone
out.paf[, acause := "resp_copd"]
out.paf[, cause_id := 509]
out.paf[, age_group_id := 99]
out.paf[, location_id := country]
out.paf[, year_id := this.year]
out.paf[, risk := "air_ozone"]

#pafs must be saved in this stupid way (0-999 instead of 1-1000)
paf.draw.colnames <- c(paste0("paf_", 0:(draws.required-1)))
setnames(out.paf, paste0("V", 1:draws.required), paf.draw.colnames)

# generate mean and CI for summary figures
out.paf[, paf_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=paf.draw.colnames]
out.paf[, paf_mean := rowMeans(.SD), .SDcols=paf.draw.colnames, by=list(cause_id,age_group_id)]
out.paf[, paf_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=paf.draw.colnames]


#Order columns to your liking
out.paf <- setcolorder(out.paf, c("location_id", "risk", "acause", "cause_id", "age_group_id", "year_id", "paf_lower", "paf_mean", "paf_upper", paf.draw.colnames))

# Save summary version of PAF output for experts
out.paf.summary <- out.paf[, c("age_group_id","acause","paf_lower","paf_mean","paf_upper", "location_id","year_id")  , with=F]
write.csv(out.paf.summary, file=paste0(out.paf.dir, "/summary/paf_yll_", country, "_", this.year,  ".csv"),row.names=F)

# Convert from age 99 to the correct ages
for (cause.code in c("resp_copd")) {
  # Take out this cause
  temp.paf <- out.paf[out.paf$acause == cause.code, ]
  out.paf <- out.paf[!out.paf$acause == cause.code, ]

  # Add back in with proper ages (need to use age.id instead of age number)
  if (cause.code %in% c("resp_copd")) ages <-  c(10:20, 30:32, 235) # resp_copd are 25 plus

  for (age.code in ages) {
    temp.paf$age_group_id <- age.code
    out.paf <- rbind(out.paf, temp.paf)
  }
}

#duplicate sex
out.paf[,merge:=1]
out.paf <- merge(out.paf,data.table(sex_id=c(1,2),merge=1), by="merge", allow.cartesian=T)
out.paf[,merge:=NULL]

# Save Mortality PAFs
write.csv(out.paf, file=paste0(out.tmp, "/draws/paf_yll_", country, "_", this.year, ".csv"), row.names=F)


#----EXPOSURE--------------------------------------------------------------------------------------------------------------------
# Save average ozone at the country level, 3-year mean
# Prep datasets
out.exp <- rep(NA, draws.required)
out.exp.summary <- as.data.frame(matrix(as.integer(NA), nrow=1, ncol=3))

# calculate population weighted draws of exposure 
out.exp <- sapply(1:draws.required,
                  function(draw.number)
                    weighted.mean(exp[[ozone.draw.colnames[draw.number]]],
                                  exp[,pop]))

# calculate mean and CI for summary figures
out.exp.summary[,1] <- quantile(out.exp, .025)
out.exp.summary[,2] <- mean(out.exp)
out.exp.summary[,3] <- quantile(out.exp, .975)
names(out.exp.summary) <- c("exposure_lower","exposure_mean","exposure_upper")
out.exp.summary$location_id <- country
out.exp.summary$year_id <- this.year

names(out.exp) <- paste0("draw_",0:999)
out.exp <- as.data.table(t(out.exp))
out.exp$location_id <- country
out.exp$year_id <- this.year

write.csv(out.exp.summary, file=paste0(out.exp.dir, "/summary/exp_", country, "_", this.year, ".csv"),row.names=F)
write.csv(out.exp, file=paste0(out.exp.dir,"/draws/exp_", country, "_", this.year, ".csv"),row.names=F)

}


lapply(years, yearWrapper)


