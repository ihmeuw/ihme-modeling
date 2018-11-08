#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Run calculations for ozone (PAF/EXP)
#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- commandArgs()[-(1:3)]  # First args are for unix use only
  if (length(arg)==0) {
    #toggle for targeted run on cluster
    arg <- c(101, "7", 15, 1000, 5)
  }
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- c("AND", 1, 9, 1000, 4)
}

# Set parameters
country <- arg[1]
exp.grid.version <- arg[2]
output.version <- arg[3]
draws.required <- as.numeric(arg[4])
cores.provided <- as.numeric(arg[5])
years <- c(1990, 1995, 2000, 2005, 2010:2017)
location_set_version_id <- 398

# load packages, install if missing
pacman::p_load(data.table, ggplot2, grid, parallel, magrittr, RColorBrewer, reshape2, ini)

##function lib##
#general functions#
central.function.dir <- "FILEPATH"
ubcov.function.dir <- "FILEPATH"
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source
# other tools created by covs team for querying db (personal version)
file.path(central.function.dir, "db_tools.R") %>% source
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH/db_tools.r") %>% source
# central functions
file.path("FILEPATH/get_population.R") %>% source
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

# Define important directories/information in objects
# IN
# clean environment with RR and TMREL draws (done separately to preserve covariance across parallel)
file.path("FILEPATH/clean.Rdata") %>% load(envir = globalenv())

#files
#gridded exposure dataset for this country
exp.dir <- file.path("FILEPATH", exp.grid.version)
exp <- file.path(exp.dir, paste0(country, ".csv")) %>% fread

# OUT
#directories
out.paf.dir <- file.path("FILEPATH", output.version)
out.exp.dir <- file.path("FILEPATH", output.version)
out.tmp <- file.path("FILEPATH", output.version)
out.rr.dir <- file.path("FILEPATH", output.version)
#********************************************************************************************************************************

#----PREP---------------------------------------------------------------------------------------------------------------------
# Get the list of most detailed GBD locations
locs <- get_location_hierarchy(location_set_version_id)
this.iso3 <- locs[location_id == country, ihme_loc_id]

# Prep gridded exposure dataset
setkeyv(exp, c('longitude', 'latitude', 'year')) #make sure there are no duplicate grids
exp <- unique(exp)
exp <- exp[!is.na(exp$o3) 
           & !is.na(exp$o3low)
           & !is.na(exp$o3up)
           & !is.infinite(exp$o3) 
           & !is.infinite(exp$pop), ] # Get rid of missings and infinites
exp[,":="(pop=as.numeric(pop),o3=as.numeric(o3),o3low=as.numeric(o3low),o3up=as.numeric(o3up))]
exp[exp$pop <= 0, "pop"] <- 0.1 # set population values of 0 or smaller to be 0.1
exp[exp$o3 <= 0, "o3"] <- 0.1 # Set ozone values of 0 or smaller to be 0.1 (This will have a PAF of 0, so we don't want to drop.) #TODO combine these 2 steps?
exp[is.na(pop), "pop"] <- 0.1 #Set NA pop values to be 0.1. Some small countries are failing because none of the grids have pop. This is Hacky

yearWrapper <- function(this.year) {

  message("Working on the year ", this.year)

#subset to year, then generate SD based confidence interval
this.exp <- exp[year==this.year,]
this.exp[, sd := (o3up-o3low)/(1.96*2)]

#generate draws of exposure using sd and mean
ozone.draw.colnames <- paste0("ozone_",1: draws.required)

#sample draws of the ozone exposure using your predefined SD
#note that we need to do this by row because of restrictions on what the rnorm function can take as input/output

this.exp[, c(ozone.draw.colnames) := rnorm(draws.required, o3, sd) %>% as.list, by=list(rownames(this.exp))]
#********************************************************************************************************************************

#----CALC PAFS-------------------------------------------------------------------------------------------------------------------
# generate RR using draws of ozone, RR, and TMRED with formula rr = base.RR ^ ((exp-tmred)/10) because rr is in terms of 10 ppb ozone
RR <- lapply(1:draws.required,
             function(draw.number)
               ifelse(this.exp[, ozone.draw.colnames[draw.number], with=FALSE] > tmred[draw.number,],
                      rr.draws[draw.number]^((this.exp[[ozone.draw.colnames[draw.number]]]-tmred[draw.number,])/10),
                      1)) # if exposure <= TMRED, there is no elevated risk

# new aggregation formula created by NAME to address the issue that population at the grid level
# doesn't necessarily reflect the number of cases at a grid level
out.paf <- lapply(1:draws.required,
                  function(draw.number)
                    (sum((RR[[draw.number]] - 1)*this.exp[,pop]) / sum(RR[[draw.number]]*this.exp[,pop]))) %>% as.data.table
#********************************************************************************************************************************

#----FORMAT/SAVE-----------------------------------------------------------------------------------------------------------------
# Set up variables
# Currently we only estimate one cause/age group for ozone
out.paf[, acause := "resp_copd"]
out.paf[, cause_id := 509]
out.paf[, age_group_id := 99]
out.paf[, iso3 := this.iso3]
out.paf[, location_id := country]
out.paf[, year_id := this.year]
out.paf[, risk := "air_ozone"]

#pafs must be saved (0-999 instead of 1-1000)
paf.draw.colnames <- c(paste0("paf_", 0:(draws.required-1)))
setnames(out.paf, paste0("V", 1:draws.required), paf.draw.colnames)

# generate mean and CI for summary figures
out.paf[, paf_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=paf.draw.colnames]
out.paf[, paf_mean := rowMeans(.SD), .SDcols=paf.draw.colnames, by=list(cause_id,age_group_id)]
out.paf[, paf_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=paf.draw.colnames]


#Order columns to your liking
out.paf <- setcolorder(out.paf, c("iso3", "location_id", "risk", "acause", "cause_id", "age_group_id", "year_id", "paf_lower", "paf_mean", "paf_upper", paf.draw.colnames))

# Save summary version of PAF output for experts
out.paf.summary <- out.paf[, c("age_group_id","acause","paf_lower","paf_mean","paf_upper")  , with=F]
write.csv(out.paf.summary, file=paste0("FILEPATH/paf_yll_", country, "_", this.year,  ".csv"))

# Convert from age 99 to the correct ages
for (cause.code in c("resp_copd")) {
  # Take out this cause
  temp.paf <- out.paf[out.paf$acause == cause.code, ]
  out.paf <- out.paf[!out.paf$acause == cause.code, ]

  # Add back in with proper ages (need to use age.id instead of age number)
  if (cause.code %in% c("resp_copd")) ages <-  c(10:20, 30:32, 235) # resp_copd are 25+

  for (age.code in ages) {
    temp.paf$age_group_id <- age.code
    out.paf <- rbind(out.paf, temp.paf)
  }
}

#duplicate sex
#create a fucntion to duplicate the rows for all ages and sexes in the DB
saveSex <- function(sex, dt) {

  dt[, sex_id := sex]

  # Save Mortality PAFs
  write.csv(dt, file=paste0("FILEPATH/paf_yll_", country, "_", this.year, "_", sex, ".csv"), row.names=F)

  return(dt)

}

all <- lapply(1:2, saveSex, dt=out.paf) %>% rbindlist

#********************************************************************************************************************************

#----EXPOSURE--------------------------------------------------------------------------------------------------------------------
# Save average ozone at the country level
# Prep datasets
out.exp <- rep(NA, draws.required)
out.exp.summary <- as.data.frame(matrix(as.integer(NA), nrow=1, ncol=3))

# calculate population weighted draws 
out.exp <- sapply(1:draws.required,
                  function(draw.number)
                    weighted.mean(this.exp[[ozone.draw.colnames[draw.number]]],
                                  this.exp[,pop]))

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

write.csv(out.exp.summary, file=paste0("FILEPATH/exp_", country, "_", this.year, ".csv"),row.names=F)
write.csv(out.exp, file=paste0("FILEPATH/exp_", country, "_", this.year, ".csv"),row.names=F)

}


mclapply(years, yearWrapper, mc.cores=cores.provided-1)


