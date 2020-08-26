
### Setup
rm(list=ls())
windows <- Sys.info()[1][["sysname"]]=="Windows"
root <- ifelse(windows,"J:/","/home/j/")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- "FILEPATH"
## Packages
library(data.table); library(mvtnorm); library(survey); library(ggplot2); library(plyr); library(dplyr)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) > 0) {
  run.name <- args[1]
  loc <- args[2]
  stop.year <- as.integer(args[3])
  i <- as.integer(Sys.getenv("SGE_TASK_ID"))
  paediatric <- as.logical(args[4])
} else {

	run.name <- "190630_rhino"
	loc <- "AGO"
	stop.year <- 2019
	i <- 1
	paediatric <- TRUE
}

run.table <- fread("FILEPATH")
c.args <- run.table[run_name==run.name]

### Arguments
## Some arguments are likely to stay constant across runs, others we're more likely to test different options.
## The arguments that are more likely to vary are pulled from the eppasm run table
start.year <- 1970
trans.params.sub <- TRUE
pop.sub <- TRUE
art.sub <- TRUE
prev.sub <- TRUE
sexincrr.sub <- TRUE
plot.draw <- FALSE
geoadjust <- c.args[['anc_sub']]
age.prev <- c.args[['age_prev']]
popadjust <- c.args[['popadjust']]
anc.rt <- c.args[['anc_rt']]
epp.mod <- c.args[['epp_mod']]
geoadjust <- c.args[['anc_sub']]
no_anc <- c.args[['no_anc']]
anc.prior.sub <- TRUE

### Paths
out.dir <- "FILEPATH"

### Functions
library(mortdb, lib = "/home/j/WORK/02_mortality/shared/r")
setwd(paste0(ifelse(windows, "H:", paste0("/ihme/homes/", user)), "/eppasm/"))
devtools::load_all()
setwd(code.dir)
devtools::load_all()

### Tables
loc.table <- get_locations(hiv_metadata = TRUE, gbd_year=2019)

# These locations do not have information from LBD team estimates

no_geo_adj <-  c(loc.table[epp ==1 & grepl("IND",ihme_loc_id),ihme_loc_id],"PNG","HTI","DOM", loc.table[epp ==1 & grepl("ZAF",ihme_loc_id),ihme_loc_id])

# ANC data bias adjustment
if(geoadjust & !loc %in% no_geo_adj){
  geoadjust  <- TRUE
} else {
  geoadjust  <- FALSE
}

### Code
## Read in spectrum object, sub in GBD parameters
dt <- read_spec_object(loc, i, start.year, stop.year, trans.params.sub, pop.sub, prev.sub, art.sub, sexincrr.sub, popadjust, age.prev, paediatric, anc.rt, geoadjust, anc.prior.sub)

anc = attr(dt,"eppd")$ancsitedat
nrow(attr(dt,"eppd")$hhs)

if(epp.mod == 'rspline'){attr(dt, 'specfp')$equil.rprior <- TRUE}

#Some substitutions to get things running
if(grepl('NGA', loc)){
  temp <- attr(dt, 'specfp')$paedsurv_artcd4dist
  temp[temp < 0] <- 0
  attr(dt, 'specfp')$paedsurv_artcd4dist <- temp
}

## Replace on-ART mortality RR for TZA and UGA
if(loc %in% c('UGA', 'TZA')){
  temp <- readRDS(paste0("FILEPATH"))
  temp.artmxrr <- attr(temp, 'specfp')$artmx_timerr
  attr(dt, 'specfp')$artmx_timerr <- temp.artmxrr
}

if(run.name %in% c("190630_fixonARTIND","190630_fixonARTIND_tightprior")){
  temp <- readRDS(paste0("FILEPATH"))
  temp.artmxrr <- attr(temp, 'specfp')$artmx_timerr
  attr(dt, 'specfp')$artmx_timerr <- temp.artmxrr
}

attr(dt, 'eppd')$ancsitedat = unique(attr(dt, 'eppd')$ancsitedat)

attr(dt, 'eppd')$hhs <- attr(dt, 'eppd')$hhs[!attr(dt, 'eppd')$hhs$se == 0,]
attr(dt, 'specfp')$relinfectART <- 0.3

if(grepl("IND",loc)){
  if(no_anc){
    attr(dt,"eppd")$ancsitedat <- NULL
  }
  attr(dt, 'specfp')$art_alloc_mxweight <- 0.5
}

## Fit model
fit <- eppasm::fitmod(dt, eppmod = epp.mod, B0 = 1e4, B = 1e3, number_k = 200)


data.path <- "FILEPATH"
if(!file.exists(data.path)){
save_data(loc, attr(dt, 'eppd'), run.name)
}


## When fitting, the random-walk based models only simulate through the end of the
## data period. The `extend_projection()` function extends the random walk for r(t)
## through the end of the projection period.
if(epp.mod == 'rhybrid'){
  fit <- extend_projection(fit, proj_years = stop.year - start.year + 1)
}


## Simulate model for all resamples, choose a random draw, get gbd outputs
result <- gbd_sim_mod(fit, VERSION = 'R')
output.dt <- get_gbd_outputs(result, attr(dt, 'specfp'), paediatric = paediatric)
output.dt[,run_num := i]

## Write output to csv
dir.create(out.dir, showWarnings = FALSE)
write.csv(output.dt, paste0(out.dir, '/', i, '.csv'), row.names = F)

# ## under-1 splits
if(paediatric){
  split.dt <- get_under1_splits(result, attr(dt, 'specfp'))
  split.dt[,run_num := i]
  write.csv(split.dt, paste0(out.dir, '/under_1_splits_', i, '.csv' ), row.names = F)
}

## Write out theta for plotting posterior
param <- data.table(theta = attr(result, 'theta'))
write.csv(param, paste0(out.dir,'/theta_', i, '.csv'), row.names = F)

if(plot.draw){
  plot_15to49_draw(loc, output.dt, attr(dt, 'eppd'), run.name)
}



