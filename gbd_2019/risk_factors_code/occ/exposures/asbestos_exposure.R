# ---HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Calculate asbestos impact ratio (AIR) for a given country and mesothelioma PAFs
#********************************************************************************************************************************

# ---CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# Set parameters from input args
arg <- commandArgs(trailingOnly = T)
country <- arg[1]
output.version <- arg[2]
draws.required <- as.numeric(arg[3])
cores.provided <- as.numeric(arg[4])

# load packages
pacman::p_load(data.table, gridExtra, ggplot2, lme4, magrittr, parallel, stringr, zoo, openxlsx)
library(fst, lib.loc = "FILEPATH")

# set working directories
home.dir <- "FILEPATH"
  setwd(home.dir)

## set values for project
gbd.years <- 1990:2019
location_set_id <- 35
year_start <- 1970
year_end <- 2019
by_sex <- 1
by_age <- 0
  relevant.ages <- c(8:20, 30:32, 235) #only ages 15+

##varlists##
exp.cols <- paste0("exp_", 0:(draws.required-1))
draw.cols <- paste0("draw_", 0:(draws.required-1))
paf.cols <- paste0("paf_", 0:(draws.required-1))

c.cols <- paste0("c_", 0:(draws.required-1))
n.cols <- paste0("n_", 0:(draws.required-1))
s.cols <- paste0("s_", 0:(draws.required-1))

all.draws <- c(exp.cols, draw.cols, c.cols, n.cols, s.cols)

##in##
data.dir <- file.path(home.dir, "FILEPATH")

##out##
draw.dir <- "FILEPATH"
summary.dir <- file.path("FILEPATH", output.version)
if(!dir.exists(draw.dir)) dir.create(draw.dir, recursive=T)
if(!dir.exists(summary.dir)) dir.create(summary.dir, recursive=T)
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
ubcov.function.dir <- "FILEPATH"
file.path(ubcov.function.dir, "FUNCTION") %>% source
# central functions
source("FUNCTION")
source("FUNCTION")
source("FUNCTION")
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

pafCalc <- function(exp_rr, exp_prev, tmrel_rr) {

  unexp_prev <- 1-exp_prev

  pop_rr <- (exp_rr * exp_prev) + (tmrel_rr * unexp_prev)

  paf <- (pop_rr - tmrel_rr) / pop_rr

  return(paf)

}

#cap values as per original code
capValues <- function(draw) {

  if (draw > 1) {
    out <- 1
  } else if (draw < 0) {
    out <- 0
  } else {
    out <- draw
  }

  return(out)

}


#write draws for each carcinogen to dir
saveCarc <- function(this.carc) {

  me.name <- paste0('occ_carcino_', this.carc)

  high.dir <- file.path(draw.dir, 'occ_carcino', me.name, 'high', output.version)
    dir.create(high.dir, recursive = T)
  low.dir <- file.path(draw.dir, 'occ_carcino', me.name, 'low', output.version)
    dir.create(low.dir, recursive = T)

  #subset only relevant vars
  out <- dt[, c(key(dt), 'measure_id', exp.cols), with=F]
  setnames(out, exp.cols, draw.cols)
  out[, rei := me.name]

  for(rate in c('high', 'low')) {

    if (rate=='high') {
      rate.dt <- copy(out)
      rate.dt[, (draw.cols) := lapply(.SD, function(x) x * high.exp), .SDcols=draw.cols]
    } else if (rate=='low') {
      rate.dt <- copy(out)
      rate.dt[, (draw.cols) := lapply(.SD, function(x) x * low.exp), .SDcols=draw.cols]
    }

    write.csv(rate.dt, file.path(get(paste0(rate, '.dir')), paste0(country, '.csv')))

  }

  return(dim(out))

  message("results saved to ", high.dir, " and ", low.dir)

}
#***********************************************************************************************************************

#***********************************************************************************************************************

# ---PREP MESO #s-------------------------------------------------------------------------------------------------------
## Get levels and set to working country
locs <- get_location_metadata(gbd_round_id = 6, location_set_id = 22, decomp_step = "step4")
locs <- locs[is_estimate==1]
this.region <- locs[location_id == country, region_name]
dev.status <- locs[location_id == country, developed] %>% as.numeric %>% as.logical

#some of the subnats dont have developed status filled out in the db
#so keep climbing the hierarchy until you find it out
if (is.na(dev.status)) {

  parent <- locs[location_id == country, parent_id]

  while(is.na(dev.status)) {

    dev.status <- locs[location_id == parent, developed] %>% as.numeric %>% as.logical
    parent <- locs[location_id == parent, parent_id]

  }
}

# read in meso numbers from COD
meso <- get_draws(gbd_id_type='cause_id',
                  gbd_id=483, source='codcorrect', location_id=country,
                  year_id=gbd.years, age_group_id=relevant.ages,
                  measure_id=1, version=101, num_workers=(cores.provided-1),
                  decomp_step = "step4", gbd_round_id = 6)

#read in meso comparison rates
meso.s <- file.path(data.dir, "impact_ratio_s.csv") %>% fread
meso.n <- file.path(data.dir, "impact_ratio_n.csv") %>% fread

#read in population using central fx
pops <- get_population(location_set_id=location_set_id,
                       location_id = unique(meso$location_id),
                       year_id = unique(meso$year_id),
                       sex_id = unique(meso$sex_id), #pull all sex groups
                       age_group_id = unique(meso$age_group_id), #pull only >15
                       decomp_step = "step4", gbd_round_id = 6)

#convert meso numbers to rates per 100k
dt <- merge(meso, pops[, -c('run_id'), with=F],
            by=c('location_id', 'year_id', 'age_group_id', 'sex_id'))

dt[, (c.cols):= lapply(.SD, function(x) x / population * 1e5),
   .SDcols=draw.cols]


#merge the background meso rates from lit
dt <- merge(dt, meso.n[, c('sex_id', n.cols), with=F], by='sex_id') %>%
  setkeyv(c('location_id', 'year_id', 'sex_id', 'age_group_id'))

#then we cbind the high since its a global rate for both sexes
dt <- cbind(dt, meso.s)

#calculate exp using AIR
dt[, (exp.cols) := lapply(1:draws.required, function(draw) {

  numerator <- get(c.cols[draw])-get(n.cols[draw])

  denominator <- get(s.cols[draw])-get(n.cols[draw])

  air <- numerator/denominator

  return(air)

})]

#also calculate PAF for meso
dt[, (paf.cols) := lapply(1:draws.required, function(draw) {

  numerator <- get(c.cols[draw])-get(n.cols[draw])

  denominator <- get(c.cols[draw])

  paf <- numerator/denominator

  return(paf)

})]

dt[, (exp.cols) := lapply(.SD, capValues), .SDcols=exp.cols, by=key(dt)]
dt[, (paf.cols) := lapply(.SD, capValues), .SDcols=paf.cols, by=key(dt)]
dt[, measure_id := NULL]

me.name <- paste0('occ_carcino_asbestos')

out.dir <- file.path(draw.dir, 'exp', 'occ_carcino', me.name, 'any', output.version)
  dir.create(out.dir, recursive = T)

#subset only relevant vars
exp <- dt[, c(key(dt), exp.cols), with=F]
setnames(exp, exp.cols, draw.cols)
exp[, rei := me.name]

#write exposure
write.csv(exp, file.path(out.dir, paste0(country, '.csv')))

message("exposure saved to ", file.path(out.dir, paste0(country, '.csv')))

#write paf
paf <- dt[, c(key(dt), paf.cols), with=F]
paf[, cause_id := 483]
paf[, acause := 'neo_meso']
paf[, rei := me.name]
paf[,measure_id := 3]
yll <- copy(paf)
yll[,measure_id := 4]
paf <- rbind(paf,yll)

paf.dir <- file.path(draw.dir, 'paf', 'occ_carcino', me.name, output.version)
  dir.create(paf.dir, recursive = T)

for (year in gbd.years) {

  message('saving paf for the year=', year)

  out <- paf[year_id==year]

  write.csv(out,
            file.path(paf.dir, paste0('paf_', country, '_', year, '.csv')))

  message("paf saved to ", file.path(paf.dir, paste0('paf_', country, '_', year, '.csv')))

}

#create summary stats
dt[, rate_lower := apply(.SD, 1, quantile, c(.025)), .SDcols=c.cols]
dt[, rate_mean := apply(.SD, 1, mean), .SDcols=c.cols]
dt[, rate_upper := apply(.SD, 1, quantile, c(.975)), .SDcols=c.cols]

dt[, exp_lower := apply(.SD, 1, quantile, c(.025)), .SDcols=exp.cols]
dt[, exp_mean := apply(.SD, 1, mean), .SDcols=exp.cols]
dt[, exp_upper := apply(.SD, 1, quantile, c(.975)), .SDcols=exp.cols]

dt[, paf_lower := apply(.SD, 1, quantile, c(.025)), .SDcols=paf.cols]
dt[, paf_mean := apply(.SD, 1, mean), .SDcols=paf.cols]
dt[, paf_upper := apply(.SD, 1, quantile, c(.975)), .SDcols=paf.cols]

dt[, paf_mean_2 := pafCalc(exp_rr=65, exp_prev=exp_mean, tmrel_rr=1)]
dt[, diff := paf_mean - paf_mean_2]

write.csv(dt[, c(key(dt),
                  names(dt)[names(dt) %like% 'lower|mean|upper']), with=F],
              file=file.path(summary.dir, paste0(country, "_asbestos.csv")))

message("summary saved to ", file.path(summary.dir, paste0(country, "_asbestos.csv")))
