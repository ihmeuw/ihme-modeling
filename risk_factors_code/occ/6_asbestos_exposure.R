# ---HEADER-------------------------------------------------------------------------------------------------------------

# Project: OCC - Asbestos
# Purpose: Calculate AIR for a given country
#********************************************************************************************************************************

# ---CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- commandArgs()[-(1:3)]  # First args are for unix use only
  
  if (length(arg)==0) {
    #toggle for targeted run on cluster
    arg <- c("71", #location
             20, #output.version
             1000, #draws required
             10) #number of cores to provide to parallel functions
  }
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
  #toggle for targeted run on cluster
  arg <- c("492", #location
           1, #output version
           10, #draws required
           1) #number of cores to provide to parallel functions
  
}

# Set parameters from input args
country <- arg[1]
output.version <- arg[2]
draws.required <- as.numeric(arg[3])
cores.provided <- as.numeric(arg[4])

# load packages
pacman::p_load(data.table, fst, gridExtra, ggplot2, lme4, magrittr, parallel, stringr)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

#set values for project
gbd.years <- c(1990, 1995, 2000, 2005, 2010, 2013, 2015, 2016) #removing unnecessary years, takes too long
location_set_version_id <- 149
year_start <- 1970
year_end <- 2016
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
draw.dir <- file.path('FILEPATH')
summary.dir <- file.path(home.dir, "FILEPATH", output.version)

dir.create(draw.dir, recursive=T)
dir.create(summary.dir, recursive=T)
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
central.function.dir <- file.path(h_root, "FILEPATH")
ubcov.function.dir <- file.path(j_root, 'FILEPATH')
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source
# other tools created by covs team for querying db (personal version)
file.path(central.function.dir, "db_tools.R") %>% source
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH") %>% source
# central functions
file.path(j_root, 'FILEPATH') %>% source
file.path(j_root, 'FILEPATH') %>% source
file.path(j_root, 'FILEPATH') %>% source
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


#write draws for each carcinogen to dir #NOTE change to mapply with grid over high/low
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
  
}
#***********************************************************************************************************************

#***********************************************************************************************************************

# ---PREP MESO #s-------------------------------------------------------------------------------------------------------
## Get levels and set to working country
locs <- get_location_hierarchy(location_set_version_id)
locs <- locs[is_estimate==1]
locs[is.na(ihme_loc_id), ihme_loc_id := paste0('CHN_', location_id)]
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

#read in meso numbers from COD
meso <- get_draws(gbd_id_field='cause_id',
                  gbd_id=483, source='codcorrect', location_ids=country,
                  year_ids=c(1970:2016), age_group_ids=relevant.ages,
                  measure_ids=1, status='best', num_workers=(cores.provided-1))

#read in meso comparison rates
meso.s <- file.path(data.dir, "impact_ratio_s.csv") %>% fread
meso.n <- file.path(data.dir, "impact_ratio_n.csv") %>% fread

#read in population using central fx
pops <- get_population(location_set_version_id=location_set_version_id,
                       location_id = paste(unique(meso$location_id), collapse=" "),
                       year_id = paste(unique(meso$year_id), collapse=" "),
                       sex_id = unique(meso$sex_id), #pull all sex groups
                       age_group_id = unique(meso$age_group_id)) #pull only >15

#convert meso numbers to rates per 100k
dt <- merge(meso, pops[, -c('process_version_map_id'), with=F],
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
dt[, measure_id := 18] #now a proportional measure (% of pop exposed to occ asbestos)

me.name <- paste0('occ_carcino_asbestos')

out.dir <- file.path(draw.dir, 'exp', 'occ_carcino', me.name, 'any', output.version)
dir.create(out.dir, recursive = T)

#subset only relevant vars
exp <- dt[, c(key(dt), 'measure_id', exp.cols), with=F]
setnames(exp, exp.cols, draw.cols)
exp[, rei := me.name]

#write exposure
write.csv(exp, file.path(out.dir, paste0(country, '.csv')))

#write paf
paf <- dt[, c(key(dt), 'measure_id', paf.cols), with=F]
paf[, cause_id := 483]
paf[, acause := 'neo_meso']
paf[, rei := me.name]

paf.dir <- file.path(draw.dir, 'paf', 'occ_carcino', me.name, output.version)
dir.create(paf.dir, recursive = T)

for (sex in c(1,2)) {
  
  for (year in gbd.years) {
    
    message('saving paf for the year=', year, " (sex=", sex, ")")
    
    out <- paf[sex_id == sex & year_id==year]
    
    write.csv(out,
              file.path(paf.dir, paste0('paf_yll_', country, '_', year, '_', sex, '.csv')))
    
    write.csv(out,
              file.path(paf.dir, paste0('paf_yld_', country, '_', year, '_', sex, '.csv')))
    
  }
  
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

write.csv(dt[, c(key(dt), 'measure_id',
                 names(dt)[names(dt) %like% 'lower|mean|upper']), with=F],
          file=file.path(summary.dir, paste0(country, ".csv")))
#***********************************************************************************************************************