# ---HEADER-------------------------------------------------------------------------------------------------------------

# Project: OCC - OCC
# Purpose: Prep occupational carcinogen exposure

#***********************************************************************************************************************

# ---CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# set toggles

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- commandArgs()[-(1:3)]  # First args are for unix use only
  
  if (length(arg)==0) {
    #toggle for targeted run on cluster
    arg <- c("361", #location
             "11", #worker version
             "25", #output version
             "1000", #draws
             "20") #cores
  }
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
  arg <- c('492') #number of cores to provide to parallel functionss
  
}

# load packages
pacman::p_load(data.table, fst, gridExtra, ggplot2, lme4, magrittr, parallel, stringr, readxl, zoo)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

# Set parameters from input args
country   <- arg[1]
worker.version  <- arg[2]
output.version  <- arg[3]
draws.required  <- arg[4] %>% as.numeric
cores.provided  <- arg[5] %>% as.numeric

#toggle sections
est.part <- T #toggle to estimate and save results for particulates
est.noise <- T #toggle to estimate and save results for noise

#set values for project
location_set_version_id <- 149
all.ages <- c(2:20, 30, 31, 32, 235)
relevant.ages <- all.ages[all.ages >= 8] #only ages 15+
gbd.years <- c(1990, 1995, 2000, 2005, 2010, 2013, 2015, 2016)
year_start <- 1970
year_end <- 2016

#colnames
id.vars <- c('location_id', 'year_id', 'isic_code', 'sex_id', 'age_group_id')
draw.cols <- paste0('draw_', 0:(draws.required-1))
exp.cols <- paste0('exp_', 0:(draws.required-1))
emp.cols <- paste0('emp_', 0:(draws.required-1))
prop.cols <- paste0('prop_', 0:(draws.required-1))
sum.cols <- paste0('sum_', 0:(draws.required-1))
worker.cols <- paste0('workers_', 0:(draws.required-1))

##in##
cw.dir <- file.path(home.dir, "FILEPATH")
isic2.map <- file.path(cw.dir, "ISIC_2_TO_3_LVL_1_CW.csv") %>% fread
exp.dir <- file.path(home.dir, "FILEPATH")
doc.dir <- file.path(home.dir, 'FILEPATH')

exp.2015.dir <- file.path(home.dir, "FILEPATH")
other.exposures <- file.path(exp.2015.dir, 'exposures.csv') %>% fread

isic.map <- file.path(doc.dir, 'ISIC_MAJOR_GROUPS_BY_REV.xlsx')
isic.3.map <- read_xlsx(isic.map, sheet = 2) %>% as.data.table

gpr.dir <- file.path(j_root, 'FILEPATH')
worker.dir <- file.path('FILEPATH', worker.version)

##out##
summary.dir <- file.path(home.dir, "FILEPATH", output.version)
draw.dir <- file.path('FILEPATH')
lapply(c(summary.dir, draw.dir), dir.create, recursive=T)

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
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

#files#
#specify the function to define file to hold emp.occ/industry draws
fileName <- function(dir,
                     loc,
                     value,
                     suffix) {
  
  file.path(dir, paste0(loc, '_', value, suffix)) %>% return
  
}

#function to check variables of a merge and see which were dropped
checkMerge <- function(var, old, new) {
  old[, get(var) %>% unique][old[, get(var) %>% unique] %ni% new[, get(var) %>% unique]] %>% return
}

#now create cohorts of back-casted exposure in order to calculate the cumulative exposure over the past 25 years
ageWrapper <- function(this.age,
                       current.year,
                       all.data,
                       cum.grid,
                       this.function,
                       ...) {
  
  if (cores.provided == 1) message("-calculating for ", this.age, "|", current.year)
  
  #calculate year of birth for this age cohort
  yob <- current.year - age.map[age_group_id == this.age, age_mid]
  years <- seq(yob, current.year) #years produced by st-gpr for blood lead (this time only up to yob)
  
  #now loop through all the years and keep rows relevant to our cohort
  #currently if year is less than what we have from st-gpr, just use the earliest year exposure
  cohortBuilder <- function(this.year,
                            ...) {
    
    #calculate what age our start cohort would have been in this year
    cohort.age.start <- round_any(age.map[age_group_id == this.age, age_mid] - (current.year - this.year), 5)
    cohort.age.id <- age.map[age_group_years_start == cohort.age.start, age_group_id]
    
    if (cores.provided == 1) {message("age start: ", cohort.age.start, "for year", this.year)}
    
    if (cohort.age.start < 0) stop("help!, age start is zero")
    
    #if the year is prior to st-gpr's earliest estimate, just use the earliest estimate
    this.year <- ifelse(this.year<year_start, year_start, this.year)
    
    year.data <- all.data[age_group_id == cohort.age.id & year_id == this.year, ]
    
    return(year.data)
    
  }
  
  all.cohort <- lapply(years,
                       cohortBuilder) %>% rbindlist
  
  # now pass that function each draw and replace with estimated bone lead for that draw
  out <- mapply(this.function, this.sex=cum.grid$sex, this.exp=cum.grid$exp,
                MoreArgs=list(current.age=this.age, dt=all.cohort), SIMPLIFY=F) %>% rbindlist
  
  out[, age_group_id := this.age]
  out[, year_id := current.year]
  out[, location_id := country %>% as.numeric]
  
  return(out)
  
}

# now create a function that will cumulative exposure as the AVERAGE population exposure over the cohort's lifetime
meanCumulatoR <- function(this.sex, this.exp, current.age, dt) {
  
  latency <- 50 #assume "lifetime" latency for these risks
  
  if (cores.provided == 1) message('avg (', latency, ' years) exposure for sex=', this.sex)
  
  start.age <- age.map[age_group_years_start == age.map[age_group_id==current.age, age_group_years_start] - latency, age_group_id]
  start.age <- ifelse(length(start.age)==0, 2, start.age) #use 0 as the beginning of the start age < latency
  
  cumulative <- dt[age_group_id %in% c(start.age:current.age) & sex_id==this.sex & rate==this.exp,
                   lapply(.SD, mean), .SDcols=exp.cols]
  
  cumulative[, sex_id := this.sex]
  cumulative[, rate := this.exp]
  
}
#***********************************************************************************************************************

# ---PREP POPULATION----------------------------------------------------------------------------------------------------
#merge on levels
locs <- get_location_hierarchy(location_set_version_id)
locations <- locs[is_estimate==1, unique(location_id)]
levels <- locs[,grep("level|location_id|ihme_loc_id", names(locs)), with=F]
this.region <- locs[location_id == country, region_name]
dev.status <- locs[location_id == country, developed] %>% as.numeric %>% as.logical

#some of the subnats dont have developed status filled out in the db
#so keep climbing the hierarchy until you find it out
if(is.na(dev.status)) {
  
  parent <- locs[location_id == country, parent_id]
  
  while(is.na(dev.status)) {
    
    dev.status <- locs[location_id == parent, developed] %>% as.numeric %>% as.logical
    parent <- locs[location_id == parent, parent_id]
    
  }
}

# get the covariates to interpolate high/low exposure across dev status using SDI
#previously dev status indicated high exposure settings of 10% for developed and 50% for developing
#use these as the endpoints across sdi min/max and interpolate
covs <- get_covariates('sdi')
covs[sdi==min(sdi), high_exposure := .5]
covs[sdi==max(sdi), high_exposure := .1]
covs <- covs[order(sdi)]
covs[, high_exposure := na.approx(high_exposure)]
covs[, low_exposure := 1-high_exposure]
high.exposure <- covs[, -c('age_group_id', 'sex_id', 'sdi')] #cleanup superfluous vars

#age map
ages <- get_ages()
age.map <- ages[, list(age_group_id, age_group_years_start, age_group_years_end)]
age.map[age_group_id >= 8, age_mid := ceiling((age_group_years_start + age_group_years_end)/2)] #self explanatory

## then get pop
#read in population using central fx
pops <- get_population(location_set_version_id=location_set_version_id,
                       location_id = country,
                       year_id = c(year_start:year_end),
                       sex_id = c(1,2), #pull all sexes
                       age_group_id = all.ages) #pull only 15-69

#***********************************************************************************************************************

# ---PREP EMPLOYMENT DRAWS----------------------------------------------------------------------------------------------
#read in best version of workers per industry
workers <- fileName(worker.dir, country, 'ind', '_workers.fst') %>% read.fst(as.data.table=T)
workers[, isic_code := substr(me_name, 15, 15)]

#read in best version of employment
employment <- fileName(worker.dir, country, 'emp', '_prop.fst') %>% read.fst(as.data.table=T)
setnames(employment, draw.cols, emp.cols)

#read in the best version of proportions working per industry
prop <- fileName(worker.dir, country, 'ind', '_prop.fst') %>% read.fst(as.data.table=T)
prop[, isic_code := substr(me_name, 15, 15)]
#***********************************************************************************************************************

# ---EXPOSURE PREP------------------------------------------------------------------------------------------------------
#first prep the raw exposures
vars <- names(other.exposures)[names(other.exposures) %like% '_dev']
exp.dt <- other.exposures[, c('group', vars), with=F]
exp.dt <- exp.dt[-1] #remove the first row, its labelling
exp.dt[, (vars) := lapply(.SD, as.numeric), .SDcols=vars]
exp.dt[, major_isic_2 := substr(group, 1, 1) %>% as.numeric]

#crosswalk ISIC 2 to ISIC 3
exp.dt <- merge(exp.dt, isic2.map[, list(major_isic_2, major_isic_3, weight)],
                by="major_isic_2", allow.cartesian = T)

#now use the proportion to xwalk each ISIC2 category to ISIC3, then collapse
#now use the proportion to xwalk each ISIC2 category to ISIC3, then collapse
exp.dt[, (vars) := lapply(.SD, function(x) weighted.mean(as.numeric(x), w=weight)), .SDcols=vars, by=c('major_isic_3')]
exp.dt <- unique(exp.dt, by=c('major_isic_3'))
setnames(exp.dt, 'major_isic_3', 'isic_code')
exp.dt <- exp.dt[, c('isic_code', vars), with=F]

#reshape to prep for merging
exp.dt <- melt(exp.dt, id.vars='isic_code')
exp.dt[variable %like% 'hearing', exposure_type := 'noise']
exp.dt[variable %like% 'copd', exposure_type := 'particulates']
exp.dt[variable %like% 'deved', dev_status := TRUE]
exp.dt[variable %like% 'deving', dev_status := FALSE]
exp.dt[variable %like% '_hi', rate := "high"]
exp.dt[variable %like% '_low', rate := "low"]
exp.dt[variable %like% '_00', noise_year := "00"]
exp.dt[variable %like% '_90', noise_year := "90"]
#***********************************************************************************************************************

# ---ESTIMATE PARTICULATES----------------------------------------------------------------------------------------------
if (est.part==TRUE) {
  #calculate industry exposure as rate of exposure * # of workers in industry
  #note that this is still based on development status
  
  #use the relevant exposures for the working development status
  workers[, dev_status := dev.status]
  
  #merge on particulate exposure rates
  current <- merge(workers, exp.dt[exposure_type=='particulates'], by=c('isic_code', 'dev_status'), allow.cartesian=TRUE)
  
  #now calculate industry exposure as rate of exposure * # of workers in industry / total population
  current[, (exp.cols) := lapply(.SD, function(x) value * x / population), .SDcols=worker.cols]
  
  #now collapse the isic_codes, no longer need all levels
  setkeyv(current, c('location_id', 'year_id', 'sex_id', 'age_group_id', 'rate'))
  current[, (exp.cols) := lapply(.SD, sum), .SDcols=exp.cols, by=key(current)]
  current <- unique(current, by=key(current))
  current[, isic_code := "T"]
  current[, me_name := NULL]
  
  #now loop through the relevant ages to using custom backcasting fx to calc cumulative exposure for each of them
  grid <- expand.grid(ages=relevant.ages, years=gbd.years)
  past <- mcmapply(ageWrapper,
                   this.age=grid$ages,
                   current.year=grid$years,
                   MoreArgs=list(all.data=current,
                                 this.function=meanCumulatoR,
                                 cum.grid=expand.grid(sex=unique(current$sex_id), exp=unique(current$rate))),
                   mc.cores = cores.provided,
                   SIMPLIFY=F) %>% rbindlist
  
  #save the draws
  setnames(past, exp.cols, draw.cols)
  me.name <- paste0('occ_particulates')
  past[, rei := me.name]
  past[, measure_id := 18] #now a proportional measure (% of pop exposed to noise)
  
  high.dir <- file.path(draw.dir, me.name, 'high', output.version)
  dir.create(high.dir, recursive = T)
  low.dir <- file.path(draw.dir, me.name, 'low', output.version)
  dir.create(low.dir, recursive = T)
  
  for(this.rate in c('high', 'low')) {
    
    write.csv(past[rate==this.rate, c('location_id', 'year_id', 'age_group_id', 'sex_id', 'rate', 'rei', 'measure_id', draw.cols), with=F],
              file.path(get(paste0(this.rate, '.dir')), paste0(country, '.csv')))
    
  }
  
  #calculate mean/CI to save summary table
  past <- past[, exp_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=draw.cols]
  past <- past[, exp_mean := rowMeans(.SD), .SDcols=draw.cols]
  past <- past[, exp_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=draw.cols]
  
  write.csv(past[, c('location_id', 'year_id', 'sex_id', 'age_group_id', 'rate', 'measure_id',
                     names(past)[names(past) %like% 'lower|mean|upper']), with=F],
            file.path(summary.dir, paste0(country, '_particulates.csv')))
}
#***********************************************************************************************************************

# ---ESTIMATE NOISE-----------------------------------------------------------------------------------------------------
if (est.noise==TRUE) {
  #calculate industry exposure as rate of exposure * # of workers in industry
  #note that this is still based on development status (also whether the year is post 2000 or not)
  
  #use the relevant exposures for the working development status & year
  workers[year_id>=2000, noise_year := "00"]
  workers[is.na(noise_year), noise_year := "90"]
  
  if (dev.status) { #if the country is developed, there is no temporal trend for noise exposure
    
    current <- merge(workers, exp.dt[exposure_type=='noise'], by=c('isic_code', 'dev_status'), allow.cartesian=TRUE)
    
  } else current <- merge(workers, exp.dt[exposure_type=='noise'], by=c('isic_code', 'noise_year', 'dev_status'), allow.cartesian=TRUE)
  
  #now calculate industry exposure as rate of exposure * # of workers in industry / total population
  current[, (exp.cols) := lapply(.SD, function(x) value * x / population), .SDcols=worker.cols]
  
  #now collapse the isic_codes, no longer need all levels
  setkeyv(current, c('location_id', 'year_id', 'sex_id', 'age_group_id', 'rate'))
  current[, (exp.cols) := lapply(.SD, sum), .SDcols=exp.cols, by=key(current)]
  current <- unique(current, by=key(current))
  current[, isic_code := "T"]
  current[, me_name := NULL]
  
  
  #now loop through the relevant ages to using custom backcasting fx to calc cumulative exposure for each of them
  grid <- expand.grid(ages=relevant.ages, years=gbd.years)
  past <- mcmapply(ageWrapper,
                   this.age=grid$ages,
                   current.year=grid$years,
                   MoreArgs=list(all.data=current,
                                 this.function=meanCumulatoR,
                                 cum.grid=expand.grid(sex=unique(current$sex_id), exp=unique(current$rate))),
                   mc.cores = cores.provided,
                   SIMPLIFY=F) %>% rbindlist
  
  #save the draws
  setnames(past, exp.cols, draw.cols)
  me.name <- paste0('occ_hearing')
  past[, rei := me.name]
  past[, measure_id := 18] #now a proportional measure (% of pop exposed to noise)
  
  high.dir <- file.path(draw.dir, me.name, 'high',  output.version)
  dir.create(high.dir, recursive = T)
  low.dir <- file.path(draw.dir, me.name, 'low', output.version)
  dir.create(low.dir, recursive = T)
  
  for(this.rate in c('high', 'low')) {
    
    write.csv(past[rate==this.rate, c('location_id', 'year_id', 'age_group_id', 'sex_id', 'rate', 'rei', 'measure_id', draw.cols), with=F],
              file.path(get(paste0(this.rate, '.dir')), paste0(country, '.csv')))
    
  }
  
  #calculate mean/CI to save summary table
  past <- past[, exp_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=draw.cols]
  past <- past[, exp_mean := rowMeans(.SD), .SDcols=draw.cols]
  past <- past[, exp_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=draw.cols]
  
  write.csv(past[, c('location_id', 'year_id', 'sex_id', 'age_group_id', 'rate', 'measure_id',
                     names(past)[names(past) %like% 'lower|mean|upper']), with=F],
            file.path(summary.dir, paste0(country, '_noise.csv')))
}
#***********************************************************************************************************************

# ---SCRAP--------------------------------------------------------------------------------------------------------------

#***********************************************************************************************************************