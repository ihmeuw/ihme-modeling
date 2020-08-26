# ---HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: calculate carcinogen exposure
#***********************************************************************************************************************

# ---CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

pacman::p_load(data.table, gridExtra, ggplot2, lme4, magrittr, parallel, stringr, zoo, openxlsx)
library(fst, lib.loc = "FILEPATH")

# set working directories
home.dir <- "FILEPATH"
  setwd(home.dir)

# Set parameters from input args
arg <- commandArgs(trailingOnly = T)
country         <- arg[1]
worker.version  <- arg[2]
output.version  <- arg[3]
draws.required  <- arg[4] %>% as.numeric
cores.provided  <- arg[5] %>% as.numeric

#set values for project
location_set_version_id <- 443
location_set_id <- 22
all.ages <- c(2:20, 30, 31, 32, 235)
relevant.ages <- all.ages[all.ages >= 8] #only ages 15+
gbd.years <- c(seq(1990,2005,5),2007,2010,2015,2016,2017,2019)
gbd.years <- seq(1990,2019)
year_start <- 1970
year_end <- 2019

carex.version <- 4 # gbd 19 final

carc.list <- c("acid",
               "arsenic",
               "benzene",
               "beryllium",
               "cadmium",
               "chromium",
               "diesel",
               "formaldehyde",
               "nickel",
               "pah",
               "silica",
               "trichloroethylene")

#colnames
id.vars <- c('location_id', 'year_id', 'isic_code', 'sex_id', 'age_group_id')
draw.cols <- paste0('draw_', 0:(draws.required-1))
exp.cols <- paste0('exp_', 0:(draws.required-1))
emp.cols <- paste0('emp_', 0:(draws.required-1))
prop.cols <- paste0('prop_', 0:(draws.required-1))
sum.cols <- paste0('sum_', 0:(draws.required-1))
turnover.cols <- paste0('turnover_', 0:(draws.required-1))
worker.cols <- paste0('workers_', 0:(draws.required-1))

##in##
cw.dir <- "FILEPATH"
isic2.map <- file.path(cw.dir, "ISIC_2_TO_3_LVL_1_CW.csv") %>% fread
exp.dir <- file.path("FILEPATH", carex.version)
doc.dir <- file.path(home.dir, "FILEPATH")

isic.map <- file.path(doc.dir, 'ISIC_MAJOR_GROUPS_BY_REV.xlsx')
  isic.3.map <- read.xlsx(isic.map, sheet = 2) %>% as.data.table

turnover.dir <- file.path(home.dir, "FILEPATH")
  turnover <- file.path(turnover.dir, 'turnover_01292015.csv') %>% fread

gpr.dir <- "FILEPATH"
worker.dir <- file.path("FILEPATH", worker.version)

##out##
summary.dir <- file.path("FILEPATH", output.version)
draw.dir <- "FILEPATH"
if(!dir.exists(summary.dir)) dir.create(summary.dir, recursive = TRUE)
if(!dir.exists(draw.dir)) dir.create(draw.dir, recursive = TRUE)

#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
ubcov.function.dir <- "FILEPATH"
file.path(ubcov.function.dir, "utilitybelt/db_tools.r") %>% source
# central functions
file.path("FUNCTION") %>% source
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
  years <- seq(yob, current.year) #years produced by st-gpr (this time only up to yob)

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

  out <- mapply(this.function, this.sex=cum.grid$sex, this.exp=cum.grid$exp,
                MoreArgs=list(current.age=this.age, dt=all.cohort), SIMPLIFY=F) %>% rbindlist

  out[, age_group_id := this.age]
  out[, year_id := current.year]
  out[, location_id := country %>% as.numeric]

  return(out)

}

# now create a function that will cumulative exposure as the TOTAL population exposure over the cohort's lifetime
sumCumulatoR <- function(this.sex, this.exp, current.age, dt) {

    latency <- ifelse(this.exp %in% c('BENZ', 'FORM'), 10, 25) #assume latency of 25 years for all except short turnover cancers

    if (cores.provided == 1) message('avg (', latency, ' years) exposure to ', this.exp, " for sex=", this.sex)

    start.age <- age.map[age_group_years_start == age.map[age_group_id==current.age, age_group_years_start] - latency, age_group_id]
    start.age <- ifelse(length(start.age)==0, 2, start.age) #use 0 as the beginning of the start age < latency

    cumulative <- dt[age_group_id %in% c(start.age:current.age) & sex_id==this.sex & ccode==this.exp,
                     lapply(.SD, sum), .SDcols=exp.cols]

    cumulative[, ccode := this.exp]
    cumulative[, sex_id := this.sex]

}
#***********************************************************************************************************************

# ---PREP POPULATION----------------------------------------------------------------------------------------------------
#merge on levels
source("FUNCTION")
source("FUNCTION")
source("FUNCTION")

locs <- get_location_metadata(gbd_round_id = 6, location_set_id = 22)
locations <- locs[is_estimate==1, unique(location_id)] %>% sort
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

if (dev.status) {
  high.exposure <- data.table(location_id = as.numeric(country),
                              high_exposure = .1,
                              low_exposure = .9)
} else {
  high.exposure <- data.table(location_id = as.numeric(country),
                              high_exposure = .5,
                              low_exposure = .5)
}

ages <- get_ids("age_group")[age_group_id %in% all.ages]
ages[age_group_id == 235, age_group_name := "95 to 99"]
ages[,c("age_group_years_start","to","age_group_years_end") := tstrsplit(age_group_name," ")]
ages[,c("age_group_years_start","age_group_years_end") := lapply(.SD,as.numeric), .SDcols = c("age_group_years_start","age_group_years_end")]
age.map <- ages[, list(age_group_id, age_group_years_start, age_group_years_end)]
age.map[age_group_id == 2,c("age_group_years_start","age_group_years_end") := list(0,0.01917808)]
age.map[age_group_id == 3,c("age_group_years_start","age_group_years_end") := list(0.01917808,0.07671233)]
age.map[age_group_id == 4,c("age_group_years_start","age_group_years_end") := list(0.07671233,1)]
age.map[age_group_id >= 8, age_mid := ceiling((age_group_years_start + age_group_years_end)/2)]
age.map[age_group_id > 4,age_group_years_end := age_group_years_end + 1]

## then get pop
# read in population using central fx
pops <- get_population(location_set_version_id=location_set_version_id,
                       location_id = country,
                       year_id = c(year_start:year_end),
                       sex_id = c(1,2), #pull all sexes
                       age_group_id = all.ages, decomp_step = "step4") #pull only 15-69
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

# ---PREP TURNOVER------------------------------------------------------------------------------------------------------
#subset to this location
turnover <- turnover[region_name == this.region]

#interpolate years for the turnover file
#first add missing datapoints for the years in question
ot.cols <- names(turnover)[names(turnover) %like% 'OT']

duplicateYear <- function(this.year, dt) {

  message('duplicating for year=', this.year)

  out <- copy(dt[year == 1990])
  out[, year := this.year]

    #replace columns with missing
    out[, (ot.cols) := NA]

    return(out)

}

years <- (year_start:year_end)[(year_start:year_end) %ni% unique(turnover$year)] #create a vector of missing years
dupes <- lapply(years, duplicateYear, dt=turnover) %>% rbindlist
turnover <- list(turnover, dupes) %>% rbindlist

#now interpolate using the zoo package na.spline
turnover <- turnover[order(location_name, sex, duration, year)]
turnover[, (ot.cols) := lapply(.SD, na.spline), .SDcols=ot.cols, by=list(location_name, sex, duration)]

#now reshape
turnover <- melt(turnover, measure.vars = ot.cols, variable.name="age_var")
turnover[, age_start := substr(age_var, start=4, stop=5)]
turnover[, `All ages` := NULL]
turnover <- dcast(turnover, ...~duration)

#prep age ids in order to split over gbd groups
age.splits <- ages[, list(age_group_id, age_group_years_start)]
age.splits[, age_merge := as.numeric(substr(age_group_years_start, start=1, stop=1))]
age.splits[age_group_id == 9, age_merge := 20]
age.splits[age_group_id == 10, age_merge := 25]
age.splits[age_group_id > 30, age_merge := 8]
turnover[, age_merge := as.numeric(substr(age_start, start=1, stop=1))]
turnover[age_start == 20, age_merge := 20]
turnover[age_start == 25, age_merge := 25]

turnover <- merge(turnover, age.splits, by='age_merge', allow.cartesian=TRUE)
turnover <- turnover[, -c('age_merge', 'age_start', 'gbd_analytical_region_name')]
turnover[sex=="Male", sex_id := 1]
turnover[sex=="Female", sex_id := 2]
setnames(turnover, "year", "year_id")
#***********************************************************************************************************************

# ---ESTIMATE CARCINOGEN------------------------------------------------------------------------------------------------
#read in exposure rates by industry/carcinogen from CAREX regression
exposure <- file.path(exp.dir, 'exposure_rates.fst') %>% read.fst(as.data.table=T)
exposure <- exposure[location_id==country]
exposure[, c('sex_id', 'age_group_id') := NULL] #these are all sex/age rates

duplicateYear <- function(this.year, dt) {

  message('duplicating for year=', this.year)

  out <- copy(dt[year_id == min(dt$year_id)])
  out[, year_id := this.year]

  return(out)

}

years <- (year_start:year_end)[(year_start:year_end) %ni% unique(exposure$year)] #create a vector of missing years
dupes <- lapply(years, duplicateYear, dt=exposure) %>% rbindlist
exposure <- list(exposure, dupes) %>% rbindlist

#merge
all <- merge(employment, prop[, -c('age_group_id'), with=F], by=c('location_id', 'year_id', 'sex_id'), allow.cartesian=TRUE)
all <- merge(all, exposure, by=c('location_id', 'year_id', 'isic_code'), allow.cartesian=TRUE)

#calculate age/carcinogen/industry exposure as rate of exposure * industry % * employment rate for age
#this represent the CURRENT EXPOSURE
all[, (exp.cols) := lapply(1:draws.required, function(draw) exposure_rate * get(emp.cols[draw]) * get(prop.cols[draw]))]

#now collapse the isic_codes/ages, no longer need these levels
current <- all[, c('location_id', 'year_id', 'sex_id', 'ccode', 'carcinogen', exp.cols), with=F]
setkeyv(current, c('location_id', 'year_id', 'sex_id', 'ccode', 'carcinogen'))
current[, (exp.cols) := lapply(.SD, sum), .SDcols=exp.cols, by=key(current)]
current <- unique(current, by=key(current))

#now loop through the relevant ages to using custom backcasting fx to calc cumulative exposure for each of them
grid <- expand.grid(ages=relevant.ages, years=gbd.years)

past <- mcmapply(ageWrapper,
                 this.age=grid$ages,
                 current.year=grid$years,
                 MoreArgs=list(all.data=all,
                               this.function=sumCumulatoR,
                               cum.grid=expand.grid(sex=unique(all$sex_id), exp=unique(all$ccode))),
                 mc.cores = cores.provided,
                 SIMPLIFY=F) %>% rbindlist

rows <- lapply(gbd.years, function(x) nrow(past[year_id==x]))
#weird mclapply error drops some years when running at high cores. if failed, keep reducing the cores until you
#run all years successfully
new.cores <- cores.provided
while(length(unique(rows))!=1) {

  new.cores <- round(new.cores / 2, digits=0)

  message('multicore error...trying again with ', new.cores, ' cores!')

  past <- mcmapply(ageWrapper,
                   this.age=grid$ages,
                   current.year=grid$years,
                   MoreArgs=list(all.data=all,
                                 this.function=sumCumulatoR,
                                 cum.grid=expand.grid(sex=unique(all$sex_id), exp=unique(all$ccode))),
                   mc.cores = new.cores,
                   SIMPLIFY=F) %>% rbindlist

  rows <- lapply(gbd.years, function(x) nrow(past[year_id==x]))

}

#merge back on the full carcinogen names
past <- merge(past, all[, list(ccode, carcinogen)] %>% unique, by='ccode')

#merge turnover factors to the past exposure estimates
#then merge pop to weight
turnover <- merge(past, turnover, by=c('year_id', 'sex_id', 'age_group_id'))
turnover <- merge(pops, turnover, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'))

#convert exposure into workers using pop (to account for both pop composition by age and exposure),
#then sum the past exposure over the turnover age groups,
#then use the ratio of total exposure from the split age to summed age to split the turnover
turnover[, (exp.cols) := lapply(.SD, function(x) x * population), .SDcols=exp.cols]
turnover[, (sum.cols) := lapply(.SD, sum), .SDcols=exp.cols, by=c('ccode', 'year_id', 'sex_id', 'age_var')]

turnover[ccode %in% c("BENZ", "FORM"),  #these carcinogens have short duration
         (turnover.cols) := lapply(1:draws.required, function(draw) short * exp.cols[draw] %>% get / sum.cols[draw] %>% get)]
turnover[!(ccode %in% c("BENZ", "FORM")), #all others long duration = long turnover
         (turnover.cols) := lapply(1:draws.required, function(draw) long * exp.cols[draw] %>% get / sum.cols[draw] %>% get)]

#need to replace NaN values with 0, because some of the short term cancers will have a sum exposure of 0 and return NaN
turnover[short==0 & ccode %in% c("BENZ", "FORM"), (turnover.cols) := 0]
turnover <- turnover[, c('ccode', 'year_id', 'sex_id', 'age_group_id', turnover.cols), with=F]
current <- merge(current, turnover, by=c('ccode', 'year_id', 'sex_id'))

current[, (exp.cols) := lapply(1:draws.required, function(draw) exp.cols[draw] %>% get * turnover.cols[draw] %>% get)]

#cleanup for save results
current[, measure_id := 18] #now a proportional measure (% of pop exposed to carcinogen)
current[ccode=='PAH', carcinogen := 'pah'] #quick fix for the current carc subsetting (eventually move mes to match ccode)

#merge on the high/low exposure prevalences
# current <- merge(current, high.exposure, by=c('location_id', 'year_id'))
current <- merge(current, high.exposure, by='location_id')


#write draws for each carcinogen to dir
saveCarc <- function(this.carc, dt) {

  me.name <- paste0('occ_carcino_', this.carc)

    message('saving high/low exposure estimates for ', this.carc)

    high.dir <- file.path(draw.dir, 'occ_carcino', me.name, 'high', output.version)
      dir.create(high.dir, recursive = T)
    low.dir <- file.path(draw.dir, 'occ_carcino', me.name, 'low', output.version)
      dir.create(low.dir, recursive = T)

   #subset only relevant vars
   out <- dt[tolower(carcinogen) %like% this.carc,
                           c('location_id', 'year_id', 'age_group_id', 'sex_id', 'ccode', 'measure_id',
                             'high_exposure', 'low_exposure', exp.cols), with=F]
   setnames(out, exp.cols, draw.cols)
   out[, rei := me.name]

     for(rate in c('high', 'low')) {

         if (rate=='high') {

            rate.dt <- copy(out)
              rate.dt[, (draw.cols) := lapply(.SD, function(x) x * high_exposure), .SDcols=draw.cols]

            } else if (rate=='low') {
                  rate.dt <- copy(out)
                  rate.dt[, (draw.cols) := lapply(.SD, function(x) x * low_exposure), .SDcols=draw.cols]
            }

         write.csv(rate.dt, file.path(get(paste0(rate, '.dir')), paste0(country, '.csv')))

     }
   
   message(paste0("DONE! results saved in ", high.dir, " and ", low.dir))

     return(dim(out))

}

draws.str <- mclapply(carc.list, saveCarc, dt=current, mc.cores = cores.provided)

#calculate mean/CI to save summary table
current <- current[, exp_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=exp.cols]
current <- current[, exp_mean := rowMeans(.SD), .SDcols=exp.cols]
current <- current[, exp_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=exp.cols]

write.csv(current[, c('location_id', 'year_id', 'age_group_id', 'sex_id','ccode', 'measure_id',
                      names(current)[names(current) %like% 'lower|mean|upper']), with=F],
          file.path(summary.dir, paste0(country, '_carc.csv')))

message(paste0("DONE! summary results saved in ", summary.dir))
#***********************************************************************************************************************
