# ---HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: calculate # of workers (total, and in each occupation/industry using squeezed model outputs)
# source("FILEPATH.R", echo=T)
#********************************************************************************************************************************

# ---CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# load packages
pacman::p_load(data.table, gridExtra, ggplot2, lme4, magrittr, parallel, stringr, zoo, openxlsx)
library(fst, lib.loc = "FILEPATH")

# set working directories
home.dir <- "FILEPATH"
setwd(home.dir)

# Set parameters from input args
arg <- commandArgs(trailingOnly = T)
country <- arg[1]
output.version <- arg[2]
draws.required <- arg[3] %>% as.numeric
calc.occind <- arg[4] %>% as.numeric
decomp_step <- arg[5]
cores.provided <- arg[6] %>% as.numeric

print(country)
print(output.version)
print(draws.required)
print(calc.occind)
print(decomp_step)
print(cores.provided)

#set values for project
year_start <- 1970
year_end <- 2019
all.ages <- c(2:20, 30, 31, 32, 235)
relevant.ages <- c(8:20, 30) #only ages 15-85

#colnames
draw.cols <- paste0('draw_', 0:(draws.required-1))
prop.cols <- paste0('prop_', 0:(draws.required-1))
worker.cols <- paste0('workers_', 0:(draws.required-1))
tot.cols <- paste0(draw.cols, '_total')


##in##
data.dir <- "FILEPATH"
doc.dir <- "FILEPATH"
  isic.map <- "FILEPATH"
  isic.3.map <- read.xlsx(isic.map, sheet = "ISIC_REV_3_1") %>% as.data.table
bundles <- fread("FILEPATH")

##out##
#dirs#
summary.dir <- file.path("FILEPATH", output.version)
draw.dir <- file.path("FILEPATH", output.version)
if(!dir.exists(summary.dir)) dir.create(summary.dir, recursive = TRUE)
if(!dir.exists(draw.dir)) dir.create(draw.dir, recursive = TRUE)

#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
ubcov.function.dir <- "FILEPATH"
file.path(ubcov.function.dir, "FILEPATH.r") %>% source
# central functions
file.path("FUNCTION") %>% source
file.path("FUNCTION") %>% source
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

#create a function and use it to follow just N draw(s) through the whole process
drawTracker <- function(varlist,
                        random.draws) {

  varlist[-random.draws] %>% return

}

squeezeR <- function(models,
                     occ.type,
                     draw.dir,
                     summary.dir) {

  # country fx loop
  bindCategory <- function(x) {

    run <- models[x, run_id]
    name <- models[x, me_name]

    message("reading in draws for: ", name)

    in.dir <- file.path(data.dir, run, 'draws_temp_0')

    cat <- file.path(in.dir, paste0(country, '.csv')) %>% fread

    cat[, me_name := name]

    #some models depend on covariates that only go back to 1980
    if (min(cat$year_id)>year_start) {

      duplicateYear <- function(this.year, dt) {

        message('duplicating for year=', this.year)
        out <- copy(dt[year_id == min(cat$year_id)])
        out[, year_id := this.year]

        return(out)

      }

      years <- (year_start:year_end)[(year_start:year_end) %ni% unique(cat$year_id)] #create a vector of missing years
      dupes <- lapply(years, duplicateYear, dt=cat) %>% rbindlist
      cat <- list(cat, dupes) %>% rbindlist

    }

    return(cat)

  }

  #append all industry models
  dt <- mclapply(1:nrow(models), bindCategory, mc.cores=cores.provided) %>%
    rbindlist %>%
    setkeyv(c('location_id', 'year_id', 'age_group_id', 'sex_id'))


  #squeeze to 1
  dt[, (tot.cols) := lapply(.SD, sum), .SDcols=draw.cols, by=key(dt)] #calculate denominator
  dt[, (prop.cols) := lapply(1:draws.required, function(draw) get(draw.cols[draw]) / get(tot.cols[draw]))] #then squeeze

  #calculate avg total and then remove total columns
  dt[, total_mean := rowMeans(.SD), .SDcols=tot.cols]
  dt[, (tot.cols) := NULL]

  #calculate mean/CI for % of workers in industry
  dt[, prop_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=prop.cols]
  dt[, prop_mean := rowMeans(.SD), .SDcols=prop.cols]
  dt[, prop_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=prop.cols]

  #also calculate the raw %s and the average squeeze factor for comparison
  dt[, raw_prop_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=draw.cols]
  dt[, raw_prop_mean := rowMeans(.SD), .SDcols=draw.cols]
  dt[, raw_prop_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=draw.cols]
  dt[, avg_squeeze := raw_prop_mean / prop_mean]

  #now save the workers draw values, then save the summary vals for review
  write.fst(dt[, -c(draw.cols), with=F],
          path=fileName(draw.dir, country, occ.type, '_prop.fst'))

  #now save the squeezed proportion values/summaries
  write.csv(dt[, -c(draw.cols, prop.cols), with=F],
            fileName(summary.dir, country, occ.type, '_prop.csv'), row.names=F)

  #merge workers to your data
  dt <- merge(dt[, -c('age_group_id'), with=F], workers,
              by=c('location_id', 'year_id', 'sex_id'), allow.cartesian=T)

  #calculate the # of active workers in each industry
  dt[, (worker.cols) := lapply(1:draws.required, function(draw) get(prop.cols[draw]) * get(worker.cols[draw]))]

  #calculate mean/CI for # of workers in industry
  dt[, workers_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=worker.cols]
  dt[, workers_mean := rowMeans(.SD), .SDcols=worker.cols]
  dt[, workers_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=worker.cols]

  #now save the workers draw values, then save the summary vals for review
  write.fst(dt[, -c(draw.cols, prop.cols, 'run_id'), with=F],
          path=fileName(draw.dir, country, occ.type, '_workers.fst'))
  write.csv(dt[, -c(draw.cols, worker.cols, prop.cols), with=F],
            fileName(summary.dir, country, occ.type, '_workers.csv'), row.names=F)

  #return just the workers per id.var
  temp <- dt[, list(location_id, year_id, sex_id, age_group_id, me_name, workers_mean)]

  #calculate the workers for all ages/all sexes to use as a denominator when age sex splitting
  both.sex <- copy(temp)
  both.sex[, workers_mean := sum(workers_mean), by=list(location_id, age_group_id, year_id, me_name)]
  both.sex <- unique(both.sex, by=c('location_id', 'year_id', 'age_group_id', 'me_name'))
  both.sex[, sex_id := 3]

  #now add the pop group total
  all.age <- list(temp, both.sex) %>% rbindlist
  all.age <- all.age[age_group_id < 19]
  all.age[, workers_mean := sum(workers_mean), by=list(location_id, year_id, sex_id, me_name)]
  all.age <- unique(all.age, by=c('location_id', 'year_id', 'sex_id', 'me_name'))
  all.age[, age_group_id := 201] #age group id for 15-69 aggregate
  workers.mean <- list(all.age, both.sex, temp) %>% rbindlist
  if (occ.type == "ind") workers.mean[, isic_code := substr(me_name, 15, 15)]
  if (occ.type == "occ") workers.mean[, isco_code := substr(me_name, 15, 15)]

  write.csv(workers.mean, fileName(summary.dir, country, occ.type, '_workers_mean.csv'), row.names=F)

}
#***********************************************************************************************************************

# ---PREP POPULATION----------------------------------------------------------------------------------------------------
source("FUNCTION")
source("FUNCTION")

#merge on levels
locs <- get_location_metadata(gbd_round_id = 6, location_set_id = 22)
locations <- locs[is_estimate==1, unique(location_id)]
levels <- locs[,grep("level|location_id|ihme_loc_id", names(locs)), with=F]

#also pull the age group information
ages <- get_ids("age_group")[age_group_id %in% relevant.ages]
ages[age_group_id == 235, age_group_name := "95 to 99"]
ages[,c("age_group_years_start","to","age_group_years_end") := tstrsplit(age_group_name," ")]
ages[,c("age_group_years_start","age_group_years_end") := lapply(.SD,as.numeric), .SDcols = c("age_group_years_start","age_group_years_end")]

## then get pop
#read in population using central fx
pops <- get_population(location_set_id = 22,
                       location_id = country,
                       year_id = c(year_start:year_end),
                       sex_id = c(1,2), #pull all sexes
                       age_group_id = all.ages,
                       decomp_step = decomp_step)

#now add the pop group total
total.pop <- pops[age_group_id %in% relevant.ages]
total.pop[, population := sum(population), by=list(location_id, year_id, sex_id)]
totals <- unique(total.pop, by=c('location_id', 'year_id', 'sex_id'))
totals[, c('age_short', 'age_group_id') := list("TOTAL", 201)] # age group id for 15-69 aggregate
#***********************************************************************************************************************

# ---PREP EMPLOYMENT DRAWS------------------------------------------------------------------------------------------------
recent_runs <- read.xlsx("FILEPATH") %>% data.table
recent_runs <- recent_runs[get(paste0(decomp_step, "_best")) == 1 & me_name %like% "occ", .(me_name, run_id)]

best.emp <- recent_runs[me_name == "occ_employment_ratio"]

message("reading in draws for: ", best.emp[, me_name] %>% unique)

dt <- fread(file.path(data.dir, best.emp[1, run_id], 'draws_temp_0', paste0(country, '.csv')))
setkeyv(dt, c('location_id', 'year_id', 'age_group_id', 'sex_id'))

#coastal prop only go back to 1980
if (min(dt$year_id)>year_start) {

  duplicateYear <- function(this.year, dt) {

    message('duplicating for year=', this.year)
    out <- copy(dt[year_id == min(dt$year_id)])
    out[, year_id := this.year]

    return(out)

  }

  years <- (year_start:year_end)[(year_start:year_end) %ni% unique(dt$year_id)] #create a vector of missing years
  dupes <- lapply(years, duplicateYear, dt=dt) %>% rbindlist
  dt <- list(dt, dupes) %>% rbindlist

}

#extrapolate for ages 70-85 using the modelled age trend and assumption of retirement at 85
#need to add on the age starts to make a more interpretable x axis
dt <- merge(dt, ages[, list(age_group_id, age_group_name)], by='age_group_id')
dt[, age := substr(age_group_name, start=1, stop=2) %>% as.numeric]

#first add a datapoint at age 85 where employment is forced to be 0
#then add missing datapoints for ages 70-85, whereupon we will interpolate
duplicateAge <- function(this.age, dt) {

  message('duplicating for age=', this.age)
  out <- copy(dt[age == 65])
  out[, age := this.age]

  if (this.age!= 85) {
    out[, (draw.cols) := NA]
  } else {
    out[, (draw.cols) := 0]
  }

  return(out)

}

dupes <- lapply(seq(70, 85, 5), duplicateAge, dt=dt) %>% rbindlist
dt <- list(dt, dupes) %>% rbindlist

#now interpolate using the zoo package na.spline
dt <- dt[order(location_id, year_id, sex_id, age)]
dt[, (draw.cols) := lapply(.SD, function(x) na.approx(x, na.rm=F)), .SDcols=draw.cols,
   by=list(location_id, year_id, sex_id)]

#fix the age group ids
dt[, c('age_group_id', 'age_group_name') := NULL]
dt <- merge(dt, ages[, list(age_group_id, age_group_years_start)], by.x="age", by.y="age_group_years_start")
dt[, age := NULL]

#merge with pop and calculate # of workers
dt <- merge(dt, pops, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'), all.y=TRUE)
dt[age_group_id %ni% relevant.ages, (draw.cols) := 0] #set employment% as 0 outside of ages 15-85
dt[, (worker.cols) := lapply(.SD, function(x) x * population), .SDcols=draw.cols]

#calculate mean/CI for ratio
dt[, employment_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=draw.cols]
dt[, employment_mean := rowMeans(.SD), .SDcols=draw.cols]
dt[, employment_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=draw.cols]

#calculate mean/CI for # of workers in industry
dt[, workers_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=worker.cols]
dt[, workers_mean := rowMeans(.SD), .SDcols=worker.cols]
dt[, workers_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=worker.cols]

#save workers for next step
workers <- copy(dt[, -draw.cols, with=F])

if (calc.occind == 0){ ## writing output for total employment to be used in 2_prep_ind.R
  #now save the employment prop/workers draw values, then save the summary vals for review
  write.fst(dt[, -c(worker.cols, 'run_id'), with=F],
          path=fileName(draw.dir, country, 'emp', '_prop.fst'))
  write.fst(dt[, -c(draw.cols, 'run_id'), with=F],
          path=fileName(draw.dir, country, 'emp', '_workers.fst'))

  write.csv(dt[, -c(draw.cols, worker.cols), with=F], fileName(summary.dir, country, 'emp', '.csv'), row.names=F)

  #return just the workers per id.var
  temp <- dt[, list(location_id, year_id, sex_id, age_group_id, workers_mean)]

  #calculate the workers for all ages/all sexes to use as a denominator when age sex splitting
  both.sex <- copy(temp)
  both.sex[, workers_mean := sum(workers_mean), by=list(location_id, age_group_id, year_id)]
  both.sex <- unique(both.sex, by=c('location_id', 'year_id', 'age_group_id'))
  both.sex[, sex_id := 3]

  #now add the pop group total
  all.age <- list(temp, both.sex) %>% rbindlist
  all.age <- all.age[age_group_id < 19]
  all.age[, workers_mean := sum(workers_mean), by=list(location_id, year_id, sex_id)]
  all.age <- unique(all.age, by=c('location_id', 'year_id', 'sex_id'))
  all.age[, age_group_id := 201] #age group id for 15-69 aggregate
  employment <- list(all.age, both.sex, temp) %>% rbindlist

  write.csv(employment, fileName(summary.dir, country, 'emp', '_mean.csv'), row.names=F)
}

#***********************************************************************************************************************
if (calc.occind == 1){ ## writing industry and occupation-specific workers to be used by 2_prep_inj.R etc
  # ---PREP INDUSTRY DRAWS------------------------------------------------------------------------------------------------
  models <- recent_runs[me_name %like% 'occ_ind_major_', .(me_name,run_id)]

  squeezeR(models, occ.type='ind', draw.dir=draw.dir, summary.dir=summary.dir)

  #***********************************************************************************************************************

  # ---PREP OCCUPATIONAL DRAWS--------------------------------------------------------------------------------------------
  #read in model db to populate the run IDs
  models <- recent_runs[me_name %like% 'occ_occ_major_', .(me_name,run_id)]

  squeezeR(models, occ.type='occ', draw.dir=draw.dir, summary.dir=summary.dir)
}
