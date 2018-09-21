# ---HEADER-------------------------------------------------------------------------------------------------------------
# Project: OCC - OCC
# Purpose: Prep fatal injury rates for modelling (by economic activities)

#********************************************************************************************************************************

# ---CONFIG----------------------------------------------------------------------------------------------------------------------
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
             "9", #output version
             "1000", #draws
             "5") #cores
  }
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
  arg <- c("151", #location
           "7", #output version
           "1000", #draws
           "5") #cores
  
}

# load packages
pacman::p_load(data.table, fst, gridExtra, ggplot2, lme4, magrittr, parallel, stringr, readxl, zoo)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

# Set parameters from input args
country   <- arg[1]
output.version  <- arg[2]
draws.required  <- arg[3] %>% as.numeric
cores.provided  <- arg[4] %>% as.numeric

#set values for project
location_set_version_id <- 149
year_start <- 1970
year_end <- 2016
all.ages <- c(2:20, 30, 31, 32, 235)
relevant.ages <- c(8:20, 30) #only ages 15-85

#colnames
draw.cols <- paste0('draw_', 0:(draws.required-1))
prop.cols <- paste0('prop_', 0:(draws.required-1))
worker.cols <- paste0('workers_', 0:(draws.required-1))
tot.cols <- paste0(draw.cols, '_total')


##in##
data.dir <- file.path("FILEPATH")
doc.dir <- file.path(home.dir, 'FILEPATH')
isic.map <- file.path(doc.dir, 'FILEPATH')
isic.3.map <- read_xlsx(isic.map, sheet = "ISIC_REV_3_1") %>% as.data.table
gpr.dir <- file.path(j_root, 'FILEPATH')

##out##
#dirs#
summary.dir <- file.path(home.dir, "FILEPATH", output.version)
draw.dir <- file.path('FILEPATH', output.version)
lapply(c(draw.dir, summary.dir), dir.create, recursive=T)

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

subtract <- sapply(list(draw.cols, worker.cols, prop.cols, tot.cols),
                   drawTracker,
                   random.draws=sample(draws.required, 3))

squeezeR <- function(models,
                     occ.type,
                     draw.dir,
                     summary.dir) {
  
  # country fx loop
  bindCategory <- function(x) {
    
    run <- models[x, run_id]
    name <- models[x, me_name]
    
    message("reading in draws for: ", name)
    
    in.dir <- file.path(data.dir, run, 'draws_temp_1')
    
    cat <- file.path(in.dir, paste0(country, '.csv')) %>%
      fread
    
    cat[, me_name := name]
    
    #some models depend on covariates that only go back to 1980. If this is the case, use splines to backcast
    #splines ended up being too unstable to extrapolate, just assuming no trend before 1980 for now
    #other possible options include loess, gam, or linear model
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
  write.fst(dt[, -c(draw.cols, 'measure_id'), with=F],
            path=fileName(draw.dir, country, occ.type, '_prop.fst'))
  
  #now save the squeezed proportion values/summaries
  write.csv(dt[, -c(draw.cols, prop.cols), with=F],
            fileName(summary.dir, country, occ.type, '_prop.csv'))
  
  #merge workers to your data
  dt <- merge(dt[, -c('age_group_id', 'measure_id'), with=F], workers,
              by=c('location_id', 'year_id', 'sex_id'), allow.cartesian=T)
  
  #calculate the # of active workers in each industry
  dt[, (worker.cols) := lapply(1:draws.required, function(draw) get(prop.cols[draw]) * get(worker.cols[draw]))]
  
  #calculate mean/CI for # of workers in industry
  dt[, workers_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=worker.cols]
  dt[, workers_mean := rowMeans(.SD), .SDcols=worker.cols]
  dt[, workers_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=worker.cols]
  
  #now save the workers draw values, then save the summary vals for review
  write.fst(dt[, -c(draw.cols, prop.cols, 'process_version_map_id', 'measure_id'), with=F],
            path=fileName(draw.dir, country, occ.type, '_workers.fst'))
  write.csv(dt[, -c(draw.cols, worker.cols, prop.cols), with=F],
            fileName(summary.dir, country, occ.type, '_workers.csv'))
  
  #return just the workers per id.var
  temp <- dt[, list(location_id, year_id, sex_id, age_group_id, me_name, workers_mean)]
  
  #calculate the workers for all ages/all sexes to use as a denominator when age sex splitting
  both.sex <- copy(temp)
  both.sex[, workers_mean := sum(workers_mean), by=list(location_id, age_group_id, year_id, me_name)]
  both.sex <- unique(both.sex, by=c('location_id', 'year_id', 'age_group_id', 'me_name'))
  both.sex[, sex_id := 3]
  
  #now add the pop group total
  all.age <- list(temp, both.sex) %>% rbindlist
  all.age[, workers_mean := sum(workers_mean), by=list(location_id, year_id, sex_id, me_name)]
  all.age <- unique(all.age, by=c('location_id', 'year_id', 'sex_id', 'me_name'))
  all.age[, age_group_id := 201] #age group id for 15-69 aggregate
  workers.mean <- list(all.age, both.sex, temp) %>% rbindlist
  workers.mean[, isic_code := substr(me_name, 15, 15)]
  
  write.csv(workers.mean, fileName(summary.dir, country, occ.type, '_workers_mean.csv'))
  
}
#***********************************************************************************************************************

# ---PREP POPULATION----------------------------------------------------------------------------------------------------
#merge on levels
locs <- get_location_hierarchy(location_set_version_id)
locations <- locs[is_estimate==1, unique(location_id)]
levels <- locs[,grep("level|location_id|ihme_loc_id", names(locs)), with=F]

#also pull the age group information
ages <- get_ages()

## then get pop
#read in population using central fx
pops <- get_population(location_set_version_id=location_set_version_id,
                       location_id = country,
                       year_id = c(year_start:year_end),
                       sex_id = c(1,2), #pull all sexes
                       age_group_id = all.ages) #pull only 15-69

#now add the pop group total
total.pop <- pops[age_group_id %in% relevant.ages]
total.pop[, population := sum(population), by=list(location_id, year_id, sex_id)]
totals <- unique(total.pop, by=c('location_id', 'year_id', 'sex_id'))
totals[, c('age_short', 'age_group_id') := list("TOTAL", 201)] #age gorup id for 15-69 aggregate
#***********************************************************************************************************************

# ---PREP EMPLOYMENT DRAWS------------------------------------------------------------------------------------------------
#read in model db to populate the run IDs
run.db <- file.path(gpr.dir, 'run_db.csv') %>% fread
best.emp <- run.db[me_name %like% 'employment_ratio', list('run_id'=max(run_id)), by='me_name']

message("reading in draws for: ", best.emp[, me_name])

#read in country specific employment results from st-gpr
dt <- file.path(data.dir, best.emp[, run_id], 'draws_temp_1', paste0(country, '.csv')) %>%
  fread %>%
  setkeyv(c('location_id', 'year_id', 'age_group_id', 'sex_id'))

#coastal prop only go back to 1980
#splines ended up being too unstable to extrapolate, just assuming no trend before 1980 for now
#other possible options include loess, gam, or linear model
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

#now save the employment prop/workers draw values, then save the summary vals for review
write.fst(dt[, -c(worker.cols, 'process_version_map_id', 'measure_id'), with=F],
          path=fileName(draw.dir, country, 'emp', '_prop.fst'))
write.fst(dt[, -c(draw.cols, 'process_version_map_id', 'measure_id'), with=F],
          path=fileName(draw.dir, country, 'emp', '_workers.fst'))

write.csv(dt[, -c(draw.cols, worker.cols), with=F], fileName(summary.dir, country, 'emp', '.csv'))

#save workers for next step
workers <- copy(dt[, -draw.cols, with=F])

#return just the workers per id.var
temp <- dt[, list(location_id, year_id, sex_id, age_group_id, workers_mean)]

#calculate the workers for all ages/all sexes to use as a denominator when age sex splitting
both.sex <- copy(temp)
both.sex[, workers_mean := sum(workers_mean), by=list(location_id, age_group_id, year_id)]
both.sex <- unique(both.sex, by=c('location_id', 'year_id', 'age_group_id'))
both.sex[, sex_id := 3]

#now add the pop group total
all.age <- list(temp, both.sex) %>% rbindlist
all.age[, workers_mean := sum(workers_mean), by=list(location_id, year_id, sex_id)]
all.age <- unique(all.age, by=c('location_id', 'year_id', 'sex_id'))
all.age[, age_group_id := 201] #age group id for 15-69 aggregate
employment <- list(all.age, both.sex, temp) %>% rbindlist

write.csv(employment, fileName(summary.dir, country, 'emp', '_mean.csv'))

#***********************************************************************************************************************

# ---PREP INDUSTRY DRAWS------------------------------------------------------------------------------------------------
#read in model db to populate the run IDs
models <- run.db[me_name %like% 'occ_ind_major_', list('run_id'=max(run_id)), by='me_name']

squeezeR(models, occ.type='ind', draw.dir=draw.dir, summary.dir=summary.dir)

#***********************************************************************************************************************

# ---PREP OCCUPATIONAL DRAWS--------------------------------------------------------------------------------------------
#read in model db to populate the run IDs
models <- run.db[me_name %like% 'occ_occ_major_', list('run_id'=max(run_id)), by='me_name']

squeezeR(models, occ.type='occ', draw.dir=draw.dir, summary.dir=summary.dir)

#***********************************************************************************************************************

# ---SCRAP--------------------------------------------------------------------------------------------------------------

#
#   # Now loop over the different levels, merge the square, fill in study level covs, and save a csv
#   saveData <- function(cat, dt) {
#
#     message("saving -> ", cat)
#
#     cat.dt <- dt[isic_code==cat]
#
#     this.entity <- ifelse(cat!="TOTAL",
#                           paste("occ_inj_major", cat, cat.dt[1, major_label_me], sep = "_"),
#                           "occ_inj_major_TOTAL")
#
#     cat.dt[, me_name := this.entity]
#
#     fwrite(cat.dt, file=file.path(out.dir, paste(this.entity, ".csv", sep = "")))
#
#     return(cat.dt)
#
#   }
#
#   list <- lapply(dt[, isic_code] %>% unique, saveData, dt=dt)
#***********************************************************************************************************************