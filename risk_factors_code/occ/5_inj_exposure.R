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
             "9", #worker version
             "19", #output version
             "1000", #draws
             "5") #cores
  }
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
  arg <- c('492') #number of cores to provide to parallel functionss
  
}

# load packages
pacman::p_load(data.table, fst, gridExtra, ggplot2, lme4, magrittr, parallel, stringr, readxl)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

# Set parameters from input args
country   <- arg[1]
worker.version  <- arg[2]
output.version  <- arg[3]
draws.required  <- arg[4] %>% as.numeric
cores.provided  <- arg[5] %>% as.numeric

#set values for project
relevant.ages <- c(8:20, 30) #only ages 15-80 (85+ = 0 participation)

#colnames
draw.cols <- paste0('draw_', 0:(draws.required-1))
death.cols <- paste0('death_', 0:(draws.required-1))
worker.cols <- paste0('workers_', 0:(draws.required-1))
tot.cols <- paste0(draw.cols, '_total')
denom.cols <- paste0(draw.cols, '_denom')
squeezed.cols <- paste0(draw.cols, '_squeezed')
paf.cols <- paste0('paf_', 0:(draws.required-1))

#create a function and use it to follow just N draw(s) through the whole process
drawTracker <- function(varlist,
                        random.draws) {
  
  varlist[-random.draws] %>% return
  
}

subtract <- sapply(list(draw.cols, death.cols, worker.cols, squeezed.cols, tot.cols, denom.cols, paf.cols),
                   drawTracker,
                   random.draws=sample(draws.required, 3))

##in##
data.dir <- file.path("FILEPATH")
doc.dir <- file.path(home.dir, 'FILEPATH')
injury.causes <- file.path(doc.dir, 'Injuries', 'gbd2016_injury_cause_pairs.csv') %>% fread
isic.map <- file.path(doc.dir, 'classifications', 'ISIC_MAJOR_GROUPS_BY_REV.xlsx')
isic.3.map <- read_xlsx(isic.map, sheet = "ISIC_REV_3_1") %>% as.data.table
gpr.dir <- file.path(j_root, 'FILEPATH')
worker.dir <- file.path('FILEPATH', worker.version)

##out##
##out##
summary.dir <- file.path(home.dir, "FILEPATH", output.version)
exp.dir <- file.path('FILEPATH', output.version)
paf.dir <- file.path('FILEPATH', output.version)
lapply(c(summary.dir, exp.dir, paf.dir), dir.create, recursive=T)
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

#function to check variables of a merge and see which were dropped
checkMerge <- function(var, old, new) {
  
  old[, get(var) %>% unique][old[, get(var) %>% unique] %ni%
                               new[, get(var) %>% unique]] %>% return
  
}

#files#
#specify the function to define file to hold emp.occ/industry draws
fileName <- function(dir,
                     loc,
                     value,
                     suffix) {
  
  file.path(dir, paste0(loc, '_', value, suffix)) %>% return
  
}
#***********************************************************************************************************************

# ---PREP INJURY DRAWS--------------------------------------------------------------------------------------------------
#read in model db to populate the run IDs
run.db <- file.path(gpr.dir, 'run_db.csv') %>% fread
best.ind <- run.db[me_name %like% 'occ_inj_major_', list('run_id'=max(run_id)), by='me_name']
best.ind <- best.ind[!(me_name %like% "TOTAL_NA")] 

bindCategory <- function(x) {
  
  run <- best.ind[x, run_id]
  name <- best.ind[x, me_name]
  
  message("reading in draws for: ", name)
  
  in.dir <- file.path(data.dir, run, 'draws_temp_1')
  
  cat <- file.path(in.dir, paste0(country, '.csv')) %>%
    fread
  
  cat[, isic_code := substr(name, 15, 15)]
  
}

#append all industry models
dt <- lapply(1:nrow(best.ind), bindCategory) %>%
  rbindlist %>%
  setkeyv(c('location_id', 'year_id', 'age_group_id', 'sex_id'))

#split off the totals to use as the denominator when squeezing
totals <- dt[isic_code == "T"]
setnames(totals, draw.cols, denom.cols)

#read in best version of workers per industry
workers <- fileName(worker.dir, country, 'ind', '_workers.fst') %>% read.fst(as.data.table=T)
workers[, isic_code := substr(me_name, 15, 15)]

#merge workers to your injury rates and total injury rates
all <- merge(dt[, -c('age_group_id', 'measure_id'), with=F], workers[, -c('me_name'), with=F],
             by=c('location_id', 'year_id', 'sex_id', 'isic_code'), allow.cartesian=T)

#merge workers to your injury rates and total injury rates
all <- merge(totals[, -c('age_group_id', 'measure_id', 'isic_code'), with=F], all,
             by=c('location_id', 'year_id', 'sex_id'), allow.cartesian=T)

#reset key
setkeyv(all, c('location_id', 'year_id', 'age_group_id', 'sex_id'))

#convert injury rates per 100k to # of deaths
all[, (death.cols) := lapply(1:draws.required, function(draw) get(draw.cols[draw]) / 1e5 * get(worker.cols[draw]))]

#repeat for totals
all[, (denom.cols) := lapply(1:draws.required, function(draw) get(denom.cols[draw]) / 1e5 * get(worker.cols[draw]))]

#then squeeze using a scalar that is the sum of the industry specific draws/ the sum of the denom draws using total rate
all[, (tot.cols) := lapply(.SD, sum), .SDcols=death.cols, by=key(all)] #calculate totals
all[, (denom.cols) := lapply(.SD, sum), .SDcols=denom.cols, by=key(all)] #calculate denom
all[, (squeezed.cols) := lapply(1:draws.required, function(draw) get(death.cols[draw]) * get(denom.cols[draw]) / get(tot.cols[draw]))]

#need to replace NaN values with 0, because age groups with no economic participation will have a sum exposure of 0
#so they return NaN when squeezing
all[age_group_id %ni% relevant.ages, (squeezed.cols) := 0] #set deaths# as 0 outside of ages 15-85

#calculate avg total and then remove total columns
all[, total_mean := rowMeans(.SD), .SDcols=tot.cols]
all[, (tot.cols) := NULL]

#calculate mean/CI for # of deaths in industry (raw)
all[, deaths_raw_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=death.cols]
all[, deaths_raw_mean := rowMeans(.SD), .SDcols=death.cols]
all[, deaths_raw_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=death.cols]

#calculate mean/CI for # of deaths in industry (squeezed)
all[, deaths_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=squeezed.cols]
all[, deaths_mean := rowMeans(.SD), .SDcols=squeezed.cols]
all[, deaths_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=squeezed.cols]

all[, squeeze_factor := deaths_raw_mean/deaths_mean] #estimate how much each industry is being squeezed

out <- all[, -c(draw.cols, death.cols, denom.cols, worker.cols), with=F]
setnames(out, squeezed.cols, draw.cols)

#now save the deaths draw values, then save the summary vals for review
write.fst(out,
          path=fileName(exp.dir, country, 'inj', '.fst'))
write.csv(out[, -draw.cols, with=F],
          fileName(summary.dir, country, 'inj', '.csv'))
#***********************************************************************************************************************

# ---SCRAP--------------------------------------------------------------------------------------------------------------

#***********************************************************************************************************************