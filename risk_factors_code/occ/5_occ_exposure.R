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
    arg <- c("4626", #location
             "8", #worker version
             "18", #output version
             "1000", #draws
             "10") #cores
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
location_set_version_id <- 149
relevant.ages <- c(8:20, 30) #only ages 15-85
year_start <- 1970
year_end <- 2016

#colnames
id.vars <- c('location_id', 'year_id', 'isic_code', 'sex_id', 'age_group_id')
draw.cols <- paste0('draw_', 0:(draws.required-1))
exp.cols <- paste0('exp_', 0:(draws.required-1))
worker.cols <- paste0('workers_', 0:(draws.required-1))

##in##
cw.dir <- file.path(home.dir, "FILEPATH")
isco.map <- file.path(cw.dir, "ISCO_LVL1_CW.csv") %>% fread
exp.dir <- file.path(home.dir, "FILEPATH")
doc.dir <- file.path(home.dir, 'FILEPATH')
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
  
  old[, get(var) %>% unique][old[, get(var) %>% unique] %ni%
                               new[, get(var) %>% unique]] %>% return
  
}

#write draws for each exposure to dir
saveME <- function(me.name,
                   parent.exp,
                   exp.dt) {
  
  out.dir <- file.path(draw.dir, parent.exp, me.name, output.version)
  dir.create(out.dir, recursive = T)
  
  #subset only relevant vars
  out <- exp.dt[exposure_me == me.name,
                c(key(exp.dt), 'measure_id', exp.cols), with=F]
  
  #format draws for save_results
  setnames(out, exp.cols, draw.cols)
  
  #set rei
  out[, rei := parent.exp]
  
  #save
  write.csv(out, file.path(out.dir, paste0(country, '.csv')))
  
  dim(out) %>% return
  
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

#***********************************************************************************************************************

# ---PREP EMPLOYMENT DRAWS----------------------------------------------------------------------------------------------
#read in best version of workers per occ
#now save the workers draw values, then save the summary vals for review
workers <- fileName(worker.dir, country, 'occ', '_workers.fst') %>% read.fst(as.data.table=T)
workers[, isco_code := substr(me_name, 15, 15) %>% as.numeric]

#read in best version of workers per industry
#now save the workers draw values, then save the summary vals for review
workers.ind <- fileName(worker.dir, country, 'ind', '_workers.fst') %>% read.fst(as.data.table=T)
workers.ind[, isic_code := substr(me_name, 15, 15)]
#***********************************************************************************************************************

# ---EXPOSURE PREP------------------------------------------------------------------------------------------------------
#first prep the crosswalking document
isco.map <- isco.map[version=="ISCO68"]

#prep the 68 me names on in order to prep for backpain collapse
setnames(isco.map, c('final', 'me_name'), c('isco_code', 'backpain_me'))

#remove categories that don't contribute to backpain
backpain.map <-  isco.map[isco_code %ni% c(0, 99)]
#***********************************************************************************************************************

# ---ESTIMATE ERGO------------------------------------------------------------------------------------------------------
#calculate industry exposure to backpain as # of workers in industry
dt <- merge(workers, backpain.map[, list(isco_code, backpain_me)], by='isco_code')
setnames(dt, 'backpain_me', 'exposure_me')

#now collapse the isic_codes, no longer need all levels
setkeyv(dt, c('location_id', 'year_id', 'sex_id', 'age_group_id', 'exposure_me'))
dt[, (worker.cols) := lapply(.SD, sum), .SDcols=worker.cols, by=key(dt)]
dt <- unique(dt, by=key(dt))

#now calculate industry exposure as rate of exposure * # of workers in industry / total population
dt[, (exp.cols) := lapply(.SD, function(x) x / population), .SDcols=worker.cols]
dt[, measure_id := 18] #now a proportional measure (% of pop exposed to backpain)

#save csvs of results using custom fx
draws.str <- mclapply(unique(dt$exposure_me), saveME,
                      parent.exp = 'occ_backpain', exp.dt = dt,
                      mc.cores = cores.provided)

#calculate mean/CI to save summary table
dt <- dt[, worker_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=worker.cols]
dt <- dt[, worker_mean := rowMeans(.SD), .SDcols=worker.cols]
dt <- dt[, worker_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=worker.cols]
dt <- dt[, exp_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=exp.cols]
dt <- dt[, exp_mean := rowMeans(.SD), .SDcols=exp.cols]
dt <- dt[, exp_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=exp.cols]

write.csv(dt[, c(key(dt), 'exposure_me', 'measure_id', names(dt)[names(dt) %like% 'lower|mean|upper']), with=F],
          file.path(summary.dir, paste0(country, '_ergonomics.csv')))

#***********************************************************************************************************************

# ---ESTIMATE ASTHMAGENS------------------------------------------------------------------------------------------------
#combine the occupations (and in some cases industries) in order to match the RRs
dt <- copy(workers)
dt[isco_code %in% c(1, 4), exposure_me := 'admin']
dt[isco_code %in% c(2, 3), exposure_me := 'technical']
dt[isco_code==6, exposure_me := 'agriculture'] #note this is SKILLED agri

#drop other occs
dt <- dt[!is.na(exposure_me)]

#now collapse the isco_codes, no longer need all levels
setkeyv(dt, c('location_id', 'year_id', 'sex_id', 'age_group_id', 'exposure_me'))
dt[, (worker.cols) := lapply(.SD, sum), .SDcols=worker.cols, by=key(dt)]
asthma.occs <- unique(dt, by=key(dt))

#the other ones dont map well to our occ variables, so using industries instead
#tried to match them up using the child categories in the 2002 RR paper
dt <- copy(workers.ind)
#these 3 seemed to fit quite well
dt[isic_code == "C", exposure_me := 'mining']
dt[isic_code == "I", exposure_me := 'transport']
dt[isic_code == "D", exposure_me := 'manufacturing']
#for services, currently using H=hospitality, L=public administration/defence, O=other community/personal service, P=private houseolds
dt[isic_code %in% c("H", "L", "O", "P"), exposure_me := 'services']
#for sales, using wholesale and retail trade/repair
dt[isic_code == "G", exposure_me := 'sales']

#drop other inds
dt <- dt[!is.na(exposure_me)]

#now collapse the isic_codes, no longer need all levels
setkeyv(dt, c('location_id', 'year_id', 'sex_id', 'age_group_id', 'exposure_me'))
dt[, (worker.cols) := lapply(.SD, sum), .SDcols=worker.cols, by=key(dt)]
asthma.inds <- unique(dt, by=key(dt))

#combine the 2 datasets
dt <- list(asthma.occs, asthma.inds) %>%
  rbindlist %>%
  setkeyv(key(asthma.occs)) #rbindlist removes the key

#now calculate pop exposure as rate of exposure * # of workers in industry / total population
dt[, (exp.cols) := lapply(.SD, function(x) x / population), .SDcols=worker.cols]
dt[, measure_id := 18] #now a proportional measure (% of pop exposed to asthmagens)

#save csvs of results using custom fx
draws.str <- mclapply(unique(dt$exposure_me), saveME,
                      parent.exp = 'occ_asthmagens', exp.dt = dt,
                      mc.cores = cores.provided)

#calculate mean/CI to save summary table
dt <- dt[, worker_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=worker.cols]
dt <- dt[, worker_mean := rowMeans(.SD), .SDcols=worker.cols]
dt <- dt[, worker_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=worker.cols]
dt <- dt[, exp_lower := apply(.SD, 1, quantile, probs=.025), .SDcols=exp.cols]
dt <- dt[, exp_mean := rowMeans(.SD), .SDcols=exp.cols]
dt <- dt[, exp_upper := apply(.SD, 1, quantile, probs=.975), .SDcols=exp.cols]

write.csv(dt[, c(key(dt), 'exposure_me', 'measure_id', names(dt)[names(dt) %like% 'lower|mean|upper']), with=F],
          file.path(summary.dir, paste0(country, '_asthmagens.csv')))
#***********************************************************************************************************************

# ---SCRAP--------------------------------------------------------------------------------------------------------------

#***********************************************************************************************************************