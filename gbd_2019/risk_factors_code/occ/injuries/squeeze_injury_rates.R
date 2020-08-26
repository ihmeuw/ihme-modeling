# ---HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Squeeze modeled industry fatal injury rates to the total model
#***********************************************************************************************************************

# ---CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# load packages
# pacman::p_load(data.table, fst, gridExtra, ggplot2, lme4, magrittr, parallel, stringr, readxl)
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
decomp_step     <- arg[5]
cores.provided  <- arg[6] %>% as.numeric

#set values for project
relevant.ages <- c(8:20, 30) #only ages 15-80 (85+ = 0 participation)

#colnames
draw.cols <- paste0("draw_", 0:(draws.required-1))
death.cols <- paste0("death_", 0:(draws.required-1))
worker.cols <- paste0("workers_", 0:(draws.required-1))
tot.cols <- paste0(draw.cols, "_total")
denom.cols <- paste0(draw.cols, "_denom")
squeezed.cols <- paste0(draw.cols, "_squeezed")
paf.cols <- paste0("paf_", 0:(draws.required-1))

#create a function and use it to follow just N draw(s) through the whole process
drawTracker <- function(varlist,
                        random.draws) {

  varlist[-random.draws] %>% return

}

subtract <- sapply(list(draw.cols, death.cols, worker.cols, squeezed.cols, tot.cols, denom.cols, paf.cols),
                   drawTracker,
                   random.draws=sample(draws.required, 3))

##in##
data.dir <- "FILEPATH"
doc.dir <- file.path(home.dir, "FILEPATH")
injury.causes <- file.path(doc.dir, "FILEPATH") %>% fread
isic.map <- file.path(doc.dir, "FILEPATH")
isic.3.map <- read.xlsx(isic.map, sheet = "ISIC_REV_3_1") %>% as.data.table
worker.dir <- file.path("FILEPATH", worker.version)

##out##
summary.dir <- file.path("FILEPATH", output.version)
exp.dir <- file.path("FILEPATH", output.version)
if(!dir.exists(summary.dir)) dir.create(summary.dir, recursive = TRUE)
if(!dir.exists(exp.dir)) dir.create(exp.dir, recursive = TRUE)
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
ubcov.function.dir <- "FILEPATH"
file.path(ubcov.function.dir, "FUNCTION") %>% source
# central functions
source("FUNCTION")
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

  file.path(dir, paste0(loc, "_", value, suffix)) %>% return

}
#***********************************************************************************************************************

# ---PREP INJURY DRAWS--------------------------------------------------------------------------------------------------
#### get best ST-GPR run_ids
stgpr_models <- read.xlsx("FILEPATH") %>% as.data.table
best_run_ids <- stgpr_models[resub_best == 1 & me_name %like% "occ_inj_major_", .(me_name, run_id)] # resubmission runs

bindCategory <- function(x) {

  run <- best_run_ids[x, run_id]
  name <- best_run_ids[x, me_name]

  message("reading in draws for: ", name)

  in.dir <- file.path(data.dir, run, "draws_temp_0")

  cat <- file.path(in.dir, paste0(country, ".csv")) %>% fread

  cat[, isic_code := substr(name, 15, 15)]

}

#append all injury models
dt <- lapply(1:nrow(best_run_ids), bindCategory) %>%
  rbindlist(fill=T) %>%
  setkeyv(c("location_id", "year_id", "age_group_id", "sex_id"))
dt[,measure_id := NULL]

#split off the totals to use as the denominator when squeezing
totals <- dt[isic_code == "T"]
setnames(totals, draw.cols, denom.cols)

#read in best version of workers per industry
workers <- fileName(worker.dir, country, "ind", "_workers.fst") %>% read.fst(as.data.table=T)
workers[, isic_code := substr(me_name, 15, 15)]

#merge workers to your injury rates and total injury rates
all <- merge(dt[, -c("age_group_id", "measure_id"), with=F], workers[, -c("me_name"), with=F],
             by=c("location_id", "year_id", "sex_id", "isic_code"), allow.cartesian=T)

#merge workers to your injury rates and total injury rates
all <- merge(totals[, -c("age_group_id", "measure_id", "isic_code"), with=F], all,
             by=c("location_id", "year_id", "sex_id"), allow.cartesian=T)

#reset key
setkeyv(all, c("location_id", "year_id", "age_group_id", "sex_id"))

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
        path=fileName(exp.dir, country, "inj", ".fst"))
write.csv(out[, -draw.cols, with=F],
          fileName(summary.dir, country, "inj", ".csv"))

message(paste0("DONE! inj results saved in ", exp.dir, " and ", summary.dir))
