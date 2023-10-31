################
# 4.1. Pull DALY draws for TMREL
################

### Grab packages ----------------------------------------------------------
library(data.table)
library(plyr)
library(dplyr)
library(parallel)

### Source functions -------------------------------------------------------
setwd('FILEPATH')
source("get_draws.R")
source("get_location_metadata.R")
source("interpolate.R")
source("get_population.R")

### Specific args ----------------------------------------------------------
param_map <- fread('FILEPATH')
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))

i <- param_map[task_id, cause_num]
j <- param_map[task_id, sex]

paf_path <- 'FILEPATH'

#-----------------------------------------------------------------------------

locs <- get_location_metadata(22, gbd_round_id = 7)
cause_map <- fread('FILEPATH')

regions <- unique(locs[level == 3]$region_id)
ages <- c(8:20, 30:32, 235)
years <- c(1990:2022)
  
#Set options for TMREL calculation
directory <- 'FILEPATH'
setwd(directory)

files <- list.files(directory)

causes <- as.numeric(unique(regmatches(files, regexpr("\\d{3}", files))))
causes <- c(causes, unique(cause_map[cause_id_old == 696]$cause_id_new))
causes <- causes[!causes %in% c(696)] #parent cause
causes <- c(causes, c(522, 523, 524, 525, 971, 418, 419, 420, 421, 996)) #cirrhosis and liver cancer redistribution
causes <- unique(causes)

message(i)
message(j)

single_cause <- interpolate("cause_id", gbd_id=causes[i], 
              location_id= regions, 
              sex_id=j,
              age_group_id=ages, 
              reporting_year_start = 1990,
              reporting_year_end = 2022,
              num_workers=5,
              gbd_round_id=7, 
              measure_id=2,
              decomp_step = "iterative",
              version_id = 48,
              source="dalynator") %>%
  as.data.table %>%
  .[metric_id == 3] %>%
  .[, c("measure_id", "metric_id") := NULL] 

single_cause <- melt(single_cause, id.vars = c("location_id", "sex_id", "year_id", "age_group_id", "cause_id"))
print("done aggregating")

path <- 'FILEPATH'

if (!dir.exists(path)){
  dir.create(path)
}

fwrite(single_cause, 'FILEPATH', row.names = F)
