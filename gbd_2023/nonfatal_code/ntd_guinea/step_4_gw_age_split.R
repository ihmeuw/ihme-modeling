###########################################################################
# Description: Guinea worm age-splitting                                  #
#                                                                         #
#                                                                         #
###########################################################################

### ======================= BOILERPLATE ======================= ###

rm(list=ls())
code_root <-"FILEPATH"
data_root <- "FILEPATH"
cause <- "ntd_guinea"

## Define paths 
# Toggle btwn production arg parsing vs interactive development
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir  <- "FILEPATH"
  draws_dir   <- "FILEPATH"
  interms_dir <- "FILEPATH"
  logs_dir    <- "FILEPATH"
}

##	Source relevant libraries
library(readstata13)
library(data.table)
source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")

## Define Constants
gbd_round_id <- "ADDRESS"
decomp_step <- "ADDRESS"
study_dems <- get_demographics("ADDRESS", release_id = release_id)
final_study_year <- last(study_dems$year_id)


### ======================= MAIN EXECUTION ======================= ###

## (1) Import incidence data
incidence <- readRDS("FILEPATH"))

## (2) Merge all-age incidence onto age-specific population
locations <- unique(incidence$location_id)
pops <- get_population(location_id= locations,
                       year_id=1984:final_study_year,
                       age_group_id=study_dems$age_group_id,
                       sex_id=c(1,2),
                       release_id = release_id)

# within a location year, calculate pop across sex and age groups
pops[, totalPop := sum(population), by=.(location_id, year_id)]
inc <- merge(pops, incidence, by=c("location_id", "year_id", "sex_id"))


## (3) Merge Age-Curve from Dismod
# Import age splitting draws file from dismod
draws <- read.dta13("FILEPATH"))
draws <- as.data.table(draws)[,c("location_id","year_id") := NULL] # age curve is for all years and locations
setnames(draws, old=paste0("draw_",0:999), new=paste0("ageCurve_",0:999))


## (4) Adjust to GBD2020 age groups
age_4 <- draws[age_group_id == 4]
age_5 <- draws[age_group_id == 5]
age_388 <- copy(age_4[, age_group_id := 388])
age_389 <- copy(age_4[, age_group_id := 389])
age_238 <- copy(age_5[, age_group_id := 238])
age_34 <- copy(age_5[, age_group_id := 34])
draws <- rbind(draws, age_388, age_389, age_238, age_34)
split <- merge(inc, draws, by=c("sex_id", "age_group_id"), all.x=TRUE, all.y=FALSE)


## (5) Apply age split
split[, paste0("casesCurve_",0:999) := lapply(0:999, function(x)
  get(paste0("ageCurve_", x)) * get("population") )]

# Sum casesCurve_i across age groups (within a location/year/sex)
split[, paste0("totalCasesCurve_",0:999) := lapply(0:999, function(x)
  sum(get(paste0("casesCurve_", x))) ), by=.(location_id, year_id, sex_id)]

# Calculate age-specific incidence
split[, paste0("inc_age_draw_",0:999) := lapply(0:999, function(x)
  get(paste0("casesCurve_", x)) * (get(paste0("sex_cases_", x)) / get(paste0("totalCasesCurve_", x))) / get("population") )]

# Drop casesCurve_i and totalCasesCurve_i
split <- split[, paste0("casesCurve_",0:999):=NULL]
split <- split[, paste0("totalCasesCurve_",0:999):=NULL]


## (6) Prepare data for upload
"ADDRESS" <- split[, c("location_id", "year_id", "age_group_id", "sex_id", "measure_id", paste0("inc_age_draw_",0:999))]
setnames("ADDRESS", old=paste0("inc_age_draw_",0:999), new=paste0("draw_",0:999))
"ADDRESS"$model_id<- "ADDRESS"
"ADDRESS"$measure_id <- 6

# For under 2 yrs, set incidence to 0
"ADDRESS"[age_group_id %in% c(2,3,238,388,389), paste0("draw_",0:999) := 0]


# Create prevalence == incidence
row_list <- rep(seq_len(nrow("ADDRESS")), each = 2)
"ADDRESS" <- "ADDRESS"[row_list,]
"ADDRESS"$measure_id <- rep(c(6,5), length.out=nrow("ADDRESS"))

# Save file
write.csv(file = "FILEPATH"))
