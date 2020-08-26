###########################################################################
# Description: Process/clean guinea worm case data (all-ages). Merge with #
#              population data. Output results to gw_pop_corr.csv         #
#                                                                         #
###########################################################################

### ========================= BOILERPLATE ========================= ###

rm(list=ls())
code_root <- FILEPATH
data_root <- FILEPATH
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
  params_dir  <- FILEPATH
  draws_dir   <- FILEPATH
  interms_dir <- FILEPATH
  logs_dir    <- FILEPATH
}

##	Source relevant libraries
library(dplyr)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)

## Constants
gbd_round_id <- 7
decomp_step <- 'iterative'
study_dems <- get_demographics("epi", gbd_round_id=gbd_round_id)
final_study_year <- last(study_dems$year_id)

### ========================= MAIN EXECUTION ========================= ###

## (1) Read in country-year-specific case data from Bundle ID ADDRESS
cases <- fread(FILEPATH)


## (2) Manual handling of outliers
cases[location_id==169 & year_start %in% c(1996, 1997), is_outlier := 1] # Central African Republic
cases[location_id==44860 & year_start == 1992, is_outlier := 1] # Ethiopia - Gambella
cases[location_id==35659 & year_start == 1990, is_outlier := 1] # Kenya-Turkana
cases[location_id==190 & year_start %in% c(1990, 1992), is_outlier := 1] # Uganda
cases[location_id==200 & year_start %in% c(1991, 1992), is_outlier := 1] # Benin
cases[location_id==204 & year_start == 1992, is_outlier := 1] # Chad
cases[location_id==205 & year_start == 1990, is_outlier := 1] # Cote d'Ivorie
cases[location_id==211 & year_start == 1990, is_outlier := 1] # Mali
cases[location_id==212 & year_start == 1992, is_outlier := 1] # Mauritinia
cases[location_id==213 & year_start == 1992, is_outlier := 1] # Niger
cases[location_id==216 & year_start == 1990, is_outlier := 1] # Senegal
cases[location_id==218 & year_start %in% c(1990, 1991), is_outlier := 1] # Togo
cases[location_id==435 & year_start == 1996, is_outlier := 1] # South Sudan
cases[location_id==522 & year_start == 1994, is_outlier := 1] # Sudan
cases <- cases[is_outlier != 1]    # Drop all outliers


## (3) Cleaning
cases[, sex := NULL]
cases[, age_group_id := 22]
setnames(cases, old="year_start", new="year_id")
cases <- unique(cases) # drop duplicates


## (4) Merge all-age, both-sex population data
locations <- unique(cases$location_id)
pops <- get_population(location_id = locations,
                       age_group_id = ADDRESS,
                       sex_id = 3,
                       year_id = 1984:final_study_year,
                       gbd_round_id = gbd_round_id,
                       decomp_step = decomp_step)
cases <- merge(pops, cases, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)
cases[, sample_size := population]
cases <- cases[,c("location_id", "year_id", "sex_id", "age_group_id", "cases", "is_outlier", "sample_size")]


## (5) Write to a csv file
write.csv(cases, file=paste0(interms_dir, FILEPATH))
