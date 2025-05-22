# Project: NTDS - Lymphatic filariasis
# Purpose: code to account for ADL due to hydrocele and lymphedema

############## BOILERPLATE ############## 
rm(list = ls())
user <- Sys.info()[["user"]]
cause <- "ntd_lf"
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"
options(max.print=999999)

# toggle btwn production arg parsing vs interactive development
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  parser$add_argument("--release_id", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir  <- "FILEPATH"
  draws_dir   <- "FILEPATH"
  interms_dir <- "FILEPATH"
  logs_dir    <- "FILEPATH"
  release_id  <- ADDRESS
}

# 1. Setup libraries, functions, parameters, filepaths, etc. ---------------------------------------------------------------------
# install non-included libraries
path <- "FILEPATH"
if (!dir.exists(file.path(path))){dir.create(path)}
install.packages("BayesianTools", lib=path)
install.packages("Rcpp", lib=path)

# load libraries
library(BayesianTools, lib.loc=path)
library(Rcpp, lib.loc=path)
require(splines)
library(INLA)
library(ggplot2)
library(dplyr)
library(purrr)

# source functions
source("FILEPATH")

# directories
params_dir <- "FILEPATH"
run_file <- fread("FILEPATH")
run_folder_path <- run_file[nrow(run_file), run_folder_path]
draws_dir <- "FILEPATH"
interms_dir <- "FILEPATH"
crosswalks_dir <- "FILEPATH"
lymph_draws_dir <- "FILEPATH"
hydro_draws_dir <- "FILEPATH"
adl_draws_dir <- "FILEPATH"
if (!dir.exists(file.path(adl_draws_dir))){dir.create(adl_draws_dir)}

# set parameters
draws <- 1000
release_id <- ADDRESS

# 2. Load GBD location information ---------------------------------------------------------------------
# get current GBD values for primary demographics
demo <- get_demographics(gbd_team = 'epi', release_id = release_id)
gbd_years <- sort(demo$year_id)
gbd_loc_ids <- sort(demo$location_id)
gbd_ages <- sort(demo$age_group_id)
gbd_sex_id <- sort(demo$sex_id)

# load LF geographic restrictions
lf_endems <- fread("FILEPATH")[value_endemicity == 1,]
lf_endems <- sort(unique(lf_endems$location_id))
## add India level-5 sub-nationals
### add Rural units to lf_endems and Urban to zero_locs
lvl5_rural <- c(43908,43909,43910,43911,43913,43916,43917,43918,43919,43920,43921,43922,
                43923,43924,43926,43927,43928,43929,43930,43931,43932,43934,43935,43936,
                43937,43938,43939,43940,43941,43942,44539)
lvl5_urban <- c(43872,43873,43874,43875,43877,43880,43881,43882,43883,43884,43885,43886,
                43887,43888,43890,43891,43892,43893,43894,43895,43896,43898,43899,43900,
                43901,43902,43903,43904,43905,43906,44540)
lvl_india <- c(lvl5_rural,lvl5_urban)
lf_endems <- c(lf_endems,lvl_india)
lf_endems <- unique(lf_endems)

# 3. Estimate ADL prevalence ---------------------------------------------------------------------
# pull in lymphedema draws
lymph_draws<-read.csv("FILEPATH")
lymph_draws<-as.data.table(lymph_draws)

## assume that 95% of patients have 4 episodes per year, 7 days each
for(j in grep('draw', names(lymph_draws))){
  set(lymph_draws, i= which(lymph_draws[[j]]<=1), j= j, value=lymph_draws[[j]]*(.95*4*(7/365)))
}

# pull in hydrocele draws
hydro_draws <- read.csv("FILEPATH")
#hydro_draws <- hydro_draws[hydro_draws$dataset==1,]

# assume that 70% of patients have 2 episodes per year, 7 days each
for(j in grep('draw', names(hydro_draws))){
  set(hydro_draws, i= which(hydro_draws[[j]]<=1), j= j, value=hydro_draws[[j]]*(.7*2*(7/365)))
}

# add together lymphedema & hydrocele
## create matrix for each
### sort each by location_id year_id sex_id age_group_id
draw.cols <- paste0("draw_", 0:999)
ADL <- bind_rows(lymph_draws, hydro_draws) %>%
  group_by(location_id, year_id, sex_id, age_group_id) %>%
  summarise_at(draw.cols,funs(sum(., na.rm = TRUE)))

# 4. Generate zero-draw files ---------------------------------------------------------------------
# get list of non-endemic locations
non_endemic <- gbd_loc_ids[! gbd_loc_ids %in% lf_endems]
### additional zero-draws needed for locations with all zeros in GBD 2021
gbd_2021_zeros <- c(5, 6, 28, 68, 118, 119, 124, 126, 175, 183, 185, 
                    186, 203, 206, 212, 491, 493, 494, 496, 497, 498, 499, 
                    502, 503, 504, 506, 507, 513, 514, 516, 521, 4751, 
                    4754, 4763, 25342, 60908, 94364)
batanes <- c(53547)
### combine location_id lists for 
zero_locs <- c(non_endemic,gbd_2021_zeros,batanes)
### ensure no location_ids are duplicated
zero_locs <- unique(zero_locs)

# establish expected number of rows for each output draws file for validation step
exp_rows <- length(gbd_years) * length(gbd_ages) * length(gbd_sex_id) * 1

# generate, validate, and write-out all zero-draws files
for (lc in 1:length(zero_locs)) {
  loc <- zero_locs[lc]
  
  loc_zeros <- gen_zero_draws(modelable_entity_id = ADDRESS, location_id = loc, measure_id = c(5), metric_id = c(3), year_id = gbd_years, release_id = release_id, team = 'epi')
  
  ## verify that each location_id has expected number of observations (10 years * 25 age groups * 2 sexes * 1 location)
  if (nrow(loc_zeros) != exp_rows) {
    stop(print(paste0("location_id ", loc,"'s .csv file has ", nrow(loc_zeros)," rows but should have ", exp_rows," rows.")))
  }
  
  write.csv(loc_zeros, "FILEPATH", row.names = FALSE)
  
  print(paste0("Finished writing-out .csv file for location ", lc," of ", length(zero_locs)))
}

# 5. Format data, export draw files, and save ADL prevalence estimates ---------------------------------------------------------------------
# add columns for export files
ADL$modelable_entity_id <- ADDRESS
ADL$measure_id <- 5
ADL$metric_id <- 3

# export files for all locations
for(n in 1:length(lf_endems)){
  i <- lf_endems[[n]]
  upload_file<-ADL[ADL$location_id==i,]
  write.csv(upload_file,"FILEPATH")
  print(paste0("Finished writing-out .csv file for location ", n," of ", length(lf_endems)))
}

###### manually save results ######
save_results_epi(input_dir = adl_draws_dir,
                 input_file_pattern = "{location_id}.csv",
                 modelable_entity_id = ADDRESS,
                 description = "fixed lfmda cov measure and projected years",
                 measure_id = 5,
                 release_id = release_id,
                 mark_best = TRUE,
                 crosswalk_version_id = ADDRESS, 
                 bundle_id = ADDRESS
)       
