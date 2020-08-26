#' @author 
#' @date 2019/10/08
#' @description compute squeezed incidence and squeezed mortality proportions for 
#'              all meningitis etiologys
#'              saves them in subdirectories of out_dir for each etiology for upload
rm(list=ls())

library(pacman)
pacman::p_load(data.table, boot, ggplot2)

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
library(argparse)

# Get arguments from parser
parser <- ArgumentParser()
parser$add_argument("--date", help = "timestamp of current run (i.e. 2014_01_17)", default = NULL, type = "character")
parser$add_argument("--out_dir", help = "directory for writing outputs csvs", default = NULL, type = "character")
parser$add_argument("--check_dir", help = "directory for to write finished check file", default = NULL, type = "character")
parser$add_argument("--param_map_path", help = "directory for this steps intermediate draw files", default = NULL, type = "character")
parser$add_argument("--ds", help = "specify decomp step", default = 'step4', type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)

# get location_id from parameter map
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
parameters <- fread(param_map_path)
loc <- parameters[task_id, location_id]

# SOURCE FUNCTIONS --------------------------------------------------------
source("current/r/get_draws.R")
source("interpolate.R")

# SET OBJECTS -------------------------------------------------------------
# directories where fatal and nonfatal PAF csvs will be written
fatal_out_dir <- file.path(out_dir, "fatal/")
spn_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_pneumo/")
hib_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_hib/")
nm_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_meningo/")
other_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_other/")

nonfatal_out_dir <- file.path(out_dir, "nonfatal/")
spn_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_pneumo/")
hib_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_hib/")
nm_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_meningo/")
other_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_other/")

# INTERPOLATE FATAL PAFS ------------------------------------------------------
rei_id <- c(386,188,189) # menigo, pneumo, hib
names <- c('meningitis_meningo', 'meningitis_pneumo', 'meningitis_hib')
date <- date <- gsub("-", "_", Sys.Date())
out_dir <- # filepath

## FATAL OR NONFATAL TOGGLE
fatal = 1
if (fatal == 1) {
  modelable_entity_id <- c(10495,10494,10496) # menigo, pneumo, hib
  fatality = 'fatal'
} else if (fatal == 0){
  modelable_entity_id <- c(24741,24739,24740) # menigo, pneumo, hib
  fatality = 'nonfatal'
}

## INTERPOLATE 
gbd_round_id <- 6
sex_id <- c(1,2)
for(v in 1:3){
  m <- modelable_entity_id[v]
  r <- rei_id[v] 
  name <- names[v]
  print(paste0("Interpolating years for ", name))
  df <- interpolate(gbd_id_type="modelable_entity_id"
                    , gbd_id=m, source='epi'
                    , measure_id=18
                    , location_id=loc, sex_id=sex_id
                    , gbd_round_id=gbd_round_id
                    , decomp_step='step4'
                    , reporting_year_start=1990
                    , reporting_year_end=2019
  )
  df$cause_id <- 332
  df$rei_id <- r
  df$modelable_entity_id <- m
  fwrite(df, paste0(out_dir, fatality, "/", name, "/", loc, "_annual_paf.csv"), row.names=F)
  print(paste0("Saved for ", name, fatality, "!"))
}

if (length(grep("draw_", colnames(df))) < 1000) {
  file.create(paste0(h, "failed_", loc, ".txt"), overwrite=T)
  stop(paste("Error: output files do not have 1000 draws for location", loc))
}

# INTERPOLATE NONFATAL PAFS ------------------------------------------------------
## FATAL OR NONFATAL TOGGLE
fatal = 0
if (fatal == 1) {
  modelable_entity_id <- c(10495,10494,10496) # menigo, pneumo, hib
  fatality = 'fatal'
} else if (fatal == 0){
  modelable_entity_id <- c(24741,24739,24740) # menigo, pneumo, hib
  fatality = 'nonfatal'
}

## INTERPOLATE 
gbd_round_id <- 6
sex_id <- c(1,2)
for(v in 1:3){
  m <- modelable_entity_id[v]
  r <- rei_id[v] 
  name <- names[v]
  print(paste0("Interpolating years for ", name))
  df <- interpolate(gbd_id_type="modelable_entity_id"
                    , gbd_id=m, source='epi'
                    , measure_id=18
                    , location_id=loc, sex_id=sex_id
                    , gbd_round_id=gbd_round_id
                    , decomp_step='step4'
                    , reporting_year_start=1990
                    , reporting_year_end=2019
  )
  df$cause_id <- 332
  df$rei_id <- r
  df$modelable_entity_id <- m
  fwrite(df, paste0(out_dir, fatality, "/", name, "/", loc, "_annual_paf.csv"), row.names=F)
  print(paste0("Saved for ", name, fatality, "!"))
}


# WRITE CHECKS ------------------------------------------------------------
if (length(grep("draw_", colnames(df))) < 1000) {
  file.create(paste0(h, "failed_", loc, ".txt"), overwrite=T)
  stop(paste("Error: output files do not have 1000 draws for location", loc))
}

file.create(paste0(check_dir, "finished_", loc, ".txt"), overwrite=T)
