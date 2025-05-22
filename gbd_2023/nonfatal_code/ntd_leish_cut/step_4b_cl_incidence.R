########################################################################
# Description:                                                         #
# Pulls ST-GPR draws, applies age-pattern from DisMod and calculates   #
# incidence estimates for each endemic location                        s#
#                                                                      #
########################################################################

### ========================= BOILERPLATE ========================= ###

rm(list = ls())
# Load packages/central functions
library(dplyr)
library(data.table)
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_age_metadata.R")
source("/FILEPATH/interpolate.R")

param_map <- fread("/FILEPATH")
task_id <- as.integer(Sys.getenv("ADDRESS"))
i <- param_map[task_id, location_id]

code_root <- "FILEPATH"
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common /ihme/ IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  parser$add_argument("--location_id", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- paste0(data_root, "FILEPATH")
  draws_dir <- paste0(data_root, "FILEPATH")
  interms_dir <- paste0(data_root, "FILEPATH")
  logs_dir <- paste0(data_root, "FILEPATH")
}

# Set run dir
run_file <- fread(paste0(data_root, "/FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "/crosswalks/")
draws_dir    <- paste0(run_dir, "/draws/")
interms_dir    <- paste0(run_dir, "/interms/")

# Define constants
release_id <- release_id 
years <- 1980:2024

### ========================= MAIN EXECUTION ========================= ###

## Pull in population estimates
ages <- get_age_metadata(release_id = release_id)$age_group_id
pop <- get_population(location_id = i, year_id = years, age_group_id = ages, sex_id = c(1,2), release_id = release_id)

pop[, totalPop := sum(population), by=.(location_id, year_id)]

  loc_stgpr <- read.csv(file = paste0(interms_dir,"/FILEPATH"))

  # merge population, all-age estimates
  inc <- merge(pop, loc_stgpr, by=c("location_id", "year_id"), all = F)
  inc <- select(inc, -c(X, run_id, sex_id.y, age_group_id.y))
  setnames(inc, "age_group_id.x", "age_group_id")
  setnames(inc, "sex_id.x", "sex_id")
  
  # merge age/sex curve
  ageCurve <- read.csv(file = paste0("/FILEPATH"))
  
  ageCurve <- select(ageCurve, -c(modelable_entity_id, model_version_id))

  split <- merge(inc, ageCurve, by=c("sex_id", "age_group_id", "year_id", "location_id", "measure_id"))

  ## Apply age split
  # convert to cases
  split[, paste0("draw_",0:999) := lapply(0:999, function(x)
    get(paste0("draw_", x)) * get("totalPop") )]

  split[, paste0("casesCurve_",0:999) := lapply(0:999, function(x)
    get(paste0("ageCurve_", x)) * get("population") )]

  # sum casesCurve_i across age/sex groups (within a location/year)
  split[, paste0("totalCasesCurve_",0:999) := lapply(0:999, function(x)
    sum(get(paste0("casesCurve_", x))) ), by=.(location_id, year_id)]

  # calculate age- and sex-specific incidence
  split[, paste0("draw_",0:999) := lapply(0:999, function(x)
    get(paste0("casesCurve_", x)) * (get(paste0("draw_", x)) / get(paste0("totalCasesCurve_", x))) / get("population") )]

  cols_to_drop <- c(paste0("totalCasesCurve_",0:999), paste0("casesCurve_",0:999), paste0("ageCurve_",0:999))
  split <- split[, (cols_to_drop):=NULL]

  # export incidence draws
  write.csv(split, file = (paste0(interms_dir, "/FILEPATH")), row.names = F)

  cat("\n Writing", i)