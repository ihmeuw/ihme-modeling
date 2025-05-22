# NTDS: Visceral leishmaniasis - fatal
# Purpose: extend gbd 2017 cfr estimates with new locations and custom values
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev)
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
  params_dir <- paste0(data_root, "FILEPATH")
  draws_dir <- paste0(data_root, "FILEPATH")
  interms_dir <- paste0(data_root, "FILEPATH")
  logs_dir <- paste0(data_root, "FILEPATH")
  location_id <- 214
}

# source relevant libraries
library(data.table)
library(stringr)
library(ggplot2)
library(plotly)
library(lme4)
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_cod_data.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/custom_functions/processing.R")
source("FILEPATH/launch_function.R")
my_shell <- paste0("FILEPATH")


release_id <- ADDRESS

run_file <- fread(paste0("FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
draws_dir    <- paste0(run_dir, "FILEPATH")
interms_dir    <- paste0(run_dir, "FILEPATH")
params_dir  <- paste0(data_root, "FILEPATH")

### ======================= Main Execution ======================= ###
study_dems <- readRDS(paste0(data_root, 'FILEPATH'))
location_ids <- study_dems$location_id

stgpr_run_id <- fread(paste0(interms_dir, "FILEPATH"))
stgpr_run_id <- stgpr_run_id[,run_id]

# create directory
ifelse(!dir.exists(paste0(draws_dir, "FILEPATH")), dir.create(paste0(draws_dir, "FILEPATH")), FALSE)
locs <- get_location_metadata(ADDRESS, release_id = release_id)

# load gbd 2017 modeled grs
load(file=paste0(data_root, 'FILEPATH'))

i <- 0
l_ids <- c() 
new_subnat <- c(loc_ids)

for (i in 1:length(cfr_list)){
  cfrs <- as.data.table(cfr_list[[i]])
  loc_id <- unique(cfrs$location_id)
  l_ids <- c(l_ids, loc_id)
  # overwrite locations for new subnationals use national to overwrite
  if (loc_id %in% new_subnat){
    new_ids <- locs[parent_id == loc_id, location_id]
    for (l_id in new_ids){
      # write out children with parents values
      cfrs[, location_id := l_id]
      fwrite(cfrs, paste0(draws_dir, "FILEPATH", l_id, ".csv"))    
      }
  } else {
  fwrite(cfrs, paste0(draws_dir, "FILEPATH", loc_id, ".csv"))
  }
  cat("\nWrote ", i, " of ", length(cfr_list))
}

# values -  unifrom distribution 6 - 10%
cfr_europe_ids <- c(loc_ids)
cfr_draw <- copy(cfrs[, .(age_group_id, sex_id, year_id, location_id, cause_id, measure_id)])
row <- as.data.table(t(data.table(runif(n = 1000, min = 0.06, max = 0.10))))
setnames(row, as.character(1:1000))
cfr_draw <- cbind(cfr_draw, row)

for (loc_id in cfr_europe_ids){
  cfr_draw[, location_id := loc_id]
  fwrite(cfr_draw, paste0(draws_dir, "FILEPATH", loc_id, ".csv"))
}

# add custom cfr rate for Africa
#  uniform distribution of 10%-30% for all age and sex groups 

# SSA location
locs <- get_location_metadata(ADDRESS, release_id = release_id)
afr_r <- locs[parent_id == 166, location_id] # level 2
afr_n <- locs[parent_id %in% afr_r]
afr_sn<- locs[parent_id %in% afr_n[, location_id]]

afr_locs <- rbind(afr_sn, afr_n)
afr_locs <- afr_locs[most_detailed == 1, location_id]

cfr_draw <- copy(cfr_draw[, .(age_group_id, sex_id, year_id, location_id, cause_id, measure_id)])
row <- as.data.table(t(data.table(runif(n = 1000, min = 0.10, max = 0.30))))
setnames(row, as.character(1:1000))
cfr_draw <- cbind(cfr_draw, row)

for (loc_id in afr_locs){
  cfr_draw[, location_id := loc_id]
  fwrite(cfr_draw, paste0(draws_dir, "FILEPATH", loc_id, ".csv"))
}

# add cfr for new Norway location
cfr_draw <- as.data.table(cfr_list[[285]])
if (unique(cfr_draw[,location_id]) == 4910) { cat('pull Oslo subnational as skeleton - location is restricted but need structure for code')}
norway_sn <- locs[parent_id == 90, location_id]

for (loc_id in norway_sn){
  cfr_draw[, location_id := loc_id]
  fwrite(cfr_draw, paste0(draws_dir, "FILEPATH", loc_id, ".csv"))
}
 
# send out qsub to calculate deaths from cfr
ifelse(!dir.exists(paste0(draws_dir, "FILEPATH")), dir.create(paste0(draws_dir, "FILEPATH")), FALSE)

my_locs    <- location_ids

done_files <- list.files(paste0(draws_dir, "FILEPATH"))
done_locs <- as.integer(str_extract_all(string = done_files, pattern = "[0-9]+", simplify= TRUE))
est_locs <- setdiff(my_locs, done_locs)

grs <- fread(paste0(data_root, 'FILEPATH'))
grs <- grs[year_start == 1980, .(location_id, value_endemicity)]
  
param_map <- data.table(location_id = est_locs, release_id = release_id)
param_map <- merge(param_map, grs, by = 'location_id', all.x = TRUE)
if (nrow(param_map[is.na(value_endemicity)])> 0 ) {stop('gr endemicity merge did not work')}
  
proj <- "ADDRESS"
shell  <- "FILEPATH"
script <- "FILEPATH.R"
param_map <- fread("FILEPATH")


command   <- paste0("sbatch --mem=",mem_alloc,"G",
                    " -c ", fthreads,
                    " -t ",time_alloc,
                    " -a ",  paste0("1-", nrow(param_map), "%100"), 
                    " -p long.q",
                    " -e FILEPATH",
                    " -o FILEPATH",
                    " -A ", proj, 
                    " -i ADDRESS",
                    " -J MESSAGE",
                    " ", shell, 
                    " -s ", script)

system(command)
