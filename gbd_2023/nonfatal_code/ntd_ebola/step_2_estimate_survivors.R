# Purpose: estimate survivors for ebola
# Notes: zero out non-endemic, write out endemic/imported draws, apply UR
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-"FILEPATH"
data_root <- "FILEPATH"

# Source relevant libraries
library(data.table)
library(stringr)
library(argparse)
source("/FILEPATH/get_crosswalk_version.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_demographics.R")
source("/FILEPATH/get_draws.R")
source("/FILEPATH/get_age_metadata.R")
source("/FILEPATH/get_population.R")
source("/FILEPATH/save_results_cod.R")
source(paste0(code_root, "/FILEPATH/processing.R")) # function to make zero draw files
source(paste0(code_root, "/FILEPATH/utils.R")) #custom ebola functions

run_file <- fread(paste0(data_root, 'FILEPATH'))
run_path <- run_file[nrow(run_file), run_folder_path]

params_dir <- paste0(data_root, "/FILEPATH/params")
draws_dir <- paste0(run_path, '/draws')
interms_dir <- paste0(run_path, '/interms')
logs_dir <- paste0(run_path, '/logs')

# seed
data_dir   <- paste0(params_dir, "/data")    

#####################################################################
#####################################################################
###'[ ======================= MAIN EXECUTION ======================= ###
####################################################################
####################################################################

####################################################################
#' [Non-Fatal Estimates]
####################################################################

# 1) write-out non-endemic and non-imported locations
# 2) age/sex-split imported and endemic survivors
# 3) apply UR to endemic survivors
# 4) estimate -- acute with death and surv
# 5) estimate -- chronic with only surv
# 6) Submit results

#'[2) set release id
release_id <- ADDRESS

epi_demographics <- get_demographics('ADDRESS', release_id = release_id) 
years <- epi_demographics$year_id[1]:epi_demographics$year_id[length(epi_demographics$year_id)] # estimate ebola for every year, not just estimation years
locs <- epi_demographics$location_id

#  -----------------------------------------------------------------------
# 1) write out non-endemic locations -- anywhere with 0 imported cases & deaths, 0 end cases & deaths

all_data <- get_crosswalk_version(ADDRESS)
all_data[, sex_id := ifelse(sex == 'Male', 1,2)]

imported_surv    <- all_data[value_imported ==1]
all_death        <- fread(paste0(interms_dir, "/all_death_draws.csv"))

# get non_zero_locations
end_surv        <- all_data[value_imported == 0]
end_surv        <- end_surv[cases > 0]
end_surv[, age_group_id := value_age_group_id]
non_zero_locs   <- unique(c(imported_surv[,location_id], end_surv[,location_id]))  

# zero_locs for any not in death OR surv, any not in surv only; found that surv includes all of death location ids too
zero_locs       <- locs[!(locs %in% non_zero_locs)]

wrote <- 0
zeros <- gen_zero_draws(model_id = NA, location_id = NA, measure_id = 1, metric_id = 1, year_id = years, release_id = release_id, team = 'ADDRESS')
zeros[, metric_id := 3]

for (loc in zero_locs){ 
  zero <- copy(zeros)
  zero[, location_id := loc]
  fwrite(zero, paste0(draws_dir, "/ADDRESS/", loc, ".csv"))
  fwrite(zero, paste0(draws_dir, "/ADDRESS/", loc, ".csv"))
  wrote <- wrote + 1
  cat("\n Wrote", wrote, "of", length(zero_locs), "!!!")
}

#  -----------------------------------------------------------------------
# 2) put in draw space for calculation 

imported_surv_split <- imported_surv

for (draw in 0:999){
  imported_surv_split[, paste0("draw_", draw) := cases]
}

#  -----------------------------------------------------------------------
# 3) apply UR to endemic survivors

ebola_surv_split <- end_surv
ur <- fread(paste0(draws_dir, "/FILEPATH"), stringsAsFactors = FALSE)
ebola_surv_split <- as.data.table(apply_ur(data = ebola_surv_split, ur = ur))

#  -----------------------------------------------------------------------
# 4) estimate

# get all survivors and deaths in one table
ebola_surv_split[, measure_id := "S"]
imported_surv_split[, measure_id := "S"]
imported_surv_split[, year_id := as.character(year_start)]
all_death[, measure_id := "1"]

ebola_surv_split[, value_age_group_id := age_group_id]
all_death[, value_age_group_id := age_group_id]
imported_surv_split[, age_group_id := value_age_group_id]

shared_cols <- names(imported_surv_split)[(names(imported_surv_split) %in% names(ebola_surv_split))]
shared_cols <- shared_cols[shared_cols %in% names(all_death)]

all_draws   <- rbind(all_death[, ..shared_cols], ebola_surv_split[, ..shared_cols], imported_surv_split[, ..shared_cols])

prev   <- copy(all_draws)
acute_dur   <- fread(paste0(draws_dir, "/FILEPATH"))

#estimate death
prev_death <- copy(prev[measure_id == "1"])
inc_death <- copy(prev_death)

dur_death       <- acute_dur[group == "acute_death", ]
dur_inc <- copy(dur_death)
dur_inc <- dur_inc[, paste0("draw_", 0:999) := 1]

prev_death <- cases_to_prev(prev_death, dur_death, release_id)
inc_death  <- cases_to_prev(inc_death, dur_inc, release_id)

#estimate survivors (different duration)
prev_surv  <- copy(prev[measure_id == "S"])
inc_surv   <- copy(prev_surv)

dur_surv        <- acute_dur[group == "acute_survival", ]
prev_surv  <- cases_to_prev(prev_surv, dur_surv, release_id)
inc_surv  <- cases_to_prev(inc_surv, dur_inc, release_id)

# incidence

# bind, aggregate, and write

all_prev <- rbind(prev_death, prev_surv)
all_prev <- agg_asldy(all_prev) #function sums prevalence by age sex loc year draw

all_inc <- rbind(inc_death, inc_surv)
all_inc <- agg_asldy(all_inc) #function sums prevalence by age sex loc year draw

all_prev[, measure_id := 5]
all_prev[, metric_id := 3]

all_inc[, measure_id := 6]
all_inc[, metric_id := 3]

wrote <- 0

for (loc in unique(all_prev[,location_id])){
  data_inc <- copy(all_inc[location_id == loc])
  data_prev <- copy(all_prev[location_id == loc])
  
  zero <- copy(zeros)
  zero[,location_id := loc]
  zero_prev <- copy(zero[measure_id == 5])
  zero_inc <- copy(zero[measure_id == 6])
  csv   <- inserter(zero_draw_file = zero_prev, new_data = data_prev, loc = loc, m_id = 5)
  csv_inc   <- inserter(zero_draw_file = zero_inc, new_data = data_inc, loc = loc, m_id = 6)
  csv <- rbind(csv, csv_inc)
  wrote <- wrote + 1
  cat("\n Writing", loc, ";", wrote,  "of", length(unique(all_inc[,location_id])), "!!! \n")
  fwrite(csv, paste0(draws_dir, "FILEPATH", loc, ".csv"))
}

cat("Finished in :", as.character(unique(all_inc[,location_id])))

############################################

# 5) estimate -- just survivors

all_surv          <- rbind(ebola_surv_split[, ..shared_cols], imported_surv_split[, ..shared_cols])
chronic_dur_yr1   <- fread(paste0(draws_dir, "/FILEPATH"))
chronic_dur_yr2   <- fread(paste0(draws_dir, "/FILEPATH"))
chronic_dur_inc   <- copy(chronic_dur_yr1)
chronic_dur_inc[, paste0("draw_", 0:999) := 1]

# estimate year 1
cases     <- copy(all_surv)

# ** CHECK TYPE on melt
prev_yr1     <- cases_to_prev(cases, chronic_dur_yr1, release_id = release_id)
inc_yr1     <- cases_to_prev(cases, chronic_dur_inc, release_id = release_id)

# estimate year 2 
prev_yr2     <- as.data.table(cases_to_prev(cases, chronic_dur_yr2, release_id = release_id))
prev_yr2[, year_id := year_id + 1] # set year to in front

# bind, aggregate, and write
all <- rbind(prev_yr1, prev_yr2)
all <- agg_asldy(all)

all_inc <- inc_yr1
all_inc <- agg_asldy(all_inc)

all[, measure_id := 5]
all[, metric_id := 3]

all_inc[, measure_id := 6]
all_inc[, metric_id := 3]

wrote <- 0

for (loc in unique(all[,location_id])){
  data_inc <- copy(all_inc[location_id == loc])
  data_prev <- copy(all[location_id == loc])
  
  zero <- copy(zeros)
  zero[,location_id := loc]
  zero_prev <- copy(zero[measure_id == 5])
  zero_inc <- copy(zero[measure_id == 6])
  csv   <- inserter(zero_draw_file = zero_prev, new_data = data_prev, loc = loc, m_id = 5)
  csv_inc <- inserter(zero_draw_file = zero_inc, new_data = data_inc, loc = loc,  m_id = 6)
  csv <- rbind(csv, csv_inc)
  wrote <- wrote + 1
  cat("\n Writing", loc, ";", wrote,  "of", length(unique(all[,location_id])), "!!! \n")
  
  fwrite(csv, paste0(draws_dir, "FILEPATH", loc, ".csv"))
  
}