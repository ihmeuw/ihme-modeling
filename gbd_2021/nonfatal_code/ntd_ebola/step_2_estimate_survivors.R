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
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/get_demographics.R")
source(paste0(code_root, "FILEPATH/processing.R")) # function to make zero draw files
source(paste0(code_root, "FILEPATH/utils.R")) #custom ebola functions

data_dir   <- paste0(params_dir, "/data") 

run_file <- fread(paste0(data_root, 'FILEPATH/run_file.csv'))
run_path <- run_file[nrow(run_file), run_folder_path]

params_dir <- paste0("FILEPATH/params")
draws_dir <- paste0(run_path, '/draws')
interms_dir <- paste0(run_path, '/interms')
logs_dir <- paste0(run_path, '/logs')

#####################################################################

# set round id
gbd_round_id <- ADDRESS
decomp_step <- 'ADDRESS'

epi_demographics <- get_demographics('ADDRESS',gbd_round_id = gbd_round_id) 
years <- epi_demographics$year_id[1]:epi_demographics$year_id[length(epi_demographics$year_id)] # estimate ebola for every year, not just estimation years
locs <- epi_demographics$location_id

#  -----------------------------------------------------------------------
# 1) write out non-endemic locations -- anywhere with 0 imported cases & deaths, 0 end cases & deathss

all_data <- get_crosswalk_version(ADDRESS)
all_data[, sex_id := ifelse(sex == 'Male', 1,2)]

imported_surv    <- all_data[value_imported ==1]
all_death        <- fread(paste0(interms_dir, "/all_death_draws.csv"))

# get non_zero_locations
end_surv        <- all_data[value_imported == 0]
end_surv        <- end_surv[cases > 0]
end_surv[, age_group_id := value_age_group_id]
non_zero_locs   <- unique(c(imported_surv[,location_id], end_surv[,location_id]))  

# zero_locs for 'acute with death and surv' and any not in death OR surv, 'chronic with only surv' any not in surv only; 
# found that surv includes all of death location ids too
zero_locs       <- locs[!(locs %in% non_zero_locs)]

wrote <- 0
zeros <- gen_zero_draws(model_id= NA, location_id = NA, measure_id = c(5,6), metric_id = 1, year_id = years, gbd_round_id = gbd_round_id, team = 'ADDRESS')
zeros[, metric_id := 3]

for (loc in zero_locs){ 
  zero <- copy(zeros)
  zero[, location_id := loc]
  fwrite(zero, paste0(draws_dir, "/ADDRESS-acute/", loc, ".csv"))
  fwrite(zero, paste0(draws_dir, "/ADDRESS-chronic/", loc, ".csv"))
  wrote <- wrote + 1
  cat("\n Wrote", wrote, "of", length(zero_locs), "!!!")
}

#  -----------------------------------------------------------------------
# 2) put in draw space for calculation of acute and chronic

imported_surv_split <- imported_surv

for (draw in 0:999){
  imported_surv_split[, paste0("draw_", draw) := cases]
}

#  -----------------------------------------------------------------------
# 3) apply UR to endemic survivors

ebola_surv_split <- end_surv
ur <- fread(paste0(draws_dir, "FILEPATH/evd_underreporting_cases_draws.csv"), stringsAsFactors = FALSE)
ebola_surv_split <- apply_ur(data = ebola_surv_split, ur = ur)

#  -----------------------------------------------------------------------
# 4) estimate acute cases and deaths

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

prev_acute   <- copy(all_draws)
acute_dur   <- fread(paste0(draws_dir, "FILEPATH/evd_acute_duration_draws.csv"))

#estimate death
prev_acute_death <- copy(prev_acute[measure_id == "1"])
inc_acute_death        <- copy(prev_acute_death)

dur_death       <- acute_dur[group == "acute_death", ]
dur_inc <- copy(dur_death)
dur_inc <- dur_inc[, paste0("draw_", 0:999) := 1]

prev_acute_death <- cases_to_prev(prev_acute_death, dur_death, pop_decomp_step = decomp_step, round_id = gbd_round_id)
inc_acute_death  <- cases_to_prev(inc_acute_death, dur_inc, pop_decomp_step = decomp_step, round_id = gbd_round_id)

#estimate survivors (different duration)
prev_acute_surv  <- copy(prev_acute[measure_id == "S"])
inc_acute_surv   <- copy(prev_acute_surv)

dur_surv        <- acute_dur[group == "acute_survival", ]
prev_acute_surv  <- cases_to_prev(prev_acute_surv, dur_surv, pop_decomp_step = decomp_step, round_id = gbd_round_id)
inc_acute_surv  <- cases_to_prev(inc_acute_surv, dur_inc, pop_decomp_step = decomp_step, round_id = gbd_round_id)

# incidence for acute cases and deaths

# bind, aggregate, and write

all_acute_prev <- rbind(prev_acute_death, prev_acute_surv)
all_acute_prev <- agg_asldy(all_acute_prev) #function sums prevalence by age sex loc year draw

all_acute_inc <- rbind(inc_acute_death, inc_acute_surv)
all_acute_inc <- agg_asldy(all_acute_inc) #function sums prevalence by age sex loc year draw

all_acute_prev[, measure_id := 5]
all_acute_prev[, metric_id := 3]

all_acute_inc[, measure_id := 6]
all_acute_inc[, metric_id := 3]

wrote <- 0

for (loc in unique(all_acute_prev[,location_id])){
  data_inc <- copy(all_acute_inc[location_id == loc])
  data_prev <- copy(all_acute_prev[location_id == loc])
  
  zero <- copy(zeros)
  zero[,location_id := loc]
  zero_prev <- copy(zero[measure_id == 5])
  zero_inc <- copy(zero[measure_id == 6])
  csv   <- inserter(zero_draw_file = zero_prev, new_data = data_prev, loc = loc, m_id = 5)
  csv_inc   <- inserter(zero_draw_file = zero_inc, new_data = data_inc, loc = loc, m_id = 6)
  csv <- rbind(csv, csv_inc)
  wrote <- wrote + 1
  cat("\n Writing", loc, ";", wrote,  "of", length(unique(all_acute_inc[,location_id])), "!!! \n")
  fwrite(csv, paste0(draws_dir, "/acute/", loc, ".csv"))
}

cat("Finished for acute all in :", as.character(unique(all_acute_inc[,location_id])))

############################################

# 5) estimate chronic cases -- just survivors

all_surv          <- rbind(ebola_surv_split[, ..shared_cols], imported_surv_split[, ..shared_cols])
chronic_dur_yr1   <- fread(paste0(draws_dir, "FILEPATH/evd_chronic_duration_draws_year1.csv"))
chronic_dur_yr2   <- fread(paste0(draws_dir, "FILEPATH/evd_chronic_duration_draws_year2.csv"))
chronic_dur_inc   <- copy(chronic_dur_yr1)
chronic_dur_inc[, paste0("draw_", 0:999) := 1]

# estimate year 1
cases     <- copy(all_surv)

# ** CHECK TYPE on melt
prev_chronic_yr1     <- cases_to_prev(cases, chronic_dur_yr1, pop_decomp_step = decomp_step, round_id = gbd_round_id)
inc_chronic_yr1     <- cases_to_prev(cases, chronic_dur_inc, pop_decomp_step = decomp_step, round_id = gbd_round_id)

# estimate year 2 
prev_chronic_yr2     <- cases_to_prev(cases, chronic_dur_yr2, pop_decomp_step = decomp_step, round_id = gbd_round_id)
prev_chronic_yr2[, year_id := year_id + 1] # set year 2 in front

# bind, aggregate, and write
all_chronic <- rbind(prev_chronic_yr1, prev_chronic_yr2)
all_chronic <- agg_asldy(all_chronic)

all_chronic_inc <- inc_chronic_yr1
all_chronic_inc <- agg_asldy(all_chronic_inc)

all_chronic[, measure_id := 5]
all_chronic[, metric_id := 3]

all_chronic_inc[, measure_id := 6]
all_chronic_inc[, metric_id := 3]

wrote <- 0

for (loc in unique(all_chronic[,location_id])){
  data_inc <- copy(all_chronic_inc[location_id == loc])
  data_prev <- copy(all_chronic[location_id == loc])
  
  zero <- copy(zeros)
  zero[,location_id := loc]
  zero_prev <- copy(zero[measure_id == 5])
  zero_inc <- copy(zero[measure_id == 6])
  csv   <- inserter(zero_draw_file = zero_prev, new_data = data_prev, loc = loc, m_id = 5)
  csv_inc <- inserter(zero_draw_file = zero_inc, new_data = data_inc, loc = loc,  m_id = 6)
  csv <- rbind(csv, csv_inc)
  wrote <- wrote + 1
  cat("\n Writing", loc, ";", wrote,  "of", length(unique(all_chronic[,location_id])), "!!! \n")
  
  fwrite(csv, paste0(draws_dir, "/chronic/", loc, ".csv"))
  
}