# estimate survivors
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-"FILEPATH"
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common IHME IO Paths
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
  params_dir <- "FILEPATH"
  draws_dir <- "FILEPATH"
  interms_dir <- "FILEPATH"
  logs_dir <- "FILEPATH"
}

# Source relevant libraries
library(data.table)
library(stringr)
library(argparse)
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
my_shell <- "FILEPATH"
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH") # function to make zero draw files
source("FILEPATH/ntd_ebola/child_scripts/utils.R")) #custom ebola functions

run_file <- fread("FILEPATH")
run_path <- run_file[nrow(run_file), run_folder_path]

params_dir <- "FILEPATH"
draws_dir <- "FILEPATH"
interms_dir <- "FILEPATH"
logs_dir <- "FILEPATH"

# seed
data_dir   <- "FILEPATH"    

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
# 4) estimate ADDRESS1 -- acute with death and surv
# 5) estimate ADDRESS2 -- chronic with only surv
# 6) Submit results

#'[2) set round id
gbd_round_id <- 7
decomp_step <- 'step1'

epi_demographics <- get_demographics('epi',gbd_round_id = gbd_round_id) 
years <- epi_demographics$year_id[1]:epi_demographics$year_id[length(epi_demographics$year_id)] # estimate ebola for every year, not just estimation years
locs <- epi_demographics$location_id

#  -----------------------------------------------------------------------
# 1) write out non-endemic locations -- anywhere with 0 imported cases & deaths, 0 end cases & deathss

all_data <- fread("FILEPATH")    ## endemic survivors -- data

imported_surv    <- all_data[imported ==1]
all_death <- fread("FILEPATH")    ## all deaths -- draws

# get non_zero_locations
end_surv        <- all_data[imported == 0]
end_surv        <- end_surv[mean > 0]
non_zero_locs   <- unique(c(imported_surv[,location_id], end_surv[,location_id]))  

# zero_locs for ADDRESS1 and any not in death OR surv, ADDRESS2 any not in surv only; found that surv includes all of death location ids too
zero_locs       <- locs[!(locs %in% non_zero_locs)]

wrote <- 0
zeros <- gen_zero_draws(modelable_entity_id = NA, location_id = NA, measure_id = c(5,6), metric_id = 1, year_id = years, gbd_round_id = gbd_round_id, team = 'epi')
zeros[, metric_id := NULL]

for (loc in zero_locs){ 
  zero <- copy(zeros)
  zero[, location_id := loc]
  fwrite(zero, "FILEPATH/ADDRESS1/FILEPATH.csv"))
  fwrite(zero, "FILEPATH/ADDRESS2/FILEPATH.csv"))
  wrote <- wrote + 1
}

#  -----------------------------------------------------------------------
# 2) age-sex split endemic and imported cases

agesplit_data <- copy(end_surv[age_spec == 1])

ssa_props <- calc_as_proportion(data = agesplit_data)
sfa_props <- calc_ss_proportion(data = agesplit_data, s_id = 2)
sma_props <- calc_ss_proportion(data = agesplit_data, s_id = 1)

split_imp <- copy(ssa_props)
split_imp <- rbind(copy(split_imp[, "location_name" := "Mali"]), 
                   copy(split_imp[, "location_name" := "Rivers"]),
                   copy(split_imp[, "location_name" := "Lagos"]),
                   copy(split_imp[, "location_name" := "Italy"]))

split_imp <- merge(split_imp, imported_surv[location_name %in% c("Mali", "Rivers", "Lagos", "Italy")], by = "location_name")

split_imp[, mean := mean * proportions]                              # apply age-sex proportions
split_imp[, c("proportions", "age_group_id.y", "sex_id.y") := NULL]  # remove old age-sex columns and proportions
setnames(split_imp, c("age_group_id.x", "sex_id.x"), c("age_group_id", "sex_id"))

imported_surv_split  <- rbind(imported_surv, split_imp)
imported_surv_split  <- imported_surv_split[sex_id != 3]

# put in draw space for calculation of ADDRESS1 and ADDRESS2

for (draw in 0:999){
  imported_surv_split[, paste0("draw_", draw) := mean]
}

# 2b) age/sex split non-imported deaths

#split all sex all age 

all_aa       <- copy(end_surv[sex_id == 3 & age_group_id == 22])
as_split     <- as_age_split(all_age_data = all_aa, all_age_proportions = ssa_props)

male_aa      <- copy(end_surv[sex_id == 1 & age_group_id == 22])
male_split   <- ss_age_split(ss_all_age_data = male_aa, ss_age_proportions = sma_props)

female_aa    <- copy(end_surv[sex_id == 2 & age_group_id == 22])
female_split <- ss_age_split(ss_all_age_data = female_aa, ss_age_proportions = sfa_props)  

#combine all splits
ebola_surv_split  <- rbind(female_split, as_split, male_split, end_surv)
ebola_surv_split <-  ebola_surv_split[age_group_id != 22 & sex_id != 3] 

#  -----------------------------------------------------------------------
# 3) apply UR to endemic survivors

ur <- fread("FILEPATH"), stringsAsFactors = FALSE)

ebola_surv_split <- apply_ur(data = ebola_surv_split, ur = ur)

#  -----------------------------------------------------------------------
# 4) estimate ADDRESS1 

# get all survivors and deaths in one table
ebola_surv_split[, measure_id := "S"]
imported_surv_split[, measure_id := as.character(measure_id)][, measure_id := "S"]
imported_surv_split[, year_id := as.character(year_start)]
all_death[, measure_id := "1"]

shared_cols <- names(imported_surv_split)[(names(imported_surv_split) %in% names(ebola_surv_split))]
shared_cols <- shared_cols[shared_cols %in% names(all_death)]

all_draws   <- rbind(all_death[, ..shared_cols], ebola_surv_split[, ..shared_cols], imported_surv_split[, ..shared_cols])

prev_ADDRESS1   <- copy(all_draws)
acute_dur   <- fread("FILEPATH")

#estimate death
prev_ADDRESS1_death <- copy(prev_ADDRESS1[measure_id == "1"])
inc_ADDRESS1_death        <- copy(prev_ADDRESS1_death)

dur_death       <- acute_dur[group == "acute_death", ]
dur_inc <- copy(dur_death)
dur_inc <- dur_inc[, paste0("draw_", 0:999) := 1]

prev_ADDRESS1_death <- cases_to_prev(prev_ADDRESS1_death, dur_death, pop_decomp_step = decomp_step, round_id = gbd_round_id)
inc_ADDRESS1_death  <- cases_to_prev(inc_ADDRESS1_death, dur_inc, pop_decomp_step = decomp_step, round_id = gbd_round_id)

#estimate survivors (different duration)
prev_ADDRESS1_surv  <- copy(prev_ADDRESS1[measure_id == "S"])
inc_ADDRESS1_surv   <- copy(prev_ADDRESS1_surv)

dur_surv        <- acute_dur[group == "acute_survival", ]
prev_ADDRESS1_surv  <- cases_to_prev(prev_ADDRESS1_surv, dur_surv, pop_decomp_step = decomp_step, round_id = gbd_round_id)
inc_ADDRESS1_surv  <- cases_to_prev(inc_ADDRESS1_surv, dur_inc, pop_decomp_step = decomp_step, round_id = gbd_round_id)

# incidence for ADDRESS1

# bind, aggregate, and write

all_ADDRESS1_prev <- rbind(prev_ADDRESS1_death, prev_ADDRESS1_surv)
all_ADDRESS1_prev <- agg_asldy(all_ADDRESS1_prev) #function sums prevalence by age sex loc year draw

all_ADDRESS1_inc <- rbind(inc_ADDRESS1_death, inc_ADDRESS1_surv)
all_ADDRESS1_inc <- agg_asldy(all_ADDRESS1_inc) #function sums prevalence by age sex loc year draw

all_ADDRESS1_prev[, measure_id := 5]
all_ADDRESS1_inc[, measure_id := 6]

wrote <- 0

for (loc in unique(all_ADDRESS1_prev[,location_id])){
  data_inc <- copy(all_ADDRESS1_inc[location_id == loc])
  data_prev <- copy(all_ADDRESS1_prev[location_id == loc])
  
  zero <- copy(zeros)
  zero[,location_id := loc]
  zero_prev <- copy(zero[measure_id == 5])
  zero_inc <- copy(zero[measure_id == 6])
  csv   <- inserter(zero_draw_file = zero_prev, new_data = data_prev, loc = loc, m_id = 5)
  csv_inc   <- inserter(zero_draw_file = zero_inc, new_data = data_inc, loc = loc, m_id = 6)
  csv <- rbind(csv, csv_inc)
  wrote <- wrote + 1
  fwrite(csv, "FILEPATH/ADDRESS1/FILEPATH.csv"))
}

############################################

# 5) estimate ADDRESS2 -- just survivors

all_surv          <- rbind(ebola_surv_split[, ..shared_cols], imported_surv_split[, ..shared_cols])
chronic_dur_yr1   <- fread("FILEPATH")
chronic_dur_yr2   <- fread("FILEPATH")
chronic_dur_inc   <- copy(chronic_dur_yr1)
chronic_dur_inc[, paste0("draw_", 0:999) := 1]

# estimate year 1
cases     <- copy(all_surv)

# ** CHECK TYPE on melt
prev_ADDRESS2_yr1     <- cases_to_prev(cases, chronic_dur_yr1, pop_decomp_step = decomp_step, round_id = gbd_round_id)
inc_ADDRESS2_yr1     <- cases_to_prev(cases, chronic_dur_inc, pop_decomp_step = decomp_step, round_id = gbd_round_id)

# estimate year 2 
prev_ADDRESS2_yr2     <- cases_to_prev(cases, chronic_dur_yr2, pop_decomp_step = decomp_step, round_id = gbd_round_id)
prev_ADDRESS2_yr2[, year_id := year_id + 1] # set year to in front

# bind, aggregate, and write
all_ADDRESS2 <- rbind(prev_ADDRESS2_yr1, prev_ADDRESS2_yr2)
all_ADDRESS2 <- agg_asldy(all_ADDRESS2)

all_ADDRESS2_inc <- inc_ADDRESS2_yr1
all_ADDRESS2_inc <- agg_asldy(all_ADDRESS2_inc)

all_ADDRESS2[, measure_id := 5]
all_ADDRESS2_inc[, measure_id := 6]

wrote <- 0

for (loc in unique(all_ADDRESS2[,location_id])){
  data_inc <- copy(all_ADDRESS2_inc[location_id == loc])
  data_prev <- copy(all_ADDRESS2[location_id == loc])
  
  zero <- copy(zeros)
  zero[,location_id := loc]
  zero_prev <- copy(zero[measure_id == 5])
  zero_inc <- copy(zero[measure_id == 6])
  csv   <- inserter(zero_draw_file = zero_prev, new_data = data_prev, loc = loc, m_id = 5)
  csv_inc <- inserter(zero_draw_file = zero_inc, new_data = data_inc, loc = loc,  m_id = 6)
  csv <- rbind(csv, csv_inc)
  wrote <- wrote + 1
  
  fwrite(csv, "FILEPATH/ADDRESS2/FILEPATH.csv")
  
}
