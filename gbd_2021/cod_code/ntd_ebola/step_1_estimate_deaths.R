# Purpose: estimate deaths for ebola
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
source("FILEPATH/get_demographics.R")
source(paste0(code_root, "FILEPATH/processing.R")) # function to make zero draw files
source(paste0(code_root, "FILEPATH/utils.R")) #custom ebola functions

run_file <- fread(paste0(data_root, 'FILEPATH'))
run_path <- run_file[nrow(run_file), run_folder_path]

params_dir <- "FILEPATH/params"
draws_dir <- paste0(run_path, '/draws')
interms_dir <- paste0(run_path, '/interms')
logs_dir <- paste0(run_path, '/logs')

data_dir   <- paste0(params_dir, "/data")                            

########################################################################
########################################################################
###'[ ======================= MAIN EXECUTION ======================= ###
########################################################################
########################################################################

####################################################################
#' [Death Estimates]
####################################################################

#'[2) set round id
gbd_round_id <- ADDRESS

epi_demographics <- get_demographics('ADDRESS', gbd_round_id = gbd_round_id) 
years <- epi_demographics$year_id # estimate ebola for every year, not just estimation years
locs <- epi_demographics$location_id

#  -----------------------------------------------------------------------
# 1) write out non-endemic locations


#' [3) ensure correct data selected

ebola_death <- fread(paste0(data_dir, "/FILEPATH"))
wa_death       <- fread(paste0(data_dir, "FILEPATH"))

#combines ebola death and wa_death
shared_cols  <- names(ebola_death)[names(ebola_death) %in% names(wa_death)] #keep needed columns
ebola_death  <- ebola_death[, ..shared_cols]
ebola_death  <- rbind(ebola_death, wa_death)
ebola_death[, cases := mean]
ebola_death[, mean := NULL]
ebola_death[, value_age_group_id := age_group_id]

imported_death <- fread(paste0(data_dir, "FILEPATH"))
imported_death[, cases := mean]
imported_death[, mean := NULL]
imported_death[, value_age_group_id := age_group_id]

non_zero_locs   <- unique(c(ebola_death[,location_id], imported_death[,location_id]))  
zero_locs       <- locs[!(locs %in% non_zero_locs)]

wrote <- 0
zeros <- gen_zero_draws(model_id = NA, location_id = NA, measure_id = 1, metric_id = 1, year_id = years, gbd_round_id = gbd_round_id, team = 'ADDRESS')

for (loc in zero_locs){ 
  zero <- copy(zeros)
  zero[, location_id := loc]
  fwrite(zero, paste0(draws_dir, "/deaths/", loc, ".csv"))
  wrote <- wrote + 1
  cat("Wrote", wrote, "of", length(zero_locs), "!!! \n")
}

#  -----------------------------------------------------------------------
# 2) age/sex split

# previously separate dataset used to derive age proportions - now used observed case data
agesplit_data <- copy(ebola_death)

#' [ssa_props]     

ssa_props          <- calc_as_proportion(data = agesplit_data)

# add age groups for older adults (what one all age/all sex death will go to)
ssa_props_m        <- ssa_props[sex_id == 1]
ssa_props_m_addval <- ssa_props_m[value_age_group_id == 20, proportions / 5]
ssa_props_m_add    <- expand.grid(value_age_group_id = c(20, 30, 31, 32, 235), sex_id = 1, proportions = ssa_props_m_addval)

ssa_props_f        <- ssa_props[sex_id == 2]
ssa_props_f_addval <- ssa_props_f[value_age_group_id == 19, proportions / 6]
ssa_props_f_add    <- expand.grid(value_age_group_id = c(19, 20, 30, 31, 32, 235), sex_id = 2, proportions = ssa_props_f_addval)

# remove old and bind new
ssa_props <- ssa_props[!((sex_id == 1 & value_age_group_id == 20) | (sex_id == 2 & value_age_group_id == 19))]
ssa_props <- rbind(ssa_props, ssa_props_m_add, ssa_props_f_add)

#' [sfa_props]  #all age female deaths will go to

sfa_props        <- calc_ss_proportion(data = agesplit_data, s_id = 2)

sfa_props_addval <- sfa_props[value_age_group_id == 19, proportions / 6]
sfa_props_add    <- expand.grid(value_age_group_id = c(19, 20, 30, 31, 32, 235), proportions = sfa_props_addval)
sfa_props        <- sfa_props[value_age_group_id != 19]

sfa_props <- rbind(sfa_props, sfa_props_add)

#' [sma_props]  #all age male deaths will go to

sma_props        <- calc_ss_proportion(data = agesplit_data, s_id = 1)

sma_props_addval <- sma_props[value_age_group_id == 20, proportions / 5]
sma_props_add    <- expand.grid(value_age_group_id = c(20, 30, 31, 32, 235), proportions = sma_props_addval)
sma_props        <- sma_props[value_age_group_id != 20]

sma_props <- rbind(sma_props, sma_props_add)

fwrite(ssa_props, paste0(interms_dir, "/ssa_props.csv"))
fwrite(sfa_props, paste0(interms_dir, "/sfa_props.csv"))
fwrite(sma_props, paste0(interms_dir, "/sma_props.csv"))

#' [2a) age/sex split imported deaths]

split_imp <- copy(ssa_props)
split_imp <- rbind(copy(split_imp[, "location_name" := "Mali"]), 
                   copy(split_imp[, "location_name" := "Rivers"]),
                   copy(split_imp[, "location_name" := "Lagos"]))

split_imp <- merge(split_imp, imported_death[location_name %in% c("Mali", "Rivers", "Lagos")], by = "location_name")

split_imp[, cases := cases * proportions]                              # apply age-sex proportions
split_imp[, c("proportions", "value_age_group_id.y", 'sex_id.y') := NULL]  # remove old age-sex columns and proportions
setnames(split_imp, c("value_age_group_id.x", "sex_id.x"), c("age_group_id", "sex_id"))
split_imp[, value_age_group_id := age_group_id]

imported_death_split  <- rbind(imported_death, split_imp, fill = TRUE)
imported_death_split  <- imported_death_split[sex_id != 3]
imported_death_split[, sex := NULL]

# 2b) age/sex split non-imported deaths

#split all sex all age 

all_aa       <- ebola_death[sex_id == 3 & age_group_id == 22]
as_split     <- as_age_split(all_age_data = all_aa, all_age_proportions = ssa_props)
as_split[, age_group_id := value_age_group_id]

male_aa      <- ebola_death[sex_id == 1 & age_group_id == 22]
male_split   <- ss_age_split(ss_all_age_data = male_aa, ss_age_proportions = sma_props)
male_split[, age_group_id := value_age_group_id]

female_aa    <- ebola_death[sex_id == 2 & age_group_id == 22]
female_split <- ss_age_split(ss_all_age_data = female_aa, ss_age_proportions = sfa_props)  
female_split[, age_group_id := value_age_group_id]

#combine all splits

ebola_death_split <- rbind(female_split, as_split, male_split, ebola_death, fill= TRUE)
ebola_death_split <- ebola_death_split[age_group_id != 22]

#  -----------------------------------------------------------------------
# 3) write imported death csvs

for (draw in 0:999){
  imported_death_split[, paste0("draw_", draw) := cases]
}

imported_death_split_draws <- copy(imported_death_split) #pull out to be used in estimation

wrote <- 0

for (loc in unique(imported_death_split[,location_id])){
  zero <- copy(zeros)
  zero[, location_id := loc]
  data <- imported_death_split[location_id == loc]
  csv <- inserter(zero_draw_file = zero, new_data = data, loc = loc, m_id = 1)
  wrote <- wrote + 1
  cat("\n Writing", loc, ";", wrote,  "of", length(unique(imported_death_split[,location_id])), "!!! \n")
  fwrite(csv, paste0(draws_dir, "/deaths/", loc, ".csv"))
}

cat("Finished all in :", as.character(unique(imported_death_split[,location_id])))

#  -----------------------------------------------------------------------
# 4) apply ur draws and write out endemic deaths

ur <- fread(paste0(draws_dir, "FILEPATH"), stringsAsFactors = FALSE)

ebola_death_split <- apply_ur(data = ebola_death_split, ur = ur)
ebola_death_split_draws <- copy(ebola_death_split) #pull out to be used in estimation

wrote <- 0

for (loc in unique(ebola_death_split[,location_id])){
  zero <- copy(zeros)
  zero[, location_id := loc]
  data <- ebola_death_split[location_id == loc]
  csv <- inserter(zero_draw_file = zero, new_data = data, loc = loc, m_id = 1)
  
  wrote <- wrote + 1
  cat("\n Writing", loc, ";", wrote,  "of", length(unique(ebola_death_split[,location_id])), "!!! \n")
  
  fwrite(csv, paste0(draws_dir, "/deaths/", loc, ".csv"))
}

cat("Finished all in :", as.character(unique(ebola_death_split[,location_id])))

#  -----------------------------------------------------------------------
# 5) write out split datasets to interms to estimate

imported_death_split_draws[, year_id := as.character(year_start)]
shared_cols <- names(imported_death_split_draws)[names(imported_death_split_draws) %in% names(ebola_death_split_draws)]
all_death   <- rbind(imported_death_split_draws[, ..shared_cols], ebola_death_split_draws[, ..shared_cols])

fwrite(all_death, paste0(interms_dir, "/all_death_draws.csv"))
