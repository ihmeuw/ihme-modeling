# estimate deaths eola
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

########################################################################
########################################################################
###'[ ======================= MAIN EXECUTION ======================= ###
########################################################################
########################################################################

####################################################################
#' [Death Estimates]
####################################################################

 # 1) write-out non-endemic and non-imported locations
 # 2) age/sex-split imported and endemic deaths
 # 3) write imported death csvs (1,000 draws with same value)
 # 4) write endemic deaths csvs (1,000 draws from ur draws)
 # 5) write out death estimates for ADDRESS2

 # *** death estimates not saved -- see Cause of Death team

#'[2) set round id
gbd_round_id <- 7

epi_demographics <- get_demographics('cod', gbd_round_id = gbd_round_id) 
years <- epi_demographics$year_id   # estimate ebola for every year, not just estimation years
locs <- epi_demographics$location_id

#  -----------------------------------------------------------------------
# 1) write out non-endemic locations

# "re-extract" data have gbd 2020 age groups
# "endemic_deaths_11_26_19_update" includes the update

#' [3) ensure correct data selected

ebola_death <- fread("FILEPATH")    ##"re-extract" data
wa_death       <- fread("FILEPATH")  ## West African 2016 deaths with update

#combines ebola death and wa_death
shared_cols  <- names(ebola_death)[names(ebola_death) %in% names(wa_death)]    #keep needed columns
ebola_death  <- ebola_death[, ..shared_cols]
ebola_death  <- rbind(ebola_death, wa_death)

imported_death <- fread("FILEPATH")

non_zero_locs  <- unique(c(ebola_death[,location_id], imported_death[,location_id]))  
zero_locs      <- locs[!(locs %in% non_zero_locs)]

wrote <- 0
zeros <- gen_zero_draws(modelable_entity_id = NA, location_id = NA, measure_id = 1, metric_id = 1, year_id = years, gbd_round_id = gbd_round_id, team = 'cod')

for (loc in zero_locs){ 
  zero <- copy(zeros)
  zero[, location_id := loc]
  fwrite(zero, "FILEPATH")
  wrote <- wrote + 1
}

#  -----------------------------------------------------------------------
# 2) age/sex split

# previously separate dataset used to derive age proporitons - now used observed case data
agesplit_data <- copy(ebola_death)

#' [ssa_props]     

ssa_props          <- calc_as_proportion(data = agesplit_data)

# add age groups for older adults
ssa_props_m        <- ssa_props[sex_id == 1]
ssa_props_m_addval <- ssa_props_m[age_group_id == 20, proportions / 5]
ssa_props_m_add    <- expand.grid(age_group_id = c(20, 30, 31, 32, 235), sex_id = 1, proportions = ssa_props_m_addval)

ssa_props_f        <- ssa_props[sex_id == 2]
ssa_props_f_addval <- ssa_props_f[age_group_id == 19, proportions / 6]
ssa_props_f_add    <- expand.grid(age_group_id = c(19, 20, 30, 31, 32, 235), sex_id = 2, proportions = ssa_props_f_addval)

# remove old and bind new
ssa_props <- ssa_props[!((sex_id == 1 & age_group_id == 20) | (sex_id == 2 & age_group_id == 19))]
ssa_props <- rbind(ssa_props, ssa_props_m_add, ssa_props_f_add)

#' [sfa_props]  

sfa_props        <- calc_ss_proportion(data = agesplit_data, s_id = 2)

sfa_props_addval <- sfa_props[age_group_id == 19, proportions / 6]
sfa_props_add    <- expand.grid(age_group_id = c(19, 20, 30, 31, 32, 235), proportions = sfa_props_addval)
sfa_props        <- sfa_props[age_group_id != 19]

sfa_props <- rbind(sfa_props, sfa_props_add)

#' [sma_props]  

sma_props        <- calc_ss_proportion(data = agesplit_data, s_id = 1)

sma_props_addval <- sma_props[age_group_id == 20, proportions / 5]
sma_props_add    <- expand.grid(age_group_id = c(20, 30, 31, 32, 235), proportions = sma_props_addval)
sma_props        <- sma_props[age_group_id != 20]

sma_props <- rbind(sma_props, sma_props_add)

fwrite(ssa_props, "FILEPATH")
fwrite(sfa_props, "FILEPATH")
fwrite(sma_props, "FILEPATH")

#' [2a) age/sex split imported deaths]

split_imp <- copy(ssa_props)
split_imp <- rbind(copy(split_imp[, "location_name" := "Mali"]), 
                   copy(split_imp[, "location_name" := "Rivers"]),
                   copy(split_imp[, "location_name" := "Lagos"]))

split_imp <- merge(split_imp, imported_death[location_name %in% c("Mali", "Rivers", "Lagos")], by = "location_name")

split_imp[, mean := mean * proportions]                              # apply age-sex proportions
split_imp[, c("proportions", "age_group_id.y", "sex_id.y") := NULL]  # remove old age-sex columns and proportions
setnames(split_imp, c("age_group_id.x", "sex_id.x"), c("age_group_id", "sex_id"))

imported_death_split  <- rbind(imported_death, split_imp)
imported_death_split  <- imported_death_split[sex_id != 3]
imported_death_split[, sex := NULL]

# 2b) age/sex split non-imported deaths

#split all sex all age 

all_aa       <- ebola_death[sex_id == 3 & age_group_id == 22]
as_split     <- as_age_split(all_age_data = all_aa, all_age_proportions = ssa_props)

male_aa      <- ebola_death[sex_id == 1 & age_group_id == 22]
male_split   <- ss_age_split(ss_all_age_data = male_aa, ss_age_proportions = sma_props)

female_aa    <- ebola_death[sex_id == 2 & age_group_id == 22]
female_split <- ss_age_split(ss_all_age_data = female_aa, ss_age_proportions = sfa_props)  

#combine all splits

ebola_death_split <- rbind(female_split, as_split, male_split, ebola_death)
ebola_death_split <- ebola_death_split[age_group_id != 22]

#  -----------------------------------------------------------------------
# 3) write imported death csvs

for (draw in 0:999){
  imported_death_split[, paste0("draw_", draw) := mean]
}

imported_death_split_draws <- copy(imported_death_split)    #pull out to be used in ADDRESS2 estimation

wrote <- 0

for (loc in unique(imported_death_split[,location_id])){
  zero <- copy(zeros)
  zero[, location_id := loc]
  data <- imported_death_split[location_id == loc]
  csv <- inserter(zero_draw_file = zero, new_data = data, loc = loc, m_id = 1)
  wrote <- wrote + 1
  fwrite(csv, paste0(draws_dir, "/deaths/", loc, ".csv"))
}

#  -----------------------------------------------------------------------
# 4) apply ur draws and write out endemic deaths

ur <- fread("FILEPATH", stringsAsFactors = FALSE)    #under-reporting

ebola_death_split <- apply_ur(data = ebola_death_split, ur = ur)
ebola_death_split_draws <- copy(ebola_death_split) #pull out to be used in ADDRESS2 estimation

wrote <- 0

for (loc in unique(ebola_death_split[,location_id])){
  zero <- copy(zeros)
  zero[, location_id := loc]
  
  data <- ebola_death_split[location_id == loc]
  csv <- inserter(zero_draw_file = zero, new_data = data, loc = loc, m_id = 1)
  
  wrote <- wrote + 1
  
  fwrite(csv, "FILEPATH")
}

#  -----------------------------------------------------------------------
# 5) write out split datasets to interms to estimate ADDRESS2

imported_death_split_draws[, year_id := as.character(year_start)]
shared_cols <- names(imported_death_split_draws)[names(imported_death_split_draws) %in% names(ebola_death_split_draws)]
all_death   <- rbind(imported_death_split_draws[, ..shared_cols], ebola_death_split_draws[, ..shared_cols])

fwrite(all_death, "FILEPATH"))
