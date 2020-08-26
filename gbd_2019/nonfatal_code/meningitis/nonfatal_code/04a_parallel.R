###############################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Author:		
## Last updated:	2:35 PM 8/20/2014
## Description:	Parallelization of 04a_dismod_prep_wmort; edited to include ODE solver ready files in 2017
###############################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
library(argparse)
library(data.table)
library(dplyr)

# Get arguments from parser
parser <- ArgumentParser()
parser$add_argument("--date", help = "timestamp of current run (i.e. 2014_01_17)", default = NULL, type = "character")
parser$add_argument("--step_num", help = "step number of this step (i.e. 01a)", default = NULL, type = "character")
parser$add_argument("--step_name", help = "name of current step (i.e. first_step_name)", default = NULL, type = "character")
parser$add_argument("--location", help = "location", default = NULL, type = "integer")
parser$add_argument("--code_dir", help = "code directory", default = NULL, type = "character")
parser$add_argument("--in_dir", help = "directory for external inputs", default = NULL, type = "character")
parser$add_argument("--out_dir", help = "directory for this steps checks", default = NULL, type = "character")
parser$add_argument("--tmp_dir", help = "directory for this steps intermediate draw files", default = NULL, type = "character")
parser$add_argument("--root_j_dir", help = "base directory on J", default = NULL, type = "character")
parser$add_argument("--root_tmp_dir", help = "base directory on clustertmp", default = NULL, type = "character")
parser$add_argument("--ds", help = "specify decomp step", default = 'step1', type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)

# Get location from parameter map
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
parameters <- fread(file.path(code_dir, paste0(step_num, "_parameters.csv")))
location <- parameters[task_id, location_id]

# User specified options -------------------------------------------------------
# Source GBD 2019 Shared Functions
source(paste0(code_dir, "helper_functions/source_functions.R"))
k <- # filepath
sourceDir(paste0(k, "current/r/"))
source(paste0(k, "current/r/get_age_metadata.R"))

functional <- "meningitis"

pull_epilepsy <- T
pull_long_modsev <- T

if (pull_epilepsy & pull_long_modsev) {
  groups <- c("long_modsev", "epilepsy")
} else if (!pull_epilepsy & pull_long_modsev) {
  groups <- c("long_modsev")
} else if (pull_epilepsy & !pull_long_modsev) {
  groups <- c("epilepsy")
} else {
  groups <- c()
}

pull_dir_03b <- file.path(root_tmp_dir, "03b_outcome_split", "03_outputs", "01_draws")

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id
ages <- demographics$age_group_id

# Inputs -----------------------------------------------------------------------
get_epilepsy_estimates <- function(gbd_id = 2403, meas_id, my_name) {
  draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 2403, measure_id = meas_id, age_group_id = ages, source = "epi", location_id = location, gbd_round_id = 6, decomp_step = ds)
  setDT(draws)
  # Calculate mean and standard deviation for each row
  draws <- cbind(draws, meas_value = rowMeans(draws[, paste0("draw_", 0:999)]), meas_stdev = apply(draws[, paste0("draw_", 0:999)], MARGIN = 1, FUN = sd))
  draws[, paste0("draw_", 0:999):= NULL]
  cols.remove <- c("measure_id", "model_version_id", "modelable_entity_id")
  draws[, c(cols.remove) := NULL]
  draws$measure <- my_name
  for (y in years) {
    for (s in sexes) {
      draws.tmp <- draws[sex_id == s & year_id == y]
      saveRDS(draws.tmp, file.path(out_dir, "02_temp", "03_data", paste0("epilepsy_", my_name, "_", location, "_", y, "_" ,s, ".rds")))
    }
  }
}

if (pull_epilepsy) {
  # Get estimates from epilepsy (emr) - new for ODE solver and save for all years/sex
  get_epilepsy_estimates(gbd_id = 2403, meas_id = 9, my_name = "mtexcess")
  # Get estimates from epilepsy (remission) - new for ODE solver and save for all years/sex
  get_epilepsy_estimates(gbd_id = 2403, meas_id = 7, my_name = "remission")
}

# Use get_envelope to get run_id for the current run for life tables
envelope <- get_envelope(gbd_round_id = 6, decomp_step = ds, with_shock = 1, with_hiv = 1)
# get life table for current run_id
life.table.dt <- fread(paste0("filepath", envelope$run_id, "/env_wshock/final_formatted/env_", location, ".csv"), header = T)
# Drop age group 22 (all ages) since there was an issue aggregating
life.table.dt <- life.table.dt[age_group_id != 22]

# Prepare SMR file of neonatal encephalopathy to be attached to all long_modsev location/year/sex
cp.mort.dt <- fread(file.path(in_dir, paste0(step_num, '_', step_name), "CP Mortality update for GBD2013.csv"))
cp.mort.dt <- cp.mort.dt[cause =="NE"] # Neonatal encephalopathy
cols.remove <- c("cause", "parameter", "mean_og")
cp.mort.dt[, (cols.remove):= NULL]
# ------------------------------------------------------------------------------

# Run job ----------------------------------------------------------------------
# Pull in population data and merge (update this when new life_tables are up)
pop.dt <- get_population(year_id = years, location_id = location, sex_id = sexes, age_group_id = ages, gbd_round_id = 6, decomp_step = ds)
life.table.dt <- merge(life.table.dt, pop.dt[, !"location_id"], by=c("year_id", "age_group_id", "sex_id"))
life.table.dt[, run_id := NULL]

# Calculate mortality rate
life.table.dt <- life.table.dt[ , paste0("mort_", 0:999):= lapply(0:999, function(x){ get(paste0("env_", x)) / population })]
cols.remove <- paste0("env_", 0:999)
life.table.dt[, (cols.remove) := NULL]
life.table.dt$age <- ifelse(life.table.dt$age_group_id < 9, "young", "old")

# Input code for smr --> excess mortality calculation here
# This code generates 1000 samples from a normal distribution with the means and sd incidated in the above CP csv for both age groups
mu_young <- cp.mort.dt[age_start == 0, mean]
se_young <- cp.mort.dt[age_start == 0, se]
smr_young <- rnorm(n = 1000, mu_young, se_young)
mu_old <- cp.mort.dt[age_start == 20, mean]
se_old <- cp.mort.dt[age_start == 20, se]
smr_old <- rnorm(n = 1000, mu_old, se_old)

smr_draws <- rbind(smr_young, smr_old)
colnames(smr_draws) <- paste0("smr_", 0:999)

cp.mort.dt <- cbind(cp.mort.dt, smr_draws)
# cp.mort.dt <- cp.mort.dt[, paste0("smr_", 0:999) := transpose(as.data.table(apply(cp.mort.dt, 1, function(x) rnorm(1000, mean = x[2], sd = x[3]))))]
cp.mort.dt$age <- ifelse(cp.mort.dt$age_start == 20, "old", "young")

# Merge smr to mortality data
cp.mort.dt <- merge(cp.mort.dt, life.table.dt, by="age", all.y = T)

# Generating EMR draws
cp.mort.dt <- cp.mort.dt[, paste0("emr_", 0:999) := lapply(0:999, function(x) {
                get(paste0("mort_", x)) * (get(paste0("smr_", x)) - 1)
              })]

# Regenerate mean and CIs for DisMod
cp.mort.dt[, mean := NULL]
cp.mort.dt <- cbind(cp.mort.dt, 
                    meas_value = rowMeans(cp.mort.dt[,paste0("emr_", 0:999)]), 
                    meas_stdev = apply(cp.mort.dt[,paste0("emr_", 0:999)], MARGIN = 1, FUN = sd))
cp.mort.dt <- cp.mort.dt[, c("lower", "upper") := 
                           transpose(as.data.table(apply(cp.mort.dt[,paste0("emr_", 0:999) ], 
                                                         MARGIN = 1, 
                                                         FUN = quantile, 
                                                         probs = c(.025, .975))))]
cp.mort.dt <- cp.mort.dt[, c("location_id", "age_group_id", "year_id", "sex_id", "meas_value", "meas_stdev", "lower", "upper")]
cp.mort.dt <- cp.mort.dt[order(age_group_id, year_id, sex_id)]
# Create dummy columns so that you can row bind later on
cp.mort.dt$measure <- "mtexcess"
cp.mort.dt$etiology <- 'dummy_etiology'
cp.mort.dt$modelable_entity_id <- 'dummy_meid'
cp.mort.dt$modelable_entity_name <- 'dummy_mename'
cp.mort.dt$grouping <- 'dummy_grouping'
cp.mort.dt$metric_id <- 'dummy_metric_id'

for (y in years) {
  for (s in sexes) {
    tmp.cp.mort.dt <- cp.mort.dt[year_id == y & sex_id == s]
    saveRDS(tmp.cp.mort.dt, file.path(out_dir, "02_temp", "03_data", paste0("smr_",location, "_", y, "_", s, ".rds")))
  }
}

for (y in years) {
  for (s in sexes) {
    for (g in groups){
      incid.dt <- readRDS(file.path(pull_dir_03b, g, paste0(location,"_", y,"_", s,".rds")))
      setDT(incid.dt)
      # Create DisMod input parameters
      incid.dt <- cbind(incid.dt, meas_value = rowMeans(incid.dt[,paste0("draw_", 0:999)]), 
                        meas_stdev = apply(incid.dt[,paste0("draw_", 0:999)], MARGIN = 1, FUN = sd))
      incid.dt <- incid.dt[, c("lower", "upper") := 
                             transpose(as.data.table(apply(incid.dt[,paste0("draw_", 0:999) ],
                                                           MARGIN = 1, 
                                                           FUN = quantile, 
                                                           probs = c(.025, .975))))]
      cols.remove <- c(paste0("draw_", 0:999), "measure_id")
      incid.dt[, (cols.remove) := NULL]
      incid.dt$measure = "incidence"
      
      if (g == "epilepsy") {
        mtexcess.dt <- readRDS(file.path(out_dir, "02_temp", "03_data", paste0("epilepsy_mtexcess_", location, "_", y, "_", s, ".rds")))
        remission.dt <- readRDS(file.path(out_dir, "02_temp", "03_data", paste0("epilepsy_remission_", location, "_", y, "_", s, ".rds")))
        incid.dt <- rbind(incid.dt, mtexcess.dt, fill = T)
        incid.dt <- rbind(incid.dt, remission.dt, fill = T)
      } else if (g == "long_modsev") {
        smr.dt <- readRDS(file.path(out_dir, "02_temp", "03_data", paste0("smr_",location, "_", y, "_", s, ".rds")))
        incid.dt <- rbind(incid.dt, smr.dt, fill = T)
      }
      # Add age start and end for each age group using get_age_metadata
      age.start.end.dt <- get_age_metadata(age_group_set_id=12, gbd_round_id=6)
      age.start.end.dt$age_group_weight_value <- NULL
      
      # change highest age group 235 so that it is 95 - 99
      age.start.end.dt[age.start.end.dt$age_group_id==235, "age_group_years_end"] <- 99
      
      incid.dt <- merge(incid.dt, age.start.end.dt, by = "age_group_id")
      
      cols.remove <- c("modelable_entity_name", "age_group_id", "modelable_entity_id", "location_id", "year_id", "sex_id", "lower", "upper", "etiology", "grouping")
      incid.dt[, (cols.remove) := NULL]
      setnames(incid.dt, "measure", "integrand")
      incid.dt$subreg <- "none"
      incid.dt$region <- "none"
      incid.dt$super <- "none"
      incid.dt$x_ones <- 1
      # rename columns to fit DisMod parameters
      incid.dt <- incid.dt %>% rename(age_lower = age_group_years_start, age_upper = age_group_years_end)
      setcolorder(incid.dt, c("integrand", "meas_value", "meas_stdev", "age_lower", "age_upper", "subreg", "region", "super", "x_ones", "metric_id"))
      dir.create(file.path(tmp_dir, "03_outputs", "01_draws", location), showWarnings = F)
      write.csv(incid.dt, file.path(tmp_dir, "03_outputs", "01_draws", location, paste0(g, '_', location, '_', y, '_', s, '.csv')), row.names=F)
    }
    
  }
}

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0(tmp_dir,"/02_temp/01_code/checks/","finished_loc", location, ".txt"), overwrite=T)
