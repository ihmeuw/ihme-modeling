###############################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Description:	Parallelization of 04a_dismod_prep_wmort
###############################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
pacman::p_load(R.utils, data.table, dplyr, rhdf5)

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)
# ------------------------------------------------------------------------------

# User specified options -------------------------------------------------------
# SOURCE SHARED FUNCTIONS ------------------------------
groups <- c("epilepsy", "long_modsev")

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path("FILEPATH"))
years <- demographics$year_id
sexes <- demographics$sex_id
ages <- demographics$age_group_id

# Inputs -----------------------------------------------------------------------
pull_dir_03b <- file.path("FILEPATH")

# Set step-specific output directories
tmp_dir <- paste0(tmp_dir, step_num, "_", step_name,"/")
out_dir <- paste0(out_dir, step_num, "_", step_name,"/")

envelope <- fread(file.path("FILEPATH"))
# get life table for current run_id
new_locs <- c(60908, 94364, 95069)

if (location %in% new_locs){
life.table.dt <- fread(paste0("FILEPATH"), header = T)
life.table.dt <- life.table.dt[, location_id := location]
} else {
  life.table.dt <- fread(paste0("FILEPATH"), header = T)
}
# Subset to desired location & Drop all ages since there was an issue aggregating
life.table.dt <- life.table.dt[location_id == location & age_group_id != 22]

# Make life.table.dt wide with draws
life.table.dt <- life.table.dt[,. (location_id, age_group_id, sex_id, year_id, draw, mx)]
life.table.dt <- dcast(life.table.dt, location_id + age_group_id + sex_id + year_id ~ draw, value.var="mx")
setnames(life.table.dt, old = paste0(0:999), new = paste0("mort_", 0:999))

# ------------------------------------------------------------------------------

# Run job ----------------------------------------------------------------------
# Add age_group_years_start and age_group_years_end to sort by young/old
age_meta <- fread(file.path("FILEPATH"))
life.table.dt <- merge(life.table.dt, age_meta, by = "age_group_id")
life.table.dt$age <- ifelse(life.table.dt$age_group_years_start < 20, "young", "old")

# Input code for smr --> excess mortality calculation here
# Prepare SMR file of neonatal encephalopathy to be attached to all long_modsev location/year/sex
cp.mort.dt <- fread(file.path("FILEPATH"), "CP Mortality update for GBD2013.csv")
cp.mort.dt <- cp.mort.dt[cause =="NE"] # Neonatal encephalopathy
cols.remove <- c("cause", "parameter", "mean_og")
cp.mort.dt[, (cols.remove) := NULL]
mu_young <- cp.mort.dt[age_start == 0, mean]
se_young <- cp.mort.dt[age_start == 0, se]
smr_young <- rnorm(n = 1000, mu_young, se_young)

mu_old <- cp.mort.dt[age_start == 20, mean]
se_old <- cp.mort.dt[age_start == 20, se]
smr_old <- rnorm(n = 1000, mu_old, se_old)

smr_draws <- rbind(smr_young, smr_old)
colnames(smr_draws) <- paste0("smr_", 0:999)

cp.mort.dt <- cbind(cp.mort.dt, smr_draws)

cp.mort.dt$age <- ifelse(cp.mort.dt$age_start == 20, "old", "young")

# Merge smr to mortality data
cp.mort.dt <- merge(cp.mort.dt, life.table.dt, by="age", all.y = T)

# Generating EMR draws
cp.mort.dt <- cp.mort.dt[, paste0("emr_", 0:999) := lapply(0:999, function(x) {
              get(paste0("mort_", x)) * (get(paste0("smr_", x)) - 1)
            })]

# Regenerate mean and CIs for DisMod
cp.mort.dt[, mean:= NULL]
cp.mort.dt <- cbind(cp.mort.dt, meas_value = rowMeans(cp.mort.dt[,paste0("emr_", 0:999)]), meas_stdev = apply(cp.mort.dt[,paste0("emr_", 0:999)], MARGIN = 1, FUN = sd))
cp.mort.dt <- cp.mort.dt[, c("lower", "upper") := transpose(as.data.table(apply(cp.mort.dt[,paste0("emr_", 0:999) ], MARGIN = 1, FUN = quantile, probs = c(.025, .975))))]
cp.mort.dt <- cp.mort.dt[, c("location_id", "age_group_id", "year_id", "sex_id", "meas_value", "meas_stdev", "lower", "upper")]
cp.mort.dt <- cp.mort.dt[order(age_group_id, year_id, sex_id)]
# Create dummy columns so that you can row bind later on
cp.mort.dt$measure <- "mtexcess"
cp.mort.dt$metric_id <- 3

for (y in years) {
  for (s in sexes) {
    for (g in groups){
      incid.dt <- readRDS(file.path("FILEPATH"))
      setDT(incid.dt)
      # Create DisMod input parameters
      incid.dt <- cbind(incid.dt, meas_value = rowMeans(incid.dt[,paste0("draw_", 0:999)]), meas_stdev = apply(incid.dt[,paste0("draw_", 0:999)], MARGIN = 1, FUN = sd))
      incid.dt <- incid.dt[, c("lower", "upper"):= transpose(as.data.table(apply(incid.dt[,paste0("draw_", 0:999) ], MARGIN = 1, FUN = quantile, probs = c(.025, .975))))]
      cols.remove <- c(paste0("draw_", 0:999), "measure_id")
      incid.dt[, (cols.remove) := NULL]
      incid.dt$measure = "incidence"

      if (g == "epilepsy") {
        file <- list.files(file.path("FILEPATH"), full.names = T)[list.files(file.path("FILEPATH"), full.names = T) %like% "epilepsy"]
        epilepsy_draws <- fread(file)
        incid.dt <- rbind(incid.dt, epilepsy_draws[sex_id == s & year_id == y & location_id == location], fill = TRUE)
      } else if (g == "long_modsev") {
        smr.dt <- cp.mort.dt[year_id == y & sex_id == s]
        incid.dt <- rbind(incid.dt, smr.dt, fill=T)
      }
      
      # Add age start and end for each age group 
      age.start.end.dt <- fread(file.path("FILEPATH"))

      # change highest age group so that it is 95 - 99
      age.start.end.dt[age.start.end.dt$age_group_id==235, "age_group_years_end"] <- 99
      
      incid.dt <- merge(incid.dt, age.start.end.dt, by="age_group_id") 
      
            # dtsoi -- added next lines to change to ODE solver format
      cols.remove <- c( "age_group_id", "age_group_weight_value", "location_id", "year_id", "sex_id", "lower", "upper", "grouping")
      incid.dt[, (cols.remove) := NULL]
      setnames(incid.dt, "measure", "integrand")
      incid.dt$subreg <- "none"
      incid.dt$region <- "none"
      incid.dt$super <- "none"
      incid.dt$x_ones <- 1
      # rename columns to fit DisMod parameters
      incid.dt <- incid.dt %>% rename(age_lower = age_group_years_start, age_upper = age_group_years_end)
      setcolorder(incid.dt, c("integrand", "meas_value", "meas_stdev", "age_lower", "age_upper", "subreg", "region", "super", "x_ones", "metric_id"))
      dir.create(file.path("FILEPATH"), showWarnings = F)
      write.csv(incid.dt, file.path("FILEPATH"), row.names=F)
    }
  }
}
# ------------------------------------------------------------------------------

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create("FILEPATH", overwrite=T)
# ------------------------------------------------------------------------------
