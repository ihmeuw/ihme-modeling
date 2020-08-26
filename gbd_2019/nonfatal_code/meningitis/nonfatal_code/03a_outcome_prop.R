#####################################################################################################################################################################################
## Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
## Author:		
## Last updated:	11/20/2018
#' @NOTE: At the start of each round make sure to run GDP_sequelae_prop_regression.do to create prop_major_all_locs_draws_gbd2019.csv in the input directory
## Description:	Setup draws for each outcome fraction for each etiology
## Number of output files: 20
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
library(argparse)
library(openxlsx)
pacman::p_load(data.table, dplyr)

# Get arguments from parser
parser <- ArgumentParser()
parser$add_argument("--root_j_dir", help = "base directory on J", default = NULL, type = "character")
parser$add_argument("--root_tmp_dir", help = "base directory on clustertmp", default = NULL, type = "character")
parser$add_argument("--date", help = "timestamp of current run (i.e. 2014_01_17)", default = NULL, type = "character")
parser$add_argument("--step_num", help = "step number of this step (i.e. 01a)", default = NULL, type = "character")
parser$add_argument("--step_name", help = "name of current step (i.e. first_step_name)", default = NULL, type = "character")
parser$add_argument("--hold_steps", help = "steps to wait for before running", default = NULL, nargs = "+", type = "character")
parser$add_argument("--last_steps", help = "step numbers for final steps that you are running in the current run (i.e. 04b)", default = NULL,  nargs = "+", type = "character")
parser$add_argument("--code_dir", help = "code directory", default = NULL, type = "character")
parser$add_argument("--in_dir", help = "directory for external inputs", default = NULL, type = "character")
parser$add_argument("--ds", help = "specify decomp step", default = 'step1', type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)

# Source GBD 2019 Shared Functions
source(paste0(code_dir, "helper_functions/source_functions.R"))
sourceDir("filepath")

# directory for output on the J drive
out_dir <- paste0(root_j_dir, "/", step_num, "_", step_name)
# directory for output on clustertmp
tmp_dir <- paste0(root_tmp_dir, "/",step_num, "_", step_name)

# User specific options --------------------------------------------------------
functional <- "meningitis"

# Check files before submitting job --------------------------------------------
# If rerunning, this deletes the finished.txt file for this job
if(file.exists(file.path(out_dir, "finished.txt"))) {
  file.remove(file.path(out_dir, "finished.txt"))
}
# Check for finished.txt from steps that this script was supposed to wait to be finished before running
if (!is.null(hold_steps)) {
  for (i in hold_steps) {
    sub.dir <- list.dirs(path=root_j_dir, recursive=F)
    files <- list.files(path=file.path(root_j_dir, grep(i, sub.dir, value=T)), pattern = 'finished.txt')
    if (is.null(files)) {
      stop(paste(dir, "Error"))
    }
  }
}

# Inputs -----------------------------------------------------------------------
step_in_dir <- file.path(in_dir, paste0(step_num,"_",step_name))
health_dist <- read.xlsx(file.path(step_in_dir, "Edmonds_health_dist_clean.xlsx"), sheet="data")
setDT(health_dist)
# Open file and format (used to be Ersatz output, now the result of a regression: GDP and proportion of major outcomes))
prop_major <- fread(file.path(step_in_dir, "prop_major_all_locs_draws_gbd2019.csv"))

# Run job ----------------------------------------------------------------------
# Calculating proportions of >1 major outcome for each etiology
rows.remove <- grep("Minor|IQR", health_dist$sequela)
health_dist <- health_dist[!c(rows.remove)]

# create normalized proportions: risk of outcome / all outcomes (not including clinical or minor impairments)
major_temp <- health_dist[sequela == "At least one major sequela", meningitis_all_mean]
clinical <- health_dist[sequela == "Major clinical impairments", meningitis_all_mean]
major <- major_temp - clinical
health_dist[!sequela %in% c("At least one major sequela", "At least one minor sequela"), 
            meningitis_all_mean := meningitis_all_mean / major]

# Rename outcomes
rows.remove <- grep("Major clinical impairments", health_dist$sequela)
health_dist <- health_dist[!c(rows.remove)]
major_cog_seq <- c("Major cognitive difficulties", "Major motor deficit", "Major multiple impairments")
health_dist[sequela %in% major_cog_seq,              outcome := "major_mort1"]
health_dist[sequela == "At least one minor sequela", outcome := "minor"]
health_dist[sequela == "Major visual disturbance",   outcome := "major_mort0"]
health_dist[sequela == "Major hearing loss",         outcome := "major_mort_"]
health_dist[sequela == "Major seizure disorder",     outcome := "seizure"]
health_dist[sequela == "At least one major sequela", outcome := "major"]
health_dist[, sequela := NULL]

# Proportions for major_mort0, major_mort1, major_mort_, and seizure
se.sd.median.cols <- grep(".se|.sd|.median", colnames(health_dist))
# Drops multiple major impairments row and minor impairments row
# Also drops *se, *sd, and *median columns
major_prop <- health_dist[2:7, !se.sd.median.cols, with = FALSE]
major_prop <- major_prop[, lapply(.SD, sum), by=outcome, .SDcols = c("meningitis_all_mean")] # Summed across outcomes
major_prop <- major_prop[order(outcome)]
fwrite(major_prop, paste0(tmp_dir, "/03_outputs/02_summary/major_prop.csv"))

# Creating draws for major_all and minor_all
major_minor_draws <- function(sev) { 
  mm <- health_dist[health_dist$outcome == sev, meningitis_all_mean]
  ss <- health_dist[health_dist$outcome == sev, meningitis_all_se] 
  # Setting beta distribution shape parameters (method of moments, for a non-traditional proportional distribution)
  # Constrained to give a proportion, can be over or under dispersed relative to other distributions defined by alpha and beta
  alpha <- (mm * (mm - mm^2 - ss^2)) / ss^2
  beta <- (alpha * (1 - mm)) / mm
  beta_draw <-  rbeta(n = 1000, shape1 = alpha, shape2 = beta)
  
  severity_tmp <- data.table(col = paste0("v_", 0:999),
                             draws = beta_draw)
  # Transpose data table
  severity_tmp <- dcast(melt(severity_tmp, id.vars = "col"), variable ~ col)
  severity_tmp[, variable := NULL]
  return(severity_tmp)
}

# Calculate ratio draws of minor against major 
minor.dt <- major_minor_draws('minor')
colnames(minor.dt) <- gsub("v", "w", colnames(minor.dt))
major.dt <- major_minor_draws('major')
minor.ratio.dt <- cbind(major.dt, minor.dt)
# Replace minor with 1 if greater than major
# Else replace minor with minor/major if less than major, normalizing proportion of minor to major
minor.ratio.dt[, paste0("w_",0:999) := lapply(0:999, function(i) { 
  ifelse( get(paste0("w_",i)) / get(paste0("v_",i)) > 1, 1, get(paste0("w_",i)) / get(paste0("v_",i)))
})]
minor.ratio.dt[, paste0("v_", 0:999) := NULL]

# Make proportion draws for each etiology/outcome/location/year
# Loop over all outcomes except major
# This makes etiology and outcome specific files that have proportional draws for every location/year
outcomes <- unique(health_dist[outcome != "major", outcome])
for (out in outcomes) {
  prop_tmp <- melt(prop_major, id.vars = c("year_id", "location_id"))
  if (out == "minor") {
    # sets first (and only) row of minor.ratio.dt as a vector
    minor_ratio_vector <- as.numeric(minor.ratio.dt[1])
    prop_tmp[, minor_ratio := minor_ratio_vector]
    prop_tmp[, value := value * minor_ratio]
  } else {
    major_prop_vector <- major_prop[outcome == out, meningitis_all_mean]
    prop_tmp[, value := value * major_prop_vector]
  }
  prop_tmp <- dcast(prop_tmp, location_id + year_id ~ variable, value.var = "value")
  prop_tmp[, measure_id := 6] # measure id for incidence
  if (out == "minor") {
    group <- "long_mild"
  } else if (out == "major_mort1") {
    group <- "long_modsev"
  } else if (out == "seizure") {
    group <- "epilepsy"
  } else if (out =="major_mort_") {
    group <- "_hearing"
  } else if (out == "major_mort0") {
    group <- "_vision"
  }
  prop_tmp[, grouping := group]
  saveRDS(prop_tmp, file.path(tmp_dir, "03_outputs", "01_draws", paste0("risk_", group, ".rds")))
}

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
# Write check file to indicate step has finished
file.create(file.path(out_dir,"finished.txt"), overwrite=T)