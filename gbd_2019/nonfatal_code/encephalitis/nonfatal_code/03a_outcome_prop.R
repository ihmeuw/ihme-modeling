#####################################################################################################################################################################################
## Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
## Author:		
## Last updated:	11/20/2018
#' @NOTE: At the start of each round make sure to run GDP_sequelae_prop_regression.do to create prop_major_all_locs_draws_gbd2019.csv in the input directory
## Description:	Setup draws for outcome fraction for meningitis_other etiology (create the same files as meningitis for convenience, but only use meningitis_other)
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
library(argparse)
library(openxlsx)
library(data.table)
library(dplyr)

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
parser$add_argument("--new_cluster", help = "specify which cluster (0 - old cluster, 1 - new cluster)", default = 0, type = "integer")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)

# Source helper functions
source(paste0(code_dir, "helper_functions/source_functions.R"))

# Source GBD 2019 Shared Functions
k <- # filepath
sourceDir(paste0(k, "current/r/"))

# directory for output on the J drive
out_dir <- paste0(root_j_dir, "/", step_num, "_", step_name)
# directory for output on clustertmp
tmp_dir <- paste0(root_tmp_dir, "/",step_num, "_", step_name)

# User specific options --------------------------------------------------------
functional <- "encephalitis"
etiologies <- c("meningitis_pneumo", "meningitis_hib", "meningitis_meningo", "meningitis_other")

# pull locations from CSV created in model_custom
locations <- readRDS(file.path(in_dir,"locations_temp.rds"))

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id

meningitis_pneumo_meid <- 1298
meningitis_hib_meid <- 1328
meningitis_meningo_meid <- 1358
meningitis_other_meid <- 1388

# Check files before submitting job --------------------------------------------
# If rerunning, this deletes the finished.txt file for this job
if(file.exists(file.path(out_dir,"finished.txt"))) {
  file.remove(file.path(out_dir,"finished.txt"))
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
health_dist <- read.xlsx(file.path(in_dir, paste0(step_num,"_",step_name),"Edmonds_health_dist_clean.xlsx"), sheet="data")
setDT(health_dist)
  
# Open file and format (used to be Ersatz output, now the result of a regression: GDP and proportion of major outcomes))
prop_major <- fread(file.path(in_dir, paste0(step_num, "_", step_name), "prop_major_all_locs_draws_gbd2019.csv"))
# ------------------------------------------------------------------------------

# Run job ----------------------------------------------------------------------
# Calculating proportions of >1 major outcome for each etiology  (from Theo's file)
rows.remove <- grep("Minor|IQR", health_dist$sequela)
health_dist <- health_dist[!c(rows.remove)]

# Generate meningitis_other variables
health_dist$meningitis_other_median = (health_dist$meningitis_hib_median + health_dist$meningitis_meningo_median) / 2
health_dist$meningitis_other_mean = (health_dist$meningitis_hib_mean + health_dist$meningitis_meningo_mean) / 2
health_dist$meningitis_other_se = (health_dist$meningitis_hib_se + health_dist$meningitis_meningo_se) / 2 
health_dist$meningitis_other_sd = (health_dist$meningitis_hib_sd + health_dist$meningitis_meningo_sd) / 2

# create normalized proportions: risk of outcome / all outcomes (not including clinical or minor impairments)
for (e in etiologies) {
  major_temp <- health_dist[sequela == "At least one major sequela", get(paste0(e, "_mean"))]
  clinical <-  health_dist[sequela == "Major clinical impairments", get(paste0(e, "_mean"))]
  major <- major_temp - clinical
  means  <- health_dist[sequela != "At least one major sequela" & sequela != "At least one minor sequela", get(paste0(e,"_mean"))]
  health_dist[sequela != "At least one major sequela" & sequela != "At least one minor sequela", c(paste0(e,"_mean"))] <- means / major
}

# Rename outcomes
rows.remove <- grep("Major clinical impairments", health_dist$sequela)
health_dist <- health_dist[!c(rows.remove)]
health_dist$sequela <- gsub("At least one minor sequela", "minor", health_dist$sequela)
health_dist$sequela <- gsub("Major visual disturbance", "major_mort0", health_dist$sequela)
health_dist$sequela <- gsub("Major hearing loss", "major_mort_", health_dist$sequela)
health_dist$sequela <- gsub("Major cognitive difficulties|Major motor deficit|Major multiple impairments", "major_mort1", health_dist$sequela)
health_dist$sequela <- gsub("Major seizure disorder", "seizure", health_dist$sequela)
health_dist$sequela <- gsub("At least one major sequela", "major", health_dist$sequela)
setnames(health_dist, "sequela", "outcome")

# Proportions for major_mort0, major_mort1, major_mort_, and seizure
# major_mort0 = major visual disturbance, major_mort1 = major cognitive difficulties, major motor deficit, or major multiple impairments, seizure is self explanatory
se.sd.median.cols <- grep(".se|.sd|.median", colnames(health_dist))
# Drops multiple major impairments row and minor impairments row
# Also drops *se, *sd, and *median columns
major_prop <- health_dist[2:7, !se.sd.median.cols, with=FALSE]
major_prop <- major_prop[, lapply(.SD, sum), by=outcome, .SDcols=c("meningitis_hib_mean", "meningitis_pneumo_mean", "meningitis_meningo_mean", "meningitis_other_mean")] # Summed across outcomes
major_prop <- major_prop[order(major_prop$outcome)]

# Creating draws for major_all and minor_all
severity <-  c("minor", "major")
for (e in etiologies) {
  for (sev in severity) { 
    mm <- health_dist[health_dist$outcome == sev, get(paste0(e,"_mean"))]
    ss <- health_dist[health_dist$outcome == sev, get(paste0(e,"_se"))] 
    # Setting beta distribution shape parameters (method of moments, for a non-traditional proportional distribution)
    # Constrained to give a proportion, can be over or under dispersed relative to other distributions defined by alpha1 and alpha2
    alpha1 <- (mm * (mm - mm^2 - ss^2)) / ss^2
    alpha2 <- (alpha1*(1 - mm)) / mm
    n_draws <- 1000
    beta_draw <-  rbeta(n_draws, alpha1, alpha2)
    
    severity_tmp <- data.table(col=paste0("v_", 0:999),
                               draws=beta_draw)
    # Transpose data table
    severity_tmp <- dcast(melt(severity_tmp, id.vars = "col"), variable ~ col)
    severity_tmp[, variable:=NULL]
    severity_tmp$modelable_entity_id <- get(paste0(e, "_meid"))
    saveRDS(severity_tmp, file=file.path(tmp_dir, "02_temp", "03_data", paste0(e,"_", sev,".rds")))
  }
}

# Calculate ratio draws of minor against major 
for (e in etiologies) {
  minor.dt <- readRDS(file=file.path(tmp_dir, "02_temp", "03_data", paste0(e, "_minor.rds")))
  colnames(minor.dt) <- gsub("v", "w", colnames(minor.dt))
  major.dt <- readRDS(file=file.path(tmp_dir, "02_temp", "03_data", paste0(e, "_major.rds")))
  
  minor.ratio.dt <- merge(major.dt, minor.dt, by="modelable_entity_id")
  setDT(minor.ratio.dt)
  # Replace minor with 1 if greater than major
  # Else replace minor with minor/major if less than major, normalizing proportion of minor to major?
  minor.ratio.dt[, paste0("w_",0:999) := lapply(0:999, function(i){ifelse(get(paste0("w_",i)) / get(paste0("v_",i))>1, 1, get(paste0("w_",i)) / get(paste0("v_",i)))})]
  minor.ratio.dt[, paste0("v_", 0:999) := NULL]
  saveRDS(minor.ratio.dt, file=file.path(tmp_dir, "02_temp", "03_data", paste0(e,"_minor_ratio.rds"))) ## ratio of minor/major proportion (never is greater than 1)
}

# Calculate ratios between each etiology and all cause that will be applied to the country major envelope later
# Calculates the ratio of a specific etiology / all meningitis. The inverse is the likelihood of the etiology not happening (used later) 
for (e in etiologies) {
  tmp.dt <- health_dist[outcome == "major", ]
  if (tmp.dt$meningitis_all_mean < tmp.dt[,get(paste0(e,"_mean"))]) {
    assign(paste0("ratio_",e), (1 - tmp.dt[,get(paste0(e,"_mean"))]) / (1 - tmp.dt$meningitis_all_mean))
    assign(paste0("inverse_", e), 1)
  } else {
    assign(paste0("ratio_",e), (tmp.dt[,get(paste0(e,"_mean"))]) / (tmp.dt$meningitis_all_mean))
    assign(paste0("inverse_", e), 0)
  }
}

# Make proportion draws for each etiology/outcome/location/year
# Loop over all outcomes except major
# This makes etiology and outcome specific files that have proportional draws for every location/year
etiologies.used <- "meningitis_other"
outcomes <- unique(health_dist$outcome)
outcomes.used <- outcomes[!outcomes %in% c("major_mort_", "major")] # don't do _hearing
for (e in etiologies.used) {
  for (out in outcomes.used) {
    print(out)
    prop_major_temp <- melt(prop_major, id.vars = c("year_id", "location_id"))
    
    if (get(paste0("inverse_",e)) == 0) {
      prop_major_temp$value <- prop_major_temp$value * get(paste0("ratio_", e))
    } else {
      # Calculates 1 - the likelihood of the event not happening
      prop_major_temp$value <- 1 - ((1 - prop_major_temp$value) * get(paste0("ratio_", e)))
    }
    
    if (out == "minor") {
      minor.ratio.dt <- readRDS(file=file.path(tmp_dir, "02_temp", "03_data", paste0(e, "_minor_ratio.rds")))
      setnames(minor.ratio.dt, paste0("w_", 0:999), paste0("draw_", 0:999))
      minor.ratio.dt <- melt(minor.ratio.dt, id.vars = c("modelable_entity_id"))
      setnames(minor.ratio.dt, c("value"), c("minor.ratio"))
      prop_major_temp <- merge(prop_major_temp, minor.ratio.dt, by = c("variable"))
      prop_major_temp$value <- prop_major_temp$value * prop_major_temp$minor.ratio
    } else {
      prop_major_temp$value <- prop_major_temp$value * major_prop[outcome == out, get(paste0(e, "_mean"))]
    } 
    
    if (out == "minor") {
      group <- "long_mild"
    } else if (out == "major_mort1") {
      group <- "long_modsev"
    } else if (out == "seizure") {
      group <- "epilepsy"
    } else if (out == "major_mort0") {
      group <- "_vision"
    } # no hearing here (no hearing loss due to encephalitis)
    
    prop_major_temp <- dcast(prop_major_temp, location_id + year_id ~ variable, value.var = "value")
    
    prop_major_temp$measure_id <- 6 # Measure id for incidence
    prop_major_temp$grouping <- group
    prop_major_temp$modelable_entity_id <- get(paste0(e, "_meid"))
    setnames(prop_major_temp, paste0("draw_", 0:999), paste0("v_", 0:999))
    
    saveRDS(prop_major_temp, file=file.path(tmp_dir, "03_outputs", "01_draws", paste0("risk_", e, "_", group,".rds")))
  }
}

print(paste(step_num, step_name, "sequential runs completed (but labeled as meningitis_other instead of encephalitis)"))

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
# Write check file to indicate step has finished
file.create(file.path(out_dir,"finished.txt"), overwrite=T)
# ------------------------------------------------------------------------------