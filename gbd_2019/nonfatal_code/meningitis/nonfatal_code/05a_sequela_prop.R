#####################################################################################################################################################################################
## Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
## Author:	 	
## Last updated:	3/1/2019
## Description:	Generate 1000 draws of each sequela proportion for each etiology-outcome combination, and normalize to sum to 1
## Number of output files: 8
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
library(argparse, lib.loc = "filepath")
library(data.table)

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

# directory for output on the J drive
out_dir <- paste0(root_j_dir, "/", step_num, "_", step_name)
# directory for output on clustertmp
tmp_dir <- paste0(root_tmp_dir, "/",step_num, "_", step_name)

# User specified options -------------------------------------------------------
# grouping
groupings <- c("long_mild", "long_modsev")
# etiologies
etiologies <- c("meningitis_pneumo", "meningitis_hib", "meningitis_meningo", "meningitis_other")
# measure_id for prevalence (5), this code uses measure_id
measure <- 5

# Check files before submitting job --------------------------------------------
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
# Deletes step finished.txt
if(file.exists(file.path(out_dir,"finished.txt"))) {
  file.remove(file.path(out_dir,"finished.txt"))
}

# Inputs -----------------------------------------------------------------------
freq_dt <- fread(file.path(in_dir, paste0(step_num, "_", step_name), "frequency_distributions.csv"))
freq_dt[, groupings := paste0("meningitis_", output_code)]
freq_dt[, output_code := NULL]

# Run job ----------------------------------------------------------------------
# ------------------------------------------------------------------------------
#' @description creates 1000 draws of beta distribution for each sequela (healthstate) 
#' SE =  (upper - lower) / (2 * 1.96)
#' alpha =  (mean * (mean - mean^2  - SE^2)) / SE^2
#' beta =  (alpha * (1 - mean)) / mean
#' Beta(alpha, beta)
#' @param state sequela
#' @param mm mean
#' @param u upper bound
#' @param l lower bound 
# ------------------------------------------------------------------------------
beta_dist <- function (state, mm, u, l) {
  ss <- (u - l) / (2 * 1.96)
  alpha <- (mm * (mm - mm^2 - ss^2)) / ss^2
  beta <- (alpha * (1 - mm)) / mm
  n_draws <- 1000
  beta_draw <-  rbeta(n_draws, alpha, beta)
  # Replace NAs with 0 specifically for pneumo_long_modsev
  beta_draw[is.na(beta_draw)] <- 0
  DT <- data.table(col = paste0("v_", 0:999),
                   draws = beta_draw)
  DT <- dcast(melt(DT, id.vars = "col"), variable ~ col)
  DT[, variable:=NULL]
  DT$state <- state
  DT$code <- paste0(g)
  DT$measure_id <- measure
  setcolorder(DT, c("measure_id", "code", "state", paste0("v_", 0:999)))
  return(DT)
}

for (g in groupings) {
  # for each group build a list containing sequela proportions for each etiology
  etiology_list <- lapply(etiologies, function(e) {
    freq_tmp_dt <- freq_dt[groupings == paste0(e, "_", g)]
    # create beta distribution for each sequela (healthstate) 
    state_dt <- data.table()
    for (i in 0:5) {
      state <- freq_tmp_dt[, get(paste0("healthstate_", i))]
      mm <- freq_tmp_dt[, get(paste0("proportion_", i))]
      u <- freq_tmp_dt[, get(paste0("ub_", i))]
      l <- freq_tmp_dt[, get(paste0("lb_", i))]
      dt <- beta_dist(state, mm, u, l)
      state_dt <- rbind(state_dt, dt)
    }
    state_dt[, etiology := e]
    # squeeze proportions to sum to 1
    state_dt[, paste0("total_", 0:999) := lapply(.SD, sum), by=.(code, measure_id), .SDcols = paste0("v_", 0:999)]
    state_dt[, paste0("v_", 0:999) := lapply(0:999, function(x) {
      get(paste0("v_", x)) / get(paste0("total_", x))
    })]
    cols.remove <- paste0("total_", 0:999)
    state_dt[, (cols.remove):= NULL]
    return(state_dt)
  })
  etiology_dt <- rbindlist(etiology_list)
  # average the proportions across etiologies
  sequela_prop_dt <- etiology_dt[, lapply(.SD, mean), by = c("measure_id", "code", "state"), .SDcols = paste0("v_", 0:999)]
  saveRDS(sequela_prop_dt, file.path(tmp_dir, '03_outputs', '01_draws', paste0(g, '.rds')))
}


print(paste(step_num, "sequential runs completed"))

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
# Write check file to indicate step has finished
file.create(file.path(out_dir,"finished.txt"), overwrite=T)