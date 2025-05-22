#####################################################################################################################################################################################
## Description:	Generate 1000 draws of each sequela proportion for each etiology-outcome combination, and normalize to sum to 1. This file also reformats frequency_distributions file                                                                                                           ##
#####################################################################################################################################################################################
rm(list=ls())


# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
pacman::p_load(R.utils, data.table)

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# Set step-specific output directories
tmp_dir <- paste0("FILEPATH")
out_dir <- paste0("FILEPATH")

# ------------------------------------------------------------------------------
# Helper function
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

# User specified options -------------------------------------------------------
groupings <- c("long_mild", "long_modsev")
etiologies <- c("meningitis_pneumo", "meningitis_hib", "meningitis_meningo", "meningitis_other")
measure <- 5

# Inputs -----------------------------------------------------------------------
freq_dt <- fread(file.path("FILEPATH"))
freq_dt[, groupings := paste0("meningitis_", output_code)]
freq_dt[, output_code := NULL]

# Run job ----------------------------------------------------------------------

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
  saveRDS(sequela_prop_dt, file.path("FILEPATH"))
}

print(paste(step_num, "sequential runs completed"))
# ------------------------------------------------------------------------------


# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
# Write check file to indicate step has finished
file.create(paste0("FILEPATH"), overwrite=T)
# ------------------------------------------------------------------------------

