##########################################################################
### Project: GBD Nonfatal Estimation
### Purpose: Child Script for Calculating Code Differential
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
date <- gsub("-", "_", Sys.Date())

# GET ARGUMENTS -----------------------------------------------------------

args<-commandArgs(trailingOnly = TRUE)
map_path <-args[1]
save_dir <-args[2]
params <- fread(map_path)
task_id <- Sys.getenv("SGE_TASK_ID")
code <- params[task_num == task_id, code]

# SET UP OBJECTS ----------------------------------------------------------

italy_folder <- paste0("FILEPATH")
graphs_dir <- paste0("FILEPATH")
results_dir <- paste0("FILEPATH")
repo_dir <- paste0("FILEPATH")
functions_dir <- paste0("FILEPATH")
code_cols <- paste0("diag", 1:6)
months <- c(1, 3, 6, 12)

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_ids.R"))
source(paste0(functions_dir, "get_age_metadata.R"))

get_age_groups <- function(change_dt){
  dt <- copy(change_dt)
  start_years <- age_dt[, age_group_years_start]
  dt[, age_group_years_start := start_years[findInterval(age, vec = start_years)]]
  dt <- merge(dt, age_dt[, .(age_group_years_start, age_group_name)], by = "age_group_years_start", sort = F)
  dt[, age_group_years_start := NULL]
  return(dt)
}

calc_comp <- function(cond_dt, comp_col, num_months){
  num_days <- 30.44 * num_months
  dt <- copy(cond_dt)
  dt <- merge(dt, mort_dates, by = "sid", sort = F)
  dt[, date_diff := difftime(date_death, end_date, units = "days")]
  dt[, in_range := (date_diff <= num_days)]
  dt[in_range == T, paste0(comp_col, "_range") := get(comp_col)][in_range == F, paste0(comp_col, "_range") := 0]
  dt[, paste0(comp_col, "_range") := sum(get(paste0(comp_col, "_range")), na.rm = T), by = "sid"]
  dt <- unique(dt, by = "sid")
  dt <- dt[, c("sid", paste0(comp_col, "_range")), with = F]
  return(dt)
}

prop_yes_noage <- function(range_dt, dem_dt, comp_col, num_months){
  dt <- copy(range_dt)
  dt <- merge(dt, dem_dt, by = "sid")
  dt <- merge(dt, mort_dates, by = "sid")
  dt[, mean := mean(get(paste0(comp_col, "_range")) > 0), by = c("dem")]
  dt <- unique(dt, by = c("dem"))
  dt <- dt[, .(dem, mean, cond = comp_col, months = num_months)]
  return(dt)
}

get_mort_date <- function(mort_dt){
  dt <- copy(mort_dt)
  dt <- dt[, .(sid, date_death, age)]
  return(dt)
}

# GET AGE DATA ------------------------------------------------------------

age_metadata <- get_age_metadata(12)
age_names <- get_ids(table = "age_group")
full_age_dt <- merge(age_metadata, age_names, by = "age_group_id")
full_age_dt <- full_age_dt[age_group_id >= 13]
full_age_dt[age_group_id == 235, age_group_years_end := 100]
full_age_dt[, risk_age := (age_group_years_start + age_group_years_end) / 2]
age_dt <- copy(full_age_dt[age_group_years_start >= 65])
age_dt[age_group_id == 18, `:=` (age_group_years_start = 40, age_group_name = "40 to 69")]

# DATA PREP CODE ----------------------------------------------------------

source(paste0(repo_dir, "italy_linkage_prep.R"))

# CALCULATE CODE FREQUENCY ------------------------------------------------

code_dt <- copy(hosp)
code_dt[, code_freq := apply(.SD, 1, function(x) sum(grepl(paste0("^", as.character(code), "$"), x))), .SDcols = code_cols]
mort_dates <- get_mort_date(mort)
code_list <- lapply(1:4, function(x) calc_comp(cond_dt = code_dt, comp_col = "code_freq", num_months = months[x]))
means_dt <- rbindlist(lapply(1:4, function(x) prop_yes_noage(range_dt = code_list[[x]], dem_dt = dem_status, comp_col = "code_freq", num_months = months[x])))
means_dt[, icdcode := code]
readr::write_rds(means_dt, paste0("FILEPATH"))
