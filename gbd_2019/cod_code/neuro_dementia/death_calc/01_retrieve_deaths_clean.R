##########################################################################
### Project: GBD Nonfatal Estimation
### Purpose: Get Dementia Deaths - Child Script
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
  functions_dir <- "FIILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2, readr, rhdf5)
library(mortcore, lib = "FILEPATH")
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------

mrbrt_dir <- paste0("FILEPATH")
prop_dir <- paste0("FILEPATH")
repo_dir <- paste0("FILEPATH")
functions_dir <- paste0("FILEPATH")
draws <- paste0("draw_", 0:999)
meid <- ID ## THIS IS WHERE YOU SET MEID TO BE EITHER ADJUSTED OR UNADJUSTED PREVALENCE
rr_mrbrt_model <- "pipeline/rr_2019_04_24"
ar_mrbrt_model <- "pipeline/ar_2019_04_24"
prop_file_noage <- "anycond_means_allcodesadded_2019_02_24"

## GET ARGS AND ITEMS
args<-commandArgs(trailingOnly = TRUE)
map_path <-args[1]
save_dir <-args[2]
params <- fread(map_path)
task_id <- Sys.getenv("SGE_TASK_ID")
location <- params[task_num == task_id, location]

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_draws.R"))
source(paste0(functions_dir, "get_ids.R"))
source(paste0(functions_dir, "get_age_metadata.R"))
source(paste0(functions_dir, "get_population.R"))
source(paste0(functions_dir, "get_envelope.R"))

get_age_groups <- function(change_dt, age_dt, age_col, get_col){
  dt <- copy(change_dt)
  start_years <- age_dt[, age_group_years_start]
  dt[, age_group_years_start := start_years[findInterval(get(age_col), vec = start_years)]]
  dt <- merge(dt, age_dt[, c("age_group_years_start", get_col), with = F], by = "age_group_years_start", sort = F)
  dt[, age_group_years_start := NULL]
  return(dt)
}

summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

cap_one <- function(x){
  x[x > 1] <- 1
  return(x)
}

cap_toone <- function(x){
  x[x < 1] <- 1
  return(x)
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

# GET MR-BRT RESULTS ------------------------------------------------------

get_mrbrt_data <- function(folder, sex = T){
  dt <- fread(paste0("FILEPATH"))
  mrbrtagedt <- copy(full_age_dt[age_group_years_start >=40])
  mrbrtagedt[, risk_age := (age_group_years_start + age_group_years_end)/2]
  dt <- merge(dt, mrbrtagedt[, .(risk_age, age_group_id)], by.x = "X_risk_age", by.y = "risk_age")
  if (sex == T){
    dt[X_sex_cov == -0.5, sex_id := 1][X_sex_cov == 0.5, sex_id := 2]
  }
  dt[, (draws) := lapply(.SD, exp), .SDcols = draws]
  return(dt)
}

ar_dt <- get_mrbrt_data(ar_mrbrt_model, sex = F)
rr_dt <- get_mrbrt_data(rr_mrbrt_model, sex = F)

# CALCULATE ADJUSTMENT ----------------------------------------------------

calc_prop_adjust <- function(dt){
  adjust_dt <- copy(dt)
  adjust_dt[, (draws) := lapply(.SD, cap_toone), .SDcols = draws]
  adjust_dt[, (draws) := lapply(0:999, function(x) (get(paste0("draw_", x))-1) / get(paste0("draw_", x)))]
  adjust_dt <- adjust_dt[, c("age_group_id", draws), with = F]
  return(adjust_dt)
}

adjust_dt <- calc_prop_adjust(rr_dt)

# CALCULATE EXCESS DEATHS -------------------------------------------------

excess_death <- function(id, dt){
  prev <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, source = "epi", measure_id = 5,
                    location_id = location, sex_id = c(1, 2), status = "best", metric_id = 3,
                    gbd_round_id = 6, age_group_id = full_age_dt[, age_group_id], decomp_step = "step1")
  setnames(prev, draws, paste0("prev_", 0:999))
  excessdeath <- merge(prev, dt, by = c("age_group_id"))
  excessdeath[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("prev_", x)))]
  excessdeath <- excessdeath[, c("age_group_id","year_id", "sex_id", draws), with = F]
  setnames(excessdeath, draws, paste0("death_", 0:999))
  return(excessdeath)
}

edeath_rate_dt <- excess_death(meid, ar_dt)

get_counts <- function(dt, draw_cols){
  pops <- get_population(age_group_id = full_age_dt[, unique(age_group_id)], location_id = location,
                         year_id = c(seq(1990, 2015, by = 5), 2017, 2019), sex_id = c(1, 2),
                         decomp_step = 'iterative')
  count_dt <- merge(dt, pops, by = c("year_id", "sex_id", "age_group_id"))
  count_dt[, (draw_cols) := lapply(.SD, function(x) x * population), .SDcols = draw_cols]
  count_dt <- count_dt[, c("age_group_id", "year_id", "sex_id", draw_cols), with = F]
  return(count_dt)
}

edeath_count_dt <- get_counts(edeath_rate_dt, paste0("death_", 0:999))

# GET PROPORTIONS AND ADJUST ----------------------------------------------

scaled_props_noage <- function(pafs){
  dt <- copy(pafs)
  prop_dt <- fread(paste0(prop_dir, prop_file_noage, ".csv"))
  prop_dt <- dcast(prop_dt, months ~ dem, value.var = "mean")
  setnames(prop_dt, c("0", "1"), c("no_dem", "dem"))
  prop_dt[, mean := dem - no_dem]
  prop_dt[, c("no_dem", "dem") := NULL]
  prop_dt <- prop_dt[months == 12]

  dt[, mean := prop_dt[, mean]]
  dt[, (draws) := lapply(0:999, function(x) mean / get(paste0("draw_", x)))]
  dt[, (draws) := lapply(.SD, cap_one), .SDcols = draws]

  graph_dt <- summaries(dt, draws)
  return(list(dt, graph_dt))
}

aprop_results <- scaled_props_noage(adjust_dt)
aprop_dt <- aprop_results[[1]]
aprop_summaries <- aprop_results[[2]]

# APPLY PROPORTIONS -------------------------------------------------------

apply_props <- function(props, deaths){
  dt <- merge(props, deaths, by = "age_group_id")
  dt[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("death_", x)))]
  dt[, location_id := location]
  dt <- dt[, c("location_id", "year_id","age_group_id", "sex_id", draws), with = F]
  return(dt)
}

death_rate <- apply_props(aprop_dt, edeath_rate_dt)
death_count <- apply_props(aprop_dt, edeath_count_dt)

# SAVE FILES --------------------------------------------------------------

write_rds("FILEPATH"))
write.csv("FILEPATH"), row.names = F)
write.csv("FILEPATH"), row.names = F)
