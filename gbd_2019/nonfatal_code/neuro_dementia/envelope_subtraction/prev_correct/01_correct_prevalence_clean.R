##########################################################################
### Author: USERNAME
### Date: 07/01/2019
### Project: GBD Nonfatal Estimation
### Purpose: Adjust Dementia Prevalence - Child Script
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

pacman::p_load(data.table, openxlsx, ggplot2, readr)
date <- gsub("-", "_", Sys.Date())
print(Sys.info()['nodename'])

# SET OBJECTS -------------------------------------------------------------

functions_dir <- paste0("FILEPATH")
draws <- paste0("draw_", 0:999)
envelope_dir <- paste0("FILEPATH")
mrbrt_helper_dir <- paste0("FILEPATH")
ds_dir <- paste0("FILEPATH")
pd_dir <- paste0("FILEPATH")
stroke_dir <- paste0("FILEPATH")
tbi_dir <- paste0("FILEPATH")
date_dt <- data.table(cause = c("ds", "stroke", "tbi", "pd"),
                      date = c("2019_07_02", "2019_06_26", "2019_07_03", "2019_07_24"),
                      gbd_id = c(ID, ID, ID, ID),
                      me_id = c(ID, ID, ID, ID))
dem_meid <- ID

## GET ARGS AND ITEMS
args<-commandArgs(trailingOnly = TRUE)
map_path <-args[1]
save_dir <-args[2]
params <- fread(map_path)
task_id <- Sys.getenv("SGE_TASK_ID")
location <- params[task_num == task_id, location]

# SOURCE FUNCTIONS --------------------------------------------------------

functs <- c("get_draws", "get_ids", "get_outputs", "get_age_metadata", "get_model_results", 
            "get_demographics_template", "get_population")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))
source(paste0(mrbrt_helper_dir, "predict_mr_brt_function.R"))

summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

floor_zero <- function(x){
  x[x < 0] <- 0
  return(x)
}

# GET AGES ----------------------------------------------------------------

age_dt <- get_age_metadata(12)
age_dt <- age_dt[age_group_id >= 13]
age_dt[age_group_years_end == 125, age_group_years_end := 100]
age_dt[, age := (age_group_years_start + age_group_years_end)/2]
age_dt[, age_group_years_end := age_group_years_end - 1]

# PULL IN PREDICTION DRAWS ------------------------------------------------

get_model_preds <- function(c){
  dir <- get(paste0(c, "_dir"))
  files <- list.files(dir)
  file <- files[grepl(paste0(date_dt[cause == c, date], "$"), files)]
  dt <- fread(paste0(dir, file, "/model_draws.csv"))
  dt <- unique(dt) 
  setnames(dt, names(dt)[grepl("age", names(dt))], "age")
  dt <- merge(dt, age_dt[, .(age, age_group_id, age_group_years_start, age_group_years_end)], by = "age")
  dt[, `:=` (condition = c, gbd_id = date_dt[cause == c, gbd_id])]
  if (c == "hiv"){
    dt[, (draws) := lapply(.SD, plogis), .SDcols = draws]
    setnames(dt, "X_cd4_mean", "cd4")
    keep_cols <- c("age_group_id", "age_group_years_start", "age_group_years_end", "age", "condition", "gbd_id", "cd4")
  } else {
    dt[, (draws) := lapply(.SD, floor_zero), .SDcols = draws]
    dt[, (draws) := lapply(.SD, exp), .SDcols = draws]
    keep_cols <- c("age_group_id", "age_group_years_start", "age_group_years_end", "age", "condition", "gbd_id")
  } 
  dt <- dt[, c(keep_cols, draws), with = F]
  return(dt)
}


# GET EXPOSURE PREVALENCE -------------------------------------------------
get_normal_prev <- function(id){
  prev <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, source = "epi", measure_id = 5,
                    location_id = location, sex_id = c(1, 2), status = "best", metric_id = 3, 
                    age_group_id = age_dt[, unique(age_group_id)], decomp_step = "step1")
  prev <- prev[, c("age_group_id", "sex_id", "year_id", draws), with = F]
  setnames(prev, draws, paste0("prev_", 0:999))
  return(prev)
}

get_tbi_prev <- function(){
  save_folder <- "FILEPATH"
  prev <- readr::read_rds(paste0(save_folder, location, ".rds"))
  prev2017 <- copy(prev[year_id == 2017])
  prev2015 <- copy(prev2017[, year_id := 2015])
  prev2019 <- copy(prev2017[, year_id := 2019])
  prev <- rbindlist(list(prev, prev2015, prev2017), use.names = T)
  prev <- prev[, c("age_group_id", "sex_id", "year_id", draws), with = F]
  setnames(prev, draws, paste0("prev_", 0:999))
  return(prev)
}

get_stroke_prev <- function(){
  prev <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = c(ID, ID, ID), source = "epi", measure_id = 5,
                    location_id = location, sex_id = c(1, 2), status = "best", metric_id = 3, 
                    age_group_id = age_dt[, unique(age_group_id)], decomp_step = "step1")
  prev <- prev[, c("age_group_id", "sex_id", "year_id", draws), with = F]
  prev[, (draws) := lapply(.SD, sum), by = c("age_group_id", "sex_id", "year_id"), .SDcols = draws]
  prev <- unique(prev, by = c("age_group_id", "sex_id", "year_id"))
  setnames(prev, draws, paste0("prev_", 0:999))
  return(prev)
}

ds_prev <- get_normal_prev(id = date_dt[cause == "ds", me_id])
ds_prev[age_group_id >= 19, paste0("prev_", 0:999) := 0]
pd_prev <- get_normal_prev(id = date_dt[cause == "pd", me_id])
stroke_prev <- get_stroke_prev()
tbi_prev <- get_tbi_prev()
prev <- list(ds = ds_prev, stroke = stroke_prev, tbi = tbi_prev, pd = pd_prev)

# CALCULATE PAFS ----------------------------------------------------------

paf_calc <- function(num){
  print(num)
  prev_dt <- prev[[num]]
  rr_dt <- model_draws[[num]]
  paf <- merge(prev_dt, rr_dt, by = "age_group_id")
  paf[, paste0("draw_", 0:999) := lapply(0:999, function(x)
    (get(paste0("prev_", x))*(get(paste0("draw_", x))-1))/(get(paste0("prev_", x))*(get(paste0("draw_", x))-1)+1))]
  paf[, c(paste0("prev_", 0:999)) := NULL]
  paf[, location_id := location]
  return(paf)
}

pafs <- lapply(1:4, paf_calc) 
names(pafs) <- names(model_draws)

# CALCULATE PREVALENCE ATTRIBUTABLE ---------------------------------------

dem_prev <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = dem_meid, source = "epi", measure_id = 5:6,
                      location_id = location, sex_id = c(1, 2), status = "best", metric_id = 3,
                      age_group_id = age_dt[, unique(age_group_id)], decomp_step = "step4")
setnames(dem_prev, draws, paste0("prev_", 0:999))

calc_attr <- function(paf_dt, prev_dt = dem_prev){
  dt <- merge(paf_dt, prev_dt, by = c("age_group_id", "sex_id", "year_id", "location_id"))
  dt[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("prev_", x)))]
  dt <- dt[, c("age_group_id", "sex_id", "year_id", "location_id", "measure_id", draws), with = F]
  return(dt)
}

results <- sapply(pafs, calc_attr, simplify = F)

summary_results <- sapply(results, function(x) summaries(dt = x, draw_vars = draws), simplify = F)

# CORRECT DEMENTIA PREVALENCE ---------------------------------------------

correct_dem_prev <- function(result_list, dem_dt){
  by_cols <- c("age_group_id", "sex_id", "year_id", "location_id", "measure_id")
  for (n in 1:length(results)){
    assign(names(results)[n], results[[n]])
    setnames(get(names(results[n])), draws, paste0(names(results)[n], "_", 0:999))
  }
  dt_list <- lapply(1:length(results), function(x) get(names(results)[x]))
  dt_list <- c(list(dem_dt), dt_list)
  
  full_dt <- Reduce(function(dt, dt1) merge(dt, dt1, by = by_cols), dt_list)
  
  full_dt[, (draws) := lapply(0:999, function(x) get(paste0("prev_", x)) - get(paste0("stroke_", x))
                              - get(paste0("ds_", x)) - get(paste0("tbi_", x)) - get(paste0("pd_", x)))]
  full_dt <- full_dt[, c(by_cols, draws), with = F]
  full_dt[, metric_id := 3]
  return(full_dt)
}

corrected <- correct_dem_prev(results, dem_prev)

dementia <- summaries(corrected, draw_vars = draws)
summary_results[['dementia']] <- dementia

full_results <- copy(results)
full_results[['dementia']] <- corrected

# SAVE RESULTS ------------------------------------------------------------

write.csv(corrected, paste0("FILEPATH"), row.names = F)
readr::write_rds(summary_results, paste0("FILEPATH"))
readr::write_rds(full_results, paste0("FILEPATH"))
readr::write_rds(pafs, paste0("FILEPATH"))
readr::write_rds(dem_prev, paste0("FILEPATH"))