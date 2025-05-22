rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH" 
  h <- "FILEPATH"
  l <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
  functions_dir <- "FILEPATH"
}


pacman::p_load(data.table, openxlsx, ggplot2, readr)
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------

functions_dir <- "FILEPATH"
draws <- paste0("draw_", 0:999)
envelope_dir <- paste0("FILEPATH")
mrbrt_helper_dir <- paste0("FILEPATH")
ds_dir <- paste0(envelope_dir, "FILEPATH")
pd_dir <- paste0(envelope_dir, "FILEPATH")
stroke_dir <- paste0(envelope_dir, "FILEPATH")
tbi_dir <- paste0(envelope_dir, "FILEPATH")
date_dt <- data.table(cause = c("ds", "stroke", "tbi", "pd"),
                      date = c("2019_07_02", "2019_06_26", "2019_07_03", "2019_07_24"),
                      gbd_id = c(ID, ID, ID, ID),
                      me_id = c(ID, NA, NA, ID))

dem_meid <- ID #(pre or post-mortality)
vers <- ID #Dismod envelope version to adjust (run in MEID above)
  
## GET ARGS AND ITEMS
args<-commandArgs(trailingOnly = TRUE)
map_path <-args[1]
params <- fread(map_path)
task_id <- Sys.getenv("SGE_TASK_ID")
location <- params[task_num == task_id, location]

save_dir <- paste0("FILEPATH")
exposure_dir <- "FILEPATH"

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

age_dt <- get_age_metadata(ID, gbd_round_id=ID)
age_dt <- age_dt[age_group_id >= 13]
age_dt[age_group_years_end == 125, age_group_years_end := 100]
age_dt[, age := age_group_years_start]
age_dt[, age_group_years_end := age_group_years_end - 1]

# PULL IN PREDICTION DRAWS (THIS IS RR) ------------------------------------------------

get_model_preds <- function(c){
  dir <- get(paste0(c, "_dir"))
  files <- list.files(dir)
  file <- files[grepl(paste0(date_dt[cause == c, date], "$"), files)]
  dt <- fread(paste0(dir, file, "/model_draws.csv"))
  dt <- unique(dt) 
  setnames(dt, names(dt)[grepl("age", names(dt))], "age")
  dt <- merge(dt, age_dt[, .(age, age_group_id, age_group_years_start, age_group_years_end)], by = "age")
  dt[, `:=` (condition = c, gbd_id = date_dt[cause == c, gbd_id])]
  dt[, (draws) := lapply(.SD, floor_zero), .SDcols = draws]
  dt[, (draws) := lapply(.SD, exp), .SDcols = draws]
  keep_cols <- c("age_group_id", "age_group_years_start", "age_group_years_end", "age", "condition", "gbd_id")
 
  dt <- dt[, c(keep_cols, draws), with = F]
  return(dt)
}

model_draws <- lapply(date_dt[!cause == "hiv", cause], get_model_preds)
names(model_draws) <- date_dt[!cause == "hiv", cause]

# GET EXPOSURE PREVALENCE -------------------------------------------------
draws <- paste0("draw_", 0:999)
ds_prev <- as.data.table(read_rds(paste0(exposure_dir, "FILEPATH", location, "FILEPATH")))
setnames(ds_prev, draws, paste0("prev_", 0:999))
ds_prev[age_group_id >= 19, paste0("prev_", 0:999) := 0]
pd_prev <- as.data.table(read_rds(paste0(exposure_dir, "FILEPATH", location, "FILEPATH")))
setnames(pd_prev, draws, paste0("prev_", 0:999))
stroke_prev <- as.data.table(read_rds(paste0(exposure_dir, "FILEPATH", location, "FILEPATH")))
setnames(stroke_prev, draws, paste0("prev_", 0:999))
tbi_prev <- as.data.table(read_rds(paste0(exposure_dir, "FILEPATH", location, "FILEPATH")))
setnames(tbi_prev, draws, paste0("prev_", 0:999))
prev <- list(ds = ds_prev, stroke = stroke_prev, tbi = tbi_prev, pd = pd_prev)

# CALCULATE PAFS ----------------------------------------------------------

paf_calc <- function(num){
  print(num)
  prev_dt <- prev[[num]]
  rr_dt <- model_draws[[num]]
  paf <- as.data.table(merge(prev_dt, rr_dt, by = "age_group_id"))
  paf[, paste0("draw_", 0:999) := lapply(0:999, function(x)
    (get(paste0("prev_", x))*(get(paste0("draw_", x))-1))/(get(paste0("prev_", x))*(get(paste0("draw_", x))-1)+1))]
  paf[, c(paste0("prev_", 0:999)) := NULL]
  return(paf)
}

pafs <- lapply(1:4, paf_calc) 
names(pafs) <- names(model_draws)


# CALCULATE PREVALENCE ATTRIBUTABLE ---------------------------------------

dem_prev <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = dem_meid, source = "epi", measure_id = ID,
                      location_id = location, sex_id = c(ID), version_id = vers, metric_id = ID,
                      age_group_id = age_dt[, unique(age_group_id)], decomp_step = "iterative", gbd_round_id=ID)
setnames(dem_prev, draws, paste0("prev_", 0:999))

calc_attr <- function(paf_dt, prev_dt = dem_prev){
  paf_dt[,"measure_id"] <- NULL
  dt <- merge(paf_dt, prev_dt, by = c("age_group_id", "sex_id", "year_id", "location_id"))
  dt[, (draws) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("prev_", x)))]
  dt <- dt[, c("age_group_id", "sex_id", "year_id", "location_id", "measure_id", draws), with = F]
  return(dt)
}

results <- sapply(pafs, calc_attr, simplify = F)

ds_tmp <- results$ds
ds_old_age <- expand.grid("age_group_id"=c(ID), "sex_id"=c(ID), "year_id"=c(seq(1990,2015, by=5), 2019, 2020, 2021, 2022), "location_id"=location, "measure_id"=c(ID))
ds_tmp <- as.data.table(plyr::rbind.fill(ds_tmp, ds_old_age))
ds_tmp[age_group_id > ID, paste0("draw_", 0:999) := 0]
results$ds <- NULL
results <- list(ds=ds_tmp, stroke=results$stroke, tbi=results$tbi, pd=results$pd)

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
  full_dt[, metric_id := ID]
  return(full_dt)
}

corrected <- correct_dem_prev(results, dem_prev)
corrected <- unique(corrected)

dementia <- summaries(corrected, draw_vars = draws)
summary_results[['dementia']] <- dementia

full_results <- copy(results)
full_results[['dementia']] <- corrected

# SAVE RESULTS ------------------------------------------------------------

write.csv(corrected, paste0(save_dir, "FILEPATH", location, "FILEPATH"), row.names = F)
readr::write_rds(summary_results, paste0(save_dir, "FILEPATH", location, "FILEPATH"))
readr::write_rds(full_results, paste0(save_dir, "FILEPATH", location, "FILEPATH"))
readr::write_rds(pafs, paste0(save_dir, "FILEPATH", location, "FILEPATH"))
readr::write_rds(dem_prev, paste0(save_dir, "FILEPATH", location, "FILEPATH"))
