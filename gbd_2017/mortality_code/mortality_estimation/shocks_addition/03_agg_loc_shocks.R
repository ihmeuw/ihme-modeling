
###############################################################################################################
## Set up settings
  rm(list=ls())
  
  if (Sys.info()[1]=="Windows") {
    root <- "J:" 
    user <- Sys.getenv("USERNAME")
  } else {
    root <- "/home/j"
    user <- Sys.getenv("USER")
    task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))

    current_year <- as.integer(commandArgs(trailingOnly = T)[1])
    shocks_addition_run_id <- as.integer(commandArgs(trailingOnly = T)[2])
    shock_aggregator_run_id <- as.integer(commandArgs(trailingOnly = T)[3])
    gbd_year <- as.integer(commandArgs(trailingOnly = T)[4])
  }

## Source libraries and functions
  library(readr)
  library(data.table)
  library(assertable)
  library(haven)
  library(tidyr)
  library(parallel)
  library(rhdf5)
  library(RMySQL)
  library(slackr)
  
  library(mortdb)
  library(mortcore)
  library(ltcore)

  code_dir <- paste0(filepath, user, "/death_lt_finalizer")
  source(paste0(code_dir, "/rescale_nn_qx_mx.R"))
  source(paste0(code_dir, "/gen_summary_lt.R"))

## Set primary working directory
  master_dir <- paste0(filepath,shocks_addition_run_id)
  input_dir <- paste0(master_dir, "/inputs")
  lowest_dir <- paste0(master_dir, "/lowest_outputs")

## Look up parallelization metadata
  loc_map <- fread(paste0(input_dir,"/all_locations.csv"))
  lowest_map <- fread(paste0(input_dir, "/lowest_locations.csv"))
  run_countries <- unique(lowest_map$ihme_loc_id)

  agg_locs <- unique(loc_map[level <= 2, location_id])

## Set general ID variables
  id_vars <- c("location_id", "year_id", "sex_id", "age_group_id", "draw")

## Age file
  age_map <- fread(paste0(input_dir,"/age_map.csv"))
  age_map <- age_map[, list(age_group_id, age_group_years_start)]
  setnames(age_map, "age_group_years_start", "age")

## Population file
  pop_dt <- fread(paste0(input_dir, "/population.csv"))

  env_ages <- c(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)

  lower <- function(x) quantile(x, probs=.025, na.rm=T)
  upper <- function(x) quantile(x, probs=.975, na.rm=T)

  gen_summary_env <- function(env, id_vars) {
    summary_env <- env[, list(mean = mean(deaths, na.rm = T),
                              lower = quantile(deaths, probs=.025, na.rm=T), 
                              upper = quantile(deaths, probs=.975, na.rm=T)), 
                       by = id_vars]
    return(summary_env)
  }

###############################################################################################################
## Import shock-specific numbers for all aggregate locations
import_shock_numbers <- function(filename, year) {
  shock_numbers <- tryCatch({
    fread(filename)
  }, warning = function(w) {
    warning("fread failed to read the shock aggregator file - trying read_csv from readr")
    read_csv(filename)
  }, error = function(e) {
    warning("fread failed to read the shock aggregator file - trying read_csv from readr")
    read_csv(filename)
  })

  shock_numbers <- shock_numbers[cause_id == 294 & year_id == year,] ## in shocks file, cause_id 294 is all-cause shocks
  shock_numbers <- shock_numbers[, cause_id := NULL]

}

validate_shock_aggregator <- function(dataset) {
  # No data should be missing
  assert_values(dataset, colnames = names(dataset), test = "not_na", quiet = T)
  assert_values(dataset, colnames = names(dataset), test = "gte", test_val = 0, quiet = T)
  
  # assert_ids for ages, years, sexes, that are required
  sexes <- c(1, 2)
  
  id_vars <- list("age_group_id" = env_ages, "sex_id" = sexes, "year_id" = current_year, "location_id" = agg_locs)
  assert_ids(dataset, id_vars = id_vars)
}

aggregate_shock_deaths <- function(dataset, target_age_group_id, child_age_group_ids, id_vars) {
  # subset dataset to only contain the age group ids that are used to aggregate up to the target age group id
  child_ages <- dataset[age_group_id %in% child_age_group_ids]
  # select any age groups that are missing
  child_age_groups_missing <- child_age_group_ids[!(child_age_group_ids %in% unique(child_ages[, age_group_id]))]
  if (length(child_age_groups_missing) > 0) {
    stop(paste0("The following age groups id(s) required for aggregation up to age group id ", target_age_group_id," are missing: ", paste0(child_age_groups_missing, collapse = ", ")))
  }
  
  target_age <- child_ages[, lapply(.SD, sum), .SDcols = "shock_deaths", by = id_vars]
  target_age[, age_group_id := target_age_group_id]
  return(target_age)
}

shock_filenames <- paste0("FILEPATH", shock_aggregator_run_id , "/draws/shocks_", agg_locs, ".csv")
shock_numbers <- assertable::import_files(shock_filenames, FUN = import_shock_numbers, year = current_year, multicore = T, mc.cores = 4)

validate_shock_aggregator(shock_numbers)

shock_numbers <- melt(shock_numbers, value.name = "shock_deaths",
                      id.vars = c("location_id", "sex_id", "age_group_id", "year_id"),
                      measure.vars = grep("draw", names(shock_numbers), value = T),
                      variable.name = "draw",
                      variable.factor = F)

shock_numbers[, draw := as.integer(substr(draw, 6, nchar(draw)))]

shock_numbers_28 <- aggregate_shock_deaths(dataset = shock_numbers, target_age_group_id = 28, child_age_group_ids = c(2, 3, 4), id_vars = c("location_id", "year_id", "sex_id", "draw"))

gen_old_age_shocks <- function(target_age, dt) {
  dt <- dt[age_group_id == 235]
  dt[, age_group_id := target_age]
  return(dt)
}

shock_numbers <- rbindlist(list(shock_numbers, shock_numbers_28), use.names = T)
rm(shock_numbers_28)


###############################################################################################################
## Import no-shock envelope and LT for all aggregate locations
no_shock_env <- rbindlist(lapply(agg_locs, load_hdf, filepath = paste0(master_dir, "/env_no_shock/combined_env_aggregated_", current_year, ".h5")))
no_shock_lt <- rbindlist(lapply(agg_locs, load_hdf, filepath = paste0(master_dir, "/lt_no_shock/mx_ax_", current_year, ".h5")))
with_shock_ax <- fread(paste0(master_dir, "/lt_wshock/ax/ax_", current_year, ".csv"))
with_shock_ax <- with_shock_ax[location_id %in% agg_locs]

with_shock_env <- merge(no_shock_env[age_group_id %in% env_ages], shock_numbers, by = id_vars)
with_shock_env[, deaths := deaths + shock_deaths]

with_shock_env[, shock_deaths := NULL]

with_shock_env <- agg_results(with_shock_env, 
                              id_vars = id_vars, 
                              value_vars = c("deaths"),
                              agg_sex = T, agg_hierarchy = F, age_aggs = "gbd_compare")

summary_env <- gen_summary_env(with_shock_env, id_vars = id_vars[!id_vars %in% c("draw")])

rm(no_shock_env)
gc()


## Generate shock-specific death rates
shock_rates <- merge(shock_numbers, pop_dt, by = id_vars[id_vars != "draw"])
shock_rates[, shock_mx := shock_deaths / population]
shock_rates[, c("shock_deaths") := NULL]
shock_rates_95_plus <- rbindlist(lapply(c(33, 44, 45, 148), gen_old_age_shocks, dt = shock_rates))
shock_rates <- rbindlist(list(shock_rates, shock_rates_95_plus), use.names = T)

## Generate with-shock lifetable using mx from no-shock lifetable, shock-specific deaths, and with-shock ax
with_shock_lt <- merge(no_shock_lt, shock_rates, by = id_vars)

with_shock_lt[, c("qx", "ax") := NULL]
with_shock_lt <- merge(with_shock_lt, with_shock_ax, by = id_vars)

with_shock_lt[, deaths := (mx + shock_mx) * population]
with_shock_lt[, ax := ax * deaths]

with_shock_lt <- with_shock_lt[, .SD, .SDcols = c(id_vars, "deaths", "ax", "population")]
with_shock_lt <- agg_results(with_shock_lt, 
                  id_vars = id_vars, 
                  value_vars = c("deaths", "ax", "population"),
                  agg_sex = T, agg_hierarchy = F)

with_shock_lt[, ax := ax / deaths]
with_shock_lt[, mx := deaths / population]
with_shock_lt[, c("population", "deaths") := NULL]

## Split out NN mx for separate calculations
nn_mx_ax <- with_shock_lt[age_group_id %in% 2:4]
with_shock_lt <- with_shock_lt[!age_group_id %in% 2:4]

## Merge on age map here
with_shock_lt <- merge(with_shock_lt, age_map, by = "age_group_id")
gen_age_length(with_shock_lt)

with_shock_lt[, qx := mx_ax_to_qx(m = mx, a = ax, t = age_length)]
with_shock_lt[age == 110, qx := 1]

nn_mx_ax_qx <- rescale_nn_qx_mx(with_shock_lt[age_group_id == 28],
                                nn_mx_ax,
                                id_vars = id_vars[id_vars != "age_group_id"])

with_shock_lt[, c("age_length", "age") := NULL]
with_shock_lt <- rbindlist(list(with_shock_lt, nn_mx_ax_qx), use.names = T, fill = T)

summary_lt <- gen_summary_lt(with_shock_lt, pop_dt = pop_dt, id_vars = id_vars[!id_vars %in% c("draw")])


###############################################################################################################
## Save with-shock envelope and LT for all aggregate locations
save_env_lt_summary <- function(dt, save_year, output_type, shock_hiv_type, out_dir) {
  save_folder <- paste0(out_dir, "/", output_type, "_", shock_hiv_type)
  if(output_type == "lt") filename <- paste0("summary_lt_agg_", save_year, ".csv")
  if(output_type == "env") filename <- paste0("summary_env_agg_", save_year, ".csv")
  filepath <- paste0(save_folder, "/", filename)

  if(output_type == "lt") {
    dt <- copy(dt)
    dt[lt_parameter == "mx", life_table_parameter_id := 1]
    dt[lt_parameter == "ax", life_table_parameter_id := 2]
    dt[lt_parameter == "qx", life_table_parameter_id := 3]
    dt[lt_parameter == "ex", life_table_parameter_id := 5]
    dt[lt_parameter == "nLx", life_table_parameter_id := 7]
    dt[, lt_parameter := NULL]
  }

  print(paste0("Saving ", output_type, " ", shock_hiv_type, " to ", filepath))
  write_csv(dt, filepath)
}

save_env_lt_draws <- function(dt, save_year, output_type, shock_hiv_type, out_dir) {
  save_folder <- paste0(out_dir, "/", output_type, "_", shock_hiv_type)

  save_locs <- unique(dt$location_id)

  if(output_type == "lt") {
    filename <- paste0("mx_ax_", save_year, ".h5")
    save_cols <- c("mx", "ax", "qx")
  } else if(output_type == "env") {
    filename <- paste0("combined_env_aggregated_", save_year, ".h5")
    save_cols <- c("deaths")
  }
  
  filepath <- paste0(save_folder, "/", filename)

  print(paste0("Saving ", output_type, " ", shock_hiv_type, " to ", filepath))
  setkey(dt, location_id)

  existing_locs <- h5ls(filepath, datasetinfo = F)$name
  existing_locs <- existing_locs[existing_locs %in% save_locs]
  if(length(existing_locs) > 0) {
    for(loc in existing_locs) {
      rhdf5::h5delete(filepath, paste(loc))
    }
  }

  # rhdf5::H5closeAll()
  ## Here, appending to existing HDF files with the by_vals for all locations except for these
  invisible(lapply(save_locs, save_hdf, 
                   data = dt[, .SD, .SDcols = c("location_id", "sex_id", "year_id", "age_group_id", "draw", save_cols)], 
                   filepath = filepath, 
                   by_var="location_id", level=0))
  # rhdf5::H5closeAll()
}

print(paste0("LT Draw filesave ", Sys.time()))
save_env_lt_draws(with_shock_lt, save_year = current_year, output_type = "lt", shock_hiv_type = "wshock", out_dir = master_dir)
save_env_lt_summary(summary_lt[[1]], save_year = current_year, output_type = "lt", shock_hiv_type = "wshock", out_dir = master_dir)

print(paste0("Envelope draw filesave ", Sys.time()))
save_env_lt_draws(with_shock_env, save_year = current_year, output_type = "env", shock_hiv_type = "wshock", out_dir = master_dir)

print(paste0("Envelope summary ", Sys.time()))
save_env_lt_summary(summary_env, save_year = current_year, output_type = "env", shock_hiv_type = "wshock", out_dir = master_dir)

