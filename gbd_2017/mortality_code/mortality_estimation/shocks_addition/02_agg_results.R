###############################################################################################################
## Aggregate all life tables and envelope from lowest level to aggregated location/age/sex

###############################################################################################################
## Set up settings
  rm(list=ls())

  if (Sys.info()[1]=="Windows") {
    root <- "J:" 
    user <- Sys.getenv("USERNAME")
  } else {
    root <- "/home/j"
    user <- Sys.getenv("USER")
    current_year <- as.integer(commandArgs(trailingOnly = T)[1])
    shocks_addition_run_id <- as.integer(commandArgs(trailingOnly = T)[2])
    gbd_year <- as.integer(commandArgs(trailingOnly = T)[3])
  }

  ncores <- 6

  library(rhdf5)
  library(parallel)
  library(data.table)
  library(assertable) # For check_files, import_files, etc.
  library(haven)
  library(readr)
  library(ltcore)
  library(mortdb)
  library(mortcore)

  code_dir <- FILEPATH
  source(paste0(code_dir, "/rescale_nn_qx_mx.R"))
  source(paste0(code_dir, "/gen_summary_lt.R"))

  test <- F

  if(test == T) {
    current_year <- 1986
    shocks_addition_run_id <- 37
    gbd_year <- 2017
  }

  master_dir <- paste0(FILEPATH,shocks_addition_run_id)
  input_dir <- paste0(master_dir, "/inputs")
  lowest_dir <- paste0(master_dir, "/lowest_outputs")

  master_ids <- c("location_id", "year_id", "sex_id", "age_group_id")
  draw_ids <- c(master_ids, "draw")
  

###############################################################################################################
## Import map files, assign ID values
## Location file
  loc_map <- fread(paste0(input_dir,"/all_locations.csv"))
  lowest_map <- fread(paste0(input_dir, "/lowest_locations.csv"))
  run_countries <- unique(lowest_map$ihme_loc_id)

  agg_locs <- unique(loc_map[level <= 2, location_id])

  ## Resample run countries so that all of the parallel jobs aren't hitting the same HDF at the same time
  set.seed(current_year)
  import_countries <- sample(run_countries, length(run_countries), replace = FALSE)
  if(!identical(sort(run_countries), sort(import_countries))) stop("Country resample failed")

  ## Age file
  age_map <- fread(paste0(input_dir,"/age_map.csv"))
  age_map <- age_map[, list(age_group_id, age_group_years_start)]
  setnames(age_map, "age_group_years_start", "age")
  env_age_map <- age_map[age <= 90, list(age_group_id, age)]
  env_age_map[, age := as.character(age)]

  ## Create ID combinations to use with assert_ids
  gbd_round <- gbd_year - 2012
  sexes <- c("male", "female")
  years <- c(1950:gbd_year)
  sims <- c(0:999)
  ages <- unique(age_map$age)

  ## Child and adult sims
  sim_ids <- list(ihme_loc_id = run_countries,
                  year = c(1950:gbd_year),
                  sex = c("male", "female"),
                  simulation = c(0:999))

  pop_dt <- fread(paste0(input_dir, "/population.csv"))

  ## Generate new populations for 95+ granular age groups, to use as relative population weights
  fill_ages <- function(new_age_group_id) {
    dt <- pop_dt[age_group_id == 235]
    dt[, age_group_id := new_age_group_id]
    return(dt)
  }

  old_pops <- rbindlist(lapply(c(33, 44, 45, 148), fill_ages))

  pop_dt <- rbindlist(list(pop_dt, old_pops))

  setkeyv(pop_dt, master_ids)

  ## mx and ax from mortality results
  mx_ax_filenames <- paste0("mx_ax_", import_countries, ".h5")
  mx_ax <- assertable::import_files(mx_ax_filenames, 
                                    folder = lowest_dir,
                                    FUN = load_hdf,
                                    by_val = current_year,
                                    multicore = T, 
                                    mc.cores = 2)
  setnames(mx_ax, c("mx_with_shock", "ax_with_shock"), c("mx_wshock", "ax_wshock"))
  setkeyv(mx_ax, master_ids)

  ## All-age with-shock envelope
  shock_env_filenames <- paste0("final_wshock_env_", import_countries, ".h5")
  wshock_env <- assertable::import_files(shock_env_filenames, 
                                    folder = lowest_dir,
                                    FUN = load_hdf,
                                    by_val = current_year,
                                    multicore = T, 
                                    mc.cores = 2)
  setnames(wshock_env, "deaths_with_shock", "deaths_wshock")
  setkeyv(wshock_env, master_ids)

  ## u5 envelope from mortality results
  u5_env_filenames <- paste0("u5_env_", import_countries, ".h5")
  u5_env <- assertable::import_files(u5_env_filenames, 
                                    folder = lowest_dir,
                                    FUN = load_hdf,
                                    by_val = current_year,
                                    multicore = T, 
                                    mc.cores = 2)
  setnames(u5_env, "deaths_with_shock", "deaths_wshock")
  setkeyv(u5_env, master_ids)

  ## 95+ envelope from mortality results
  over_95_filenames <- paste0("over_95_env_", import_countries, ".h5")
  over_95_env <- assertable::import_files(over_95_filenames, 
                                    folder = lowest_dir,
                                    FUN = load_hdf,
                                    by_val = current_year,
                                    multicore = T, 
                                    mc.cores = 2)
  setnames(over_95_env, "deaths_with_shock", "deaths_wshock")
  setkeyv(over_95_env, master_ids)


###############################################################################################################
## Generate summarization and collapsing functions
lower <- function(x) quantile(x, probs=.025, na.rm=T)
upper <- function(x) quantile(x, probs=.975, na.rm=T)

agg_mx_ax <- function(dt, pop_dt, age_map, shock_hiv_type, id_vars) {
  dt_rows <- nrow(dt)
  dt <- merge(dt, pop_dt, by = c("location_id", "year_id", "sex_id", "age_group_id"))
  if(nrow(dt) != dt_rows) stop("Dropped rows through population merge")

  dt[, deaths := get(paste0("mx_", shock_hiv_type)) * population]
  setnames(dt, paste0("ax_", shock_hiv_type), "ax")

  dt <- dt[, .SD, .SDcols = c(id_vars, "deaths", "ax", "population")]
  dt[, ax := ax * deaths]

  agg_dt <- agg_results(dt, 
                    id_vars = id_vars, 
                    value_vars = c("deaths", "ax", "population"),
                    agg_sex = T, loc_scalars = T, agg_hierarchy = T,
                    agg_sdi = T)

  agg_dt[, ax := ax / deaths]
  agg_dt[, deaths := deaths / population]
  setnames(agg_dt, "deaths", "mx")
  agg_dt[, c("population") := NULL]

  ## Split out NN mx for separate calculations
  nn_mx_ax <- agg_dt[age_group_id %in% 2:4]
  agg_dt <- agg_dt[!age_group_id %in% 2:4]

  agg_dt <- merge(agg_dt, age_map, by = "age_group_id")
  gen_age_length(agg_dt)
  agg_dt[, qx := mx_ax_to_qx(m = mx, a = ax, t = age_length)]
  agg_dt[age == 110, qx := 1]

  ## Insert NN rescaling here -- under-1 qx will be completely set here, and then we can just proceed as per usual
  nn_mx_ax_qx <- rescale_nn_qx_mx(agg_dt[age_group_id == 28],
                                  nn_mx_ax,
                                  id_vars = c("location_id", "year_id", "sex_id", "draw"))

  agg_dt[, c("age_length", "age") := NULL]
  agg_dt <- rbindlist(list(agg_dt, nn_mx_ax_qx), use.names = T, fill = T)

  ## ax is set as part of lifetable equations etc.
  return(agg_dt)
}

gen_summary_env <- function(env, id_vars) {
  summary_env <- env[, list(mean = mean(deaths, na.rm = T),
                            lower = quantile(deaths, probs=.025, na.rm=T), 
                            upper = quantile(deaths, probs=.975, na.rm=T)), 
                     by = id_vars]
  return(summary_env)
}

save_env_lt_draws <- function(dt, save_year, output_type, shock_hiv_type, out_dir) {
  save_folder <- paste0(out_dir, "/", output_type, "_", shock_hiv_type)

  save_locs <- unique(dt$location_id)

  if(output_type == "lt" & shock_hiv_type == "wshock") {
    save_locs <- save_locs[!save_locs %in% agg_locs]
  }

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

  if(output_type == "lt" & shock_hiv_type == "wshock") {
    ax_filepath <- paste0(save_folder, "/ax/ax_", save_year, ".csv")
    write_csv(dt[location_id %in% agg_locs, .SD, .SDcols = c("location_id", "sex_id", "year_id", "age_group_id", "draw", "ax")], 
              ax_filepath)
    save_locs <- save_locs[!save_locs %in% agg_locs]
  }

  if(file.exists(filepath)) file.remove(filepath)
  rhdf5::h5createFile(filepath)
  invisible(lapply(save_locs, save_hdf, 
                   data = dt[, .SD, .SDcols = c("location_id", "sex_id", "year_id", "age_group_id", "draw", save_cols)], 
                   filepath = filepath, 
                   by_var="location_id", level=0))
  rhdf5::H5close()

  if(output_type == "env" & shock_hiv_type == "no_hiv") {
    dt[, draw := paste0("env_", draw)]
    setnames(dt, "population", "pop")
    dt <- dcast.data.table(dt, location_id + year_id + sex_id + age_group_id + pop ~ draw, value.var = "deaths")

    codcorrect_filepath <- paste0(save_folder, "/codcorrect_draws/", filename)

    if(file.exists(codcorrect_filepath)) file.remove(codcorrect_filepath)
    rhdf5::h5createFile(codcorrect_filepath)
    invisible(save_hdf(data= dt, filepath = codcorrect_filepath, level=0))
    rhdf5::H5close()
  }
}

save_env_lt_summary <- function(dt, save_year, output_type, shock_hiv_type, out_dir) {
  save_folder <- paste0(out_dir, "/", output_type, "_", shock_hiv_type)
  if(output_type == "lt") filename <- paste0("summary_lt_", save_year, ".csv")
  if(output_type == "env") filename <- paste0("summary_env_", save_year, ".csv")
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
  print(head(dt))
  write_csv(dt, filepath)
}

validate_summary_lt <- function(dt, out_dir) {
  # test that mean is within lower and upper bounds
  dt[, mean_test := signif(mean, 5)]
  dt[, upper_test := signif(upper, 5)]
  dt[, lower_test := signif(lower, 5)]
  greater_than_upper <- assert_values(dt, colnames = c("mean_test"), test = "lte", test_val = dt[, upper_test], warn_only = T)
  less_than_lower <- assert_values(dt, colnames = c("mean_test"), test = "gte", test_val = dt[, lower_test], warn_only = T)
  dt[, c("mean_test", "lower_test", "upper_test") := NULL]

  if (!is.null(greater_than_upper)) {
    write_csv(greater_than_upper, paste0(out_dir, "/summary_lt_greater_than_upper.csv"))
  }
  if (!is.null(less_than_lower)) {
    write_csv(less_than_lower, paste0(out_dir, "/summary_lt_less_than_lower.csv"))
  }
}

validate_summary_envelope <- function(dt, out_dir) {
  # test that mean is within lower and upper bounds
  dt[, mean_test := signif(mean, 5)]
  dt[, upper_test := signif(upper, 5)]
  dt[, lower_test := signif(lower, 5)]
  greater_than_upper <- assert_values(dt, colnames = c("mean_test"), test = "lte", test_val = dt[, upper_test], warn_only = T)
  less_than_lower <- assert_values(dt, colnames = c("mean_test"), test = "gte", test_val = dt[, lower_test], warn_only = T)
  dt[, c("mean_test", "lower_test", "upper_test") := NULL]
  
  if (!is.null(greater_than_upper)) {
    write_csv(greater_than_upper, paste0(out_dir, "/summary_env_greater_than_upper.csv"))
  }
  if (!is.null(less_than_lower)) {
    write_csv(less_than_lower, paste0(out_dir, "/summary_env_less_than_lower.csv"))
  }
}

###############################################################################################################
## Aggregate, summarize, and output files


run_agg_pipeline <- function(mx_ax, u5_env, over_95_env, wshock_env, pop_dt, shock_hiv_type, age_map) {
  start_time <- Sys.time()

  print(paste0("LT mx ax aggregation ", Sys.time()))
  dt <- agg_mx_ax(dt = mx_ax, pop_dt = pop_dt, age_map = age_map, shock_hiv_type = shock_hiv_type, id_vars = draw_ids)

  print(paste0("LT Draw filesave ", Sys.time()))
  save_env_lt_draws(dt, save_year = current_year, output_type = "lt", shock_hiv_type = shock_hiv_type, out_dir = master_dir)
  if(shock_hiv_type == "wshock") dt <- dt[!location_id %in% agg_locs]
  summary_lt <- gen_summary_lt(dt, pop_dt = pop_dt, id_vars = master_ids)
  
  validate_summary_lt(summary_lt[[1]], paste0(master_dir, "/logs"))
  
  save_env_lt_summary(summary_lt[[1]], save_year = current_year, output_type = "lt", shock_hiv_type = shock_hiv_type, out_dir = master_dir)

  ## Convert to envelope draws and aggregate to larger age groups
  dt <- dt[!age_group_id %in% c(33, 44, 45, 148), .SD, .SDcols = c(draw_ids, "mx")]

  dt <- merge(dt, pop_dt, by = master_ids)
  dt[, deaths := mx * population]
  dt[, c("mx") := NULL]

  death_var <- paste0("deaths_", shock_hiv_type)

  if(shock_hiv_type != "wshock") {
    over_95_prepped <- over_95_env[, .SD, .SDcols = c(draw_ids, death_var)]
    setnames(over_95_prepped, death_var, "deaths")
    over_95_prepped <- merge(over_95_prepped, pop_dt, by = master_ids)
    over_95_prepped <- agg_results(over_95_prepped, value_vars = c("deaths", "population"), id_vars = draw_ids,
                                   agg_sex = T, loc_scalars = T, agg_hierarchy = T,
                                   agg_sdi = T)

    u5_env <- merge(u5_env, pop_dt, by = master_ids)
    u5_env <- u5_env[, .SD, .SDcols = c(draw_ids, death_var, "population")]
    setnames(u5_env, death_var, "deaths")
    u5_env <- agg_results(u5_env, value_vars = c("deaths", "population"), id_vars = draw_ids,
                          agg_sex = T, loc_scalars = T, agg_hierarchy = T,
                          agg_sdi = T)

    dt <- rbindlist(list(dt[!age_group_id %in% c(2:5, 28)], over_95_prepped, u5_env), use.names = T)
  } else {
    dt <- merge(wshock_env, pop_dt, by = master_ids)
    setnames(dt, death_var, "deaths")
    
    dt <- agg_results(dt, value_vars = c("deaths", "population"), id_vars = draw_ids,
                      agg_sex = T, loc_scalars = F, agg_hierarchy = T, end_agg_level = 3,
                      agg_sdi = T)
  }
  
  print(paste0("Envelope age aggregation ", Sys.time()))
  dt <- agg_results(dt[age_group_id != 28], value_vars = c("deaths", "population"), id_vars = draw_ids, age_aggs = "gbd_compare", agg_hierarchy = F)
  
  print(paste0("Envelope draw filesave ", Sys.time()))
  save_env_lt_draws(dt, save_year = current_year, output_type = "env", shock_hiv_type = shock_hiv_type, out_dir = master_dir)

  print(paste0("Envelope summary ", Sys.time()))
  summary_env <- gen_summary_env(dt, id_vars = master_ids)
  
  validate_summary_envelope(summary_env, paste0(master_dir, "/logs"))
  
  save_env_lt_summary(summary_env, save_year = current_year, output_type = "env", shock_hiv_type = shock_hiv_type, out_dir = master_dir)

  end_time <- Sys.time()
  total_runtime <- end_time - start_time
  print(total_runtime)
  return(data.table(shock_hiv_type = shock_hiv_type, runtime = total_runtime))
}

mclapply(c("wshock", "no_shock", "no_hiv"), 
         run_agg_pipeline,
         mx_ax = mx_ax,
         u5_env = u5_env, 
         over_95_env = over_95_env,
         wshock_env = wshock_env,
         pop_dt = pop_dt,
         age_map = age_map,
         mc.cores = 3)


