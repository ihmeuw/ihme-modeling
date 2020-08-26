###############################################################################################################
## Aggregate all life tables and envelope from national level to regional/super-region/special locations

###############################################################################################################
## Set up settings
  rm(list=ls())
  options(digits = 22)
  user <- Sys.getenv("USER")

  library(rhdf5)
  library(parallel)
  library(data.table)
  library(assertable)
  library(readr)
  library(argparse)

  library(ltcore, lib = FILEPATH)
  library(mortdb, lib = FILEPATH)
  library(mortcore, lib = FILEPATH)

  parser <- ArgumentParser()
  parser$add_argument('--shock_death_number_estimate_version', type="integer", required=TRUE,
                      help='The with shock death number estimate version for this run')
  parser$add_argument('--year', type="integer", required=TRUE,
                      help='Year to estimate')
  parser$add_argument('--gbd_year', type="integer", required=TRUE,
                      help='GBD Year')
  parser$add_argument('--aggregate_locations', type="character", required=TRUE,
                      help='True/False whether or not to aggregate locations')

  args <- parser$parse_args()
  shocks_addition_run_id <- args$shock_death_number_estimate_version
  current_year <- args$year
  gbd_year <- args$gbd_year
  aggregate_locations <- args$aggregate_locations

  master_ids <- c("location_id", "year_id", "sex_id", "age_group_id")
  draw_ids <- c(master_ids, "draw")


###############################################################################################################
## Import map files, assign ID values

  loc_map <- fread(FILEPATH)
  lowest_map <- fread(FILEPATH)
  run_countries <- unique(lowest_map$ihme_loc_id)

  run_countries <- c(run_countries, paste0("NOR_", 60132:60137))

  agg_locs <- unique(loc_map[level <= 2, location_id])

  set.seed(current_year)
  import_countries <- sample(run_countries, length(run_countries), replace = FALSE)
  if(!identical(sort(run_countries), sort(import_countries))) stop("Country resample failed")

  ## Age file
  age_map <- fread(FILEPATH)
  age_map <- age_map[, list(age_group_id, age_group_years_start)]
  setnames(age_map, "age_group_years_start", "age")
  env_age_map <- age_map[age <= 90, list(age_group_id, age)]
  env_age_map[, age := as.character(age)]

  ## Create ID combinations to use with assert_ids
  gbd_round <- get_gbd_round(gbd_year)
  sexes <- c("male", "female")
  years <- c(1950:gbd_year)
  sims <- c(0:999)
  ages <- unique(age_map$age)

  ## Child and adult sims
  sim_ids <- list(ihme_loc_id = run_countries,
                  year = c(1950:gbd_year),
                  sex = c("male", "female"),
                  simulation = c(0:999))

  pop_dt <- fread(FILEPATH)

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
  mx_ax_filenames <- FILEPATH
  mx_ax <- assertable::import_files(mx_ax_filenames,
                                    folder = lowest_dir,
                                    FUN = load_hdf,
                                    by_val = current_year,
                                    multicore = T,
                                    mc.cores = 2)


  setnames(mx_ax, c("mx_with_shock", "ax_with_shock"), c("mx_wshock", "ax_wshock"))
  setkeyv(mx_ax, master_ids)

  ## All-age with-shock envelope

  lowest_locations <- fread(FILEPATH)$location_id

  env_filenames <- FILEPATH
  final_env <- assertable::import_files(env_filenames,
                                    folder = lowest_dir,
                                    FUN = load_hdf,
                                    by_val = current_year,
                                    multicore = T,
                                    mc.cores = 2)

  setnames(final_env, "deaths_with_shock", "deaths_wshock")
  setkeyv(final_env, master_ids)


###############################################################################################################
## Generate summarization and collapsing functions
lower <- function(x) quantile(x, probs=.025, na.rm=T)
upper <- function(x) quantile(x, probs=.975, na.rm=T)

agg_mx_ax <- function(dt, pop_dt, age_map, shock_hiv_type, id_vars, gbd_year, aggregate_locations) {
  dt_rows <- nrow(dt)
  dt <- merge(dt, pop_dt, by = c("location_id", "year_id", "sex_id", "age_group_id"))
  if(nrow(dt) != dt_rows) stop("Dropped rows through population merge")

  dt[, deaths := get(paste0("mx_", shock_hiv_type)) * population]
  setnames(dt, paste0("ax_", shock_hiv_type), "ax")

  dt <- dt[, .SD, .SDcols = c(id_vars, "deaths", "ax", "population")]
  dt[, ax := ax * deaths]

  if(aggregate_locations) {

    # China aggregation first
    agg_dt <- agg_results(dt,
                       id_vars = id_vars,
                       value_vars = c("deaths", "ax", "population"),
                       agg_sex = F, loc_scalars = T,
                       start_agg_level = 5,
                       tree_only = "CHN",
                       agg_hierarchy = T, # Normal location hierarchy aggregates
                       gbd_year = gbd_year)

    agg_dt <- agg_results(agg_dt,
                      id_vars = id_vars,
                      value_vars = c("deaths", "ax", "population"),
                      agg_sex = T, loc_scalars = T,
                      start_agg_level = 3, # national on up
                      agg_hierarchy = T, # Normal location hierarchy aggregates
                      agg_reporting = T, # Special location aggregates
                      gbd_year = gbd_year)
  } else {

    agg_dt <- agg_results(dt,
                          id_vars = id_vars,
                          value_vars = c("deaths", "ax", "population"),
                          agg_sex = F,
                          start_agg_level = 5,
                          tree_only = "CHN",
                          agg_hierarchy = T, # Normal location hierarchy aggregates
                          agg_reporting = F, # Special location aggregates
                          gbd_year = gbd_year)

    agg_dt <- agg_results(agg_dt,
                          id_vars = id_vars,
                          value_vars = c("deaths", "ax", "population"),
                          agg_sex = T,
                          start_agg_level = 3,
                          agg_hierarchy = T, # Normal location hierarchy aggregates
                          agg_reporting = F, # Special location aggregates
                          gbd_year = gbd_year)
  }

  agg_dt[, ax := ax / deaths]
  agg_dt[, deaths := deaths / population]
  setnames(agg_dt, "deaths", "mx")

  ## Preserve population from aggregated locations, so that it can merge seamlessly to generate converted envelope rates
  pop_dt <- agg_dt[, .SD, .SDcols = c(id_vars, "population")]
  agg_dt[, population := NULL]

  ## Split out NN mx for separate calculations
  nn_mx_ax <- agg_dt[age_group_id %in% 2:4]
  agg_dt <- agg_dt[!age_group_id %in% 2:4]

  agg_dt <- merge(agg_dt, age_map, by = "age_group_id")
  gen_age_length(agg_dt)

  agg_dt[age >= 5, qx := mx_ax_to_qx(m = mx, a = ax, t = age_length)]
  agg_dt[age < 5, qx := mx_to_qx(m=mx, t=age_length)]
  agg_dt[age == 110, qx := 1]

  agg_dt[qx > 1, qx := 0.999]

  ## Insert NN rescaling her
  get_1q0 <- function(nn_dt, sex, id_vars) {


    # Set age length
    age_length <- data.table(
      age_group_id = 2:4,
      age_length = c(7/365, 21/365, (365-21-7)/365)
    )

    dt <- merge(nn_dt[sex_id==sex], age_length, by='age_group_id')

    # get qx
    dt[, qx := ltcore::mx_to_qx(mx, age_length)]

    # Aggregate up qx
    u1q0 <- dt[, .(qx = 1-prod(1-qx), age_group_id=28), by=setdiff(id_vars, 'age_group_id')]

    # Apply sex and qx specific formula
    if (sex==1) {

      u1q0[, ax := ifelse(
        qx < 0.0226, 0.1493 - 2.0367*qx, ifelse(
          between(qx, 0.0226, 0.0785),
          0.0244 + 3.4994*qx,
          0.2991
        )
      )]

    } else if (sex==2) {

      u1q0[, ax := ifelse(
        qx < 0.017, 0.1490 - 2.0867*qx, ifelse(
          between(qx, 0.017, 0.0658),
          0.0438 + 4.1075*qx,
          0.3141
        )
      )]

    } else {
      u1q0[, ax := NA_real_]
    }

    u1q0[, mx := ifelse(sex %in% 1:2,
      ltcore::qx_ax_to_mx(q=qx, a=ax, t=1),
      NA_real_)]


    dt[, age_length := NULL]
    final_dt <- rbind(dt, u1q0, use.names=T, fill=T)
    return(final_dt)
  }

  nn_mx_ax_qx <- rbindlist(lapply(1:3, get_1q0, nn_dt=nn_mx_ax, id_vars=id_vars), use.names=T, fill=T)

  # Use existing mx/ax for sex_id 3
  both_sex_nn <- nn_mx_ax_qx[sex_id==3 & age_group_id==28]
  both_sex_nn <- merge(both_sex_nn, agg_dt[age_group_id==28, .SD, .SDcols=c(id_vars, 'mx', 'ax')],
                       by=id_vars)
  # replace mx/ax with new values
  both_sex_nn[, `:=`(mx=mx.y, ax=ax.y)]
  both_sex_nn[, c('mx.x', 'mx.y', 'ax.x', 'ax.y') := NULL]

  # combine with rest of nn results
  nn_mx_ax_qx <- rbind(nn_mx_ax_qx[!(sex_id == 3 & age_group_id==28)], both_sex_nn, use.names=T)

  agg_dt[, c("age_length", "age") := NULL]
  agg_dt <- rbindlist(list(agg_dt[age_group_id != 28], nn_mx_ax_qx), use.names = T, fill = T)

  agg_dt <- merge(agg_dt, pop_dt, by = id_vars)

  return(agg_dt)
}

gen_summary_env <- function(env, id_vars) {
  summary_env <- env[, list(mean = mean(deaths, na.rm = T),
                            lower = lower(deaths),
                            upper = upper(deaths)),
                     by = id_vars]
  return(summary_env)
}

save_env_lt_draws <- function(dt, save_year, output_type, shock_hiv_type, out_dir) {

  filepath <- FILEPATH
  rhdf5::h5createFile(filepath)
  invisible(lapply(save_locs, save_hdf,
                   data = dt[, .SD, .SDcols = c("location_id", "sex_id", "year_id", "age_group_id", "draw", save_cols)],
                   filepath = filepath,
                   by_var="location_id", level=0))
  rhdf5::H5close()

}

save_env_lt_summary <- function(dt, save_year, output_type, shock_hiv_type, out_dir) {

  filepath <- FILEPATH

  if(output_type == "lt") {
    dt <- copy(dt)
    dt[lt_parameter == "mx", life_table_parameter_id := 1]
    dt[lt_parameter == "ax", life_table_parameter_id := 2]
    dt[lt_parameter == "qx", life_table_parameter_id := 3]
    dt[lt_parameter == "lx", life_table_parameter_id := 4]
    dt[lt_parameter == "ex", life_table_parameter_id := 5]
    dt[lt_parameter == "nLx", life_table_parameter_id := 7]
    dt[lt_parameter == "Tx", life_table_parameter_id := 8]
    dt[, lt_parameter := NULL]
  }

  print(paste0("Saving ", output_type, " ", shock_hiv_type, " to ", filepath))
  write_csv(dt, filepath)
}

validate_summary_lt <- function(dt, out_dir) {

  dt[, mean_test := signif(mean, 5)]
  dt[, upper_test := signif(upper, 5)]
  dt[, lower_test := signif(lower, 5)]
  greater_than_upper <- assert_values(dt, colnames = c("mean_test"), test = "lte", test_val = dt[, upper_test], warn_only = T)
  less_than_lower <- assert_values(dt, colnames = c("mean_test"), test = "gte", test_val = dt[, lower_test], warn_only = T)
  dt[, c("mean_test", "lower_test", "upper_test") := NULL]

  if (!is.null(greater_than_upper)) {
    write_csv(greater_than_upper,FILEPATH)
  }
  if (!is.null(less_than_lower)) {
    write_csv(less_than_lower,FILEPATH)
  }
}

validate_summary_envelope <- function(dt, out_dir) {

  dt[, mean_test := signif(mean, 5)]
  dt[, upper_test := signif(upper, 5)]
  dt[, lower_test := signif(lower, 5)]
  greater_than_upper <- assert_values(dt, colnames = c("mean_test"), test = "lte", test_val = dt[, upper_test], warn_only = T)
  less_than_lower <- assert_values(dt, colnames = c("mean_test"), test = "gte", test_val = dt[, lower_test], warn_only = T)
  dt[, c("mean_test", "lower_test", "upper_test") := NULL]

  if (!is.null(greater_than_upper)) {
    write_csv(greater_than_upper, FILEPATH)
  }
  if (!is.null(less_than_lower)) {
    write_csv(less_than_lower, FILEPATH)
  }
}

###############################################################################################################
## Aggregate, summarize, and output files

run_agg_pipeline <- function(mx_ax, env_results, pop_dt, shock_hiv_type, age_map, gbd_year, aggregate_locations) {
  start_time <- Sys.time()
  death_var <- paste0("deaths_", shock_hiv_type)

  print(paste0("LT mx ax aggregation ", Sys.time()))
  lt <- agg_mx_ax(dt = mx_ax, pop_dt = pop_dt, age_map = age_map, shock_hiv_type = shock_hiv_type, id_vars = draw_ids, gbd_year = gbd_year, aggregate_locations = aggregate_locations)

  agg_pop_dt <- lt[, .SD, .SDcols = c(draw_ids, "population")]
  lt[, population := NULL]

  print(paste0("LT Draw filesave ", Sys.time()))
  save_env_lt_draws(lt, save_year = current_year, output_type = "lt", shock_hiv_type = shock_hiv_type, out_dir = master_dir)
  summary_lt <- gen_summary_lt(lt, id_vars = master_ids,
                               qx_agg_draws = T,
                               ex_draws = T,
                               age_95_draws = T,
                               rescale_nn = F,
                               preserve_u5 = 1,
                               cap_qx = 1)

  save_env_lt_draws(summary_lt[["draws_ex"]][!location_id %in% agg_locs], save_year = current_year,
                    output_type = "ex", shock_hiv_type = shock_hiv_type, out_dir = master_dir)
  save_env_lt_draws(summary_lt[["draws_agg_qx"]][!location_id %in% agg_locs], save_year = current_year,
                    output_type = "qx_agg", shock_hiv_type = shock_hiv_type, out_dir = master_dir)

  if(shock_hiv_type == "wshock") summary_lt[["summary_lt"]] <- summary_lt[["summary_lt"]][!location_id %in% agg_locs]

  validate_summary_lt(summary_lt[["summary_lt"]], paste0(master_dir, "/logs"))

  save_env_lt_summary(summary_lt[["summary_lt"]], save_year = current_year, output_type = "lt", shock_hiv_type = shock_hiv_type, out_dir = master_dir)

  rm(summary_lt, mx_ax, lt)
  gc()

  ## Aggregate envelope draws

  env_results <- env_results[, .SD, .SDcols = c(draw_ids, death_var)]
  env_results <- merge(env_results, pop_dt, by = master_ids)
  setnames(env_results, death_var, "deaths")

  ## For with-shock results, aggregate only up to country-level
  if(shock_hiv_type == "wshock") {
    end_agg_level <- 3
  } else {
    end_agg_level <- 0
  }

  # if aggregate_locations is True, do special location aggregation and
  # sex aggregation, else, just do normal location and sex aggregation
  if(aggregate_locations) {
    env_results <- agg_results(env_results, value_vars = c("deaths", "population"), id_vars = draw_ids,
                           agg_sex = T, loc_scalars = F,
                           agg_hierarchy = T, # Normal location hierarchy aggregates
                           end_agg_level = end_agg_level,
                           agg_reporting = T, # Special location aggregates
                           gbd_year = gbd_year)
  } else {
    env_results <- agg_results(env_results, value_vars = c("deaths", "population"), id_vars = draw_ids,
                           agg_sex = T, loc_scalars = F,
                           agg_hierarchy = T, # Normal location hierarchy aggregates
                           end_agg_level = end_agg_level,
                           agg_reporting = F, # Special location aggregates
                           gbd_year = gbd_year)
  }


  nor_locs <- fread(paste0(input_dir, '/all_plus_special_locations.csv'))[ihme_loc_id %like% "NOR" & level == 5]

  nor_aggs <- env_results[location_id %in% nor_locs$location_id]
  nor_aggs <- merge(nor_aggs, nor_locs[, .(location_id, parent_id)], by='location_id')
  nor_aggs <- nor_aggs[, .(deaths=sum(deaths), population = sum(population)), by=c('parent_id', 'draw', 'sex_id', 'year_id', 'age_group_id')]
  setnames(nor_aggs, 'parent_id', 'location_id')
  env_results <- rbind(env_results, nor_aggs, use.names=T)

  print(paste0("Envelope age aggregation ", Sys.time()))
  env_results <- agg_results(env_results[age_group_id != 28], value_vars = c("deaths", "population"), id_vars = draw_ids, age_aggs = "gbd_compare", agg_hierarchy = F, agg_reporting = F, gbd_year = gbd_year)

  print(paste0("Envelope draw filesave ", Sys.time()))
  save_env_lt_draws(env_results, save_year = current_year, output_type = "env", shock_hiv_type = shock_hiv_type, out_dir = master_dir)

  print(paste0("Envelope summary ", Sys.time()))
  summary_env <- gen_summary_env(env_results, id_vars = master_ids)

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
         env_results = final_env,
         pop_dt = pop_dt,
         age_map = age_map,
         gbd_year = gbd_year,
         aggregate_locations = aggregate_locations,
         mc.cores = 3)


