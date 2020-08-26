###############################################################################################################
## Create Region, Super-Region, Global, and SDI results using aggregated shock results

###############################################################################################################
## Set up settings
  rm(list=ls())

  user <- Sys.getenv("USER")

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
  library(argparse)

  library(mortdb, lib= FILEPATH)
  library(mortcore, lib= FILEPATH)
  library(ltcore, lib= FILEPATH)

  parser <- ArgumentParser()
  parser$add_argument('--shock_death_number_estimate_version', type="integer", required=TRUE,
                      help='The with shock death number estimate version for this run')
  parser$add_argument('--shock_aggregator_version', type="integer", required=TRUE,
                      help='Shock aggregator version')
  parser$add_argument('--year', type="integer", required=TRUE,
                      help='Year to estimate')
  parser$add_argument('--gbd_year', type="integer", required=TRUE,
                      help='GBD Year')

  args <- parser$parse_args()
  shocks_addition_run_id <- args$shock_death_number_estimate_version
  shock_aggregator_run_id <- args$shock_aggregator_version
  current_year <- args$year
  gbd_year <- args$gbd_year

## Look up parallelization metadata

  loc_map <- fread(FILEPATH)
  lowest_map <- fread(FILEPATH)
  run_countries <- unique(lowest_map$ihme_loc_id)

  agg_locs <- unique(loc_map[level <= 2, location_id])

## Set general ID variables
  id_vars <- c("location_id", "year_id", "sex_id", "age_group_id", "draw")

## Age file
  age_map <- fread(FILEPATH)
  age_map <- age_map[, list(age_group_id, age_group_years_start)]
  setnames(age_map, "age_group_years_start", "age")

## Population file
  pop_dt <- fread(FILEPATH)

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

shock_filenames <- "FILEPATH"
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

no_shock_env <- rbindlist(lapply(agg_locs, load_hdf, filepath = FILEPATH))
no_shock_lt <- rbindlist(lapply(agg_locs, load_hdf, filepath =FILEPATH))
with_shock_ax <- fread(FILEPATH)
with_shock_ax <- with_shock_ax[location_id %in% agg_locs]

no_shock_lt <- no_shock_lt[!duplicated(no_shock_lt, by=id_vars)]
with_shock_ax <- with_shock_ax[!duplicated(with_shock_ax, by=id_vars)]

with_shock_env <- merge(no_shock_env[age_group_id %in% env_ages], shock_numbers, by = id_vars)
with_shock_env[, deaths := deaths + shock_deaths]

with_shock_env[, shock_deaths := NULL]

# age and sex aggregation
with_shock_env <- agg_results(with_shock_env,
                              id_vars = id_vars,
                              value_vars = c("deaths"),
                              agg_sex = T, agg_hierarchy = F, age_aggs = "gbd_compare", gbd_year = gbd_year)

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
                  agg_sex = T, agg_hierarchy = F, gbd_year = gbd_year)

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

  # combine 28 and neonatal together
  dt[, age_length := NULL]
  final_dt <- rbind(dt, u1q0, use.names=T, fill=T)
  return(final_dt)
}

nn_mx_ax_qx <- rbindlist(lapply(1:3, get_1q0, nn_dt=nn_mx_ax, id_vars=id_vars), use.names=T, fill=T)

# Use existing mx/ax for sex_id 3
both_sex_nn <- nn_mx_ax_qx[sex_id==3 & age_group_id==28]
both_sex_nn <- merge(both_sex_nn, with_shock_lt[age_group_id==28, .SD, .SDcols=c(id_vars, 'mx', 'ax')],
                     by=id_vars)
# replace mx/ax with new values
both_sex_nn[, `:=`(mx=mx.y, ax=ax.y)]
both_sex_nn[, c('mx.x', 'mx.y', 'ax.x', 'ax.y') := NULL]

# combine with rest of nn results
nn_mx_ax_qx <- rbind(nn_mx_ax_qx[!(sex_id == 3 & age_group_id == 28)], both_sex_nn, use.names=T)

# check consistency
assertable::assert_ids(nn_mx_ax_qx, id_vars = list(
  location_id = agg_locs,
  year_id = current_year,
  sex_id = 1:3,
  age_group_id = c(2:4, 28),
  draw = 0:999
))

with_shock_lt[, c("age_length", "age") := NULL]
with_shock_lt <- rbindlist(list(with_shock_lt[age_group_id != 28], nn_mx_ax_qx), use.names = T, fill = T)

summary_lt <- gen_summary_lt(with_shock_lt,
                             id_vars = id_vars[!id_vars %in% c("draw")],
                             qx_agg_draws = T,
                             ex_draws = T,
                             rescale_nn = F,
                             preserve_u5 = 1,
                             cap_qx = 1)

###############################################################################################################
## Save with-shock envelope and LT for all aggregate locations
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

save_env_lt_draws <- function(dt, save_year, output_type, shock_hiv_type, out_dir) {

  filepath <- FILEPATH

  print(paste0("Saving ", output_type, " ", shock_hiv_type, " to ", filepath))
  setkey(dt, location_id)

  existing_locs <- h5ls(filepath, datasetinfo = F)$name
  existing_locs <- existing_locs[existing_locs %in% save_locs]
  if(length(existing_locs) > 0) {
    for(loc in existing_locs) {
      rhdf5::h5delete(filepath, paste(loc))
    }
  }

  invisible(lapply(save_locs, save_hdf,
                   data = dt[, .SD, .SDcols = c("location_id", "sex_id", "year_id", "age_group_id", "draw", save_cols)],
                   filepath = filepath,
                   by_var="location_id", level=0))

}

print(paste0("LT Draw filesave ", Sys.time()))

save_env_lt_draws(with_shock_lt, save_year = current_year, output_type = "lt", shock_hiv_type = "wshock", out_dir = master_dir)
save_env_lt_draws(summary_lt[["draws_ex"]], save_year = current_year,
                  output_type = "ex", shock_hiv_type = "wshock", out_dir = master_dir)
save_env_lt_draws(summary_lt[["draws_agg_qx"]], save_year = current_year,
                  output_type = "qx_agg", shock_hiv_type = "wshock", out_dir = master_dir)
save_env_lt_summary(summary_lt[["summary_lt"]], save_year = current_year, output_type = "lt", shock_hiv_type = "wshock", out_dir = master_dir)

print(paste0("Envelope draw filesave ", Sys.time()))
save_env_lt_draws(with_shock_env, save_year = current_year, output_type = "env", shock_hiv_type = "wshock", out_dir = master_dir)

print(paste0("Envelope summary ", Sys.time()))
save_env_lt_summary(summary_env, save_year = current_year, output_type = "env", shock_hiv_type = "wshock", out_dir = master_dir)

