###############################################################################################################
## AUTHOR
## Run location scaling, parallel by year.
## Scale subnationals from bottom up to countries.
## China exception: Don't scale China mainland, HKG, and MAC to CHN national
## ZAF exception: Aggregate from ZAF children to ZAF national

## Major outputs:
##        Scaled/aggregated HIV-free and with-HIV lifetables, and with-HIV envelopes
##        Draw-level HDF files indexed by location_id
##        Summary files in csvs for upload

###############################################################################################################

rm(list=ls())
user <- Sys.getenv("USER")
ncores <- 10

library(pacman)
pacman::p_load(rhdf5, parallel, data.table, assertable, haven, readr, compiler, argparse)
library(ltcore,   lib = "FILEPATH")
library(mortdb,   lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")

if(interactive()) {
  lt_run_id <- 0
  current_year <- 0
  vers_age_sex <- 0
  gbd_year <- 0
}

parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run')
parser$add_argument('--year', type="integer", required=TRUE,
                    help='Current year')
parser$add_argument('--age_sex_estimate_version', type="integer", required=TRUE,
                    help='Age sex estimate version')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='GBD year')
parser$add_argument('--code_dir', type="character", required=TRUE,
                    help='Directory where MLT code is cloned')

args         <- parser$parse_args()
lt_run_id    <- args$version_id
current_year <- args$year
vers_age_sex <- args$age_sex_estimate_version
gbd_year     <- args$gbd_year
code_dir     <- args$code_dir

master_dir   <- paste0("FILEPATH",lt_run_id)
hiv_free_dir <- paste0(master_dir, "/lt_hiv_free")
with_hiv_dir <- paste0(master_dir, "/lt_with_hiv")
env_dir      <- paste0(master_dir, "/env_with_hiv")
input_dir    <- paste0(master_dir, "/inputs")

agesex_dir <- paste0"FILEPATH", vers_age_sex, "/draws")

id_vars       <- c("location_id", "year", "sex", "age", "sim")
id_vars_noage <- id_vars[id_vars != "age"]
id_vars_base  <- id_vars_noage[id_vars_noage != "sim"]

env_id_vars   <- c("location_id", "year_id", "sex_id", "sim", "age_group_id")
env_save_vars <- env_id_vars[env_id_vars != "sim"]

# source ax function
source(paste0(code_dir, "/mltgeneration/R/recalc_ax.R"))


###############################################################################################################
## Import map files, assign ID values
## Location file
loc_map <- fread(paste0(input_dir,"/lt_env_locations.csv"))
loc_map[, parent_ihme := substr(ihme_loc_id, 1, 3)]
run_countries <- unique(loc_map[is_estimate == 1]$ihme_loc_id)
run_loc_ids   <- unique(loc_map[is_estimate == 1]$location_id)
zaf_locs      <- loc_map[parent_ihme == "ZAF", location_id]
zaf_id        <- loc_map[ihme_loc_id == "ZAF", location_id]

## determine whether this run has only level-3 locations... in which case don't scale/agg
## TODO: setup a more robust way to pass information about whether to agg/scale
subnat_included <- any(run_countries %like% "_")

## Resample run countries so that all of the parallel jobs aren't hitting the same HDF at the same time
set.seed(current_year)
import_countries <- sample(run_countries, length(run_countries), replace = FALSE)
if(!identical(sort(run_countries), sort(import_countries))) stop("Country resample failed")

## Age file
age_map <- fread(paste0(input_dir,"/age_map.csv"))
age_end_map <- age_map[, list(age_group_years_start, age_group_years_end)]
setnames(age_end_map, c("age_group_years_start", "age_group_years_end"), c("age", "age_end"))

age_map <- age_map[, list(age_group_id, age_group_years_start)]
setnames(age_map, "age_group_years_start", "age")
env_age_map <- age_map[age <= 90, list(age_group_id, age)]
env_age_map[, age := as.character(age)]


###############################################################################################################
## Import and weight populations populations

weight_pop <- fread(paste0(input_dir, "/population.csv"))
weight_pop <- weight_pop[year == current_year]

fill_lt_pop <- function(dt, age_map) {
  max_age_val <- 95
  fill_ages <- age_map[age > max_age_val, age]
  
  create_new_age <- function(dt, new_age) {
    dt <- copy(dt[age == max_age_val])
    dt[, age := new_age]
    return(dt)
  }
  
  new_age_dt <- rbindlist(lapply(fill_ages, create_new_age, dt = dt))
  dt <- rbindlist(list(dt, new_age_dt), use.names = T)
  return(dt)
}

weight_pop <- fill_lt_pop(weight_pop, age_map = age_map)
weight_pop <- weight_pop[, .SD, .SDcols = c("location_id", "age", "sex", "year", "population")]
setkeyv(weight_pop, id_vars_base)


###############################################################################################################
## Setup functions shared between lifetable and envelope code
## Scale-aggregation function

scale_agg_lt_env <- function(data, lt_type, id_vars, weight_pop) {
  
  if(lt_type == "hiv_free_lt" | lt_type == "with_hiv_lt") lt_value <- "mx"
  if(lt_type == "with_hiv_env") lt_value <- "deaths"
  
  ## don't agg/scale if no subnational units included (ex: in a test run)
  if (subnat_included) {
    
    orig_rows <- nrow(data)
    
    ## For mx values, merge on weight_pop to allow
    if(lt_value == "mx") {
      data <- merge(data, weight_pop, by = id_vars[id_vars != "sim"], all.x = T)
      assert_values(data, "population", "not_na", quiet = T)
      data[, mx := mx * population]
      env_agg_value <- c(lt_value, "population")
    } else {
      env_agg_value <- lt_value
    }
    
    setkeyv(data, id_vars)
    
    scaled_data <- mortcore::scale_results(
      data[!location_id %in% zaf_locs],
      id_vars = id_vars,
      value_var = lt_value,
      exclude_parent = "CHN",
      exclude_tree = "ZAF",
      gbd_year = gbd_year,
      assert_ids = F
    )
    
    zaf_data <- mortcore::agg_results(
      data[location_id %in% zaf_locs],
      id_vars = id_vars,
      value_vars = env_agg_value,
      end_agg_level = 3,
      loc_scalars = F,
      tree_only = "ZAF",
      gbd_year = gbd_year
    )
    
    data <- rbindlist(list(scaled_data, zaf_data), use.names=T)
    
    if(lt_value == "mx") {
      data[, mx := mx / population]
      assert_values(data, "mx", "not_na", quiet = T)
      data[, population := NULL]
    }
    
    if(orig_rows != nrow(data)) stop(paste0("Original data had ", orig_rows, ": post-scaling has ", nrow(data), " rows"))
    
  }
  
  ## Add sex aggregates if with-HIV envelope
  ## Also aggregate locations (if subnats are included)
  ## Also aggregate ages
  if(lt_type == "with_hiv_env") {
    data <- mortcore::agg_results(
      data,
      id_vars = id_vars,
      value_vars = lt_value,
      agg_hierarchy = subnat_included,
      age_aggs = "gbd_compare",
      agg_sex = T,
      gbd_year = gbd_year,
      loc_scalar_v = "best"
    )
  }
  
  return(data)
}

agg_lt_both_sexes <- function(mx_dt, ax_dt, id_vars, weight_pop) {
  ## Aggregate lifetables to both-sexes by population-weighting mx (deaths, essentially) and death-weighting ax
  data <- merge(mx_dt, ax_dt, by = id_vars)
  both_sex_data <- merge(data, weight_pop, by = id_vars[id_vars != "sim"], all.x = T)
  assert_values(both_sex_data, "population", "not_na", quiet = T)
  both_sex_data[, mx := mx * population]
  both_sex_data[, ax := ax * mx]
  
  both_sex_data[sex == "male", sex_id := 1]
  both_sex_data[sex == "female", sex_id := 2]
  both_sex_data[, sex := NULL]
  
  both_sex_data <- mortcore::agg_results(
    both_sex_data,
    id_vars = c(id_vars[id_vars != "sex"], "sex_id"),
    value_vars = c("mx", "ax", "population"),
    agg_hierarchy = F,
    loc_scalars = F,
    agg_sex = T,
    gbd_year = gbd_year
  )
  
  both_sex_data[, ax := ax / mx]
  both_sex_data[, mx := mx / population]
  both_sex_data[, population := NULL]
  
  both_sex_data[sex_id == 1, sex := "male"]
  both_sex_data[sex_id == 2, sex := "female"]
  both_sex_data[sex_id == 3, sex := "both"]
  both_sex_data[, sex_id := NULL]
  
  assert_values(both_sex_data, c("ax", "mx"), "not_na", quiet = T)
  return(both_sex_data)
}

## HDF convenience save-function
save_lt_env <- function(data, locations, file_prefix) {
  filepath <- paste0(master_dir, "/", file_prefix, current_year, ".h5")
  if (file.exists(filepath)) file.remove(filepath)
  rhdf5::h5createFile(filepath)
  lapply(locations, save_hdf,
         data = data,
         filepath = filepath,
         by_var = "location_id",
         level = 0)
  rhdf5::H5close()
}


###############################################################################################################
## Import MLT pre-scaling results-- stored in location-specific hdf files indexed by year
## Also import age-sex results at the same time to allow to do mclapply efficiently
##   (ran into memory leak issues if done later)

import_agesex <- cmpfun(function(f) {
  agesex_ids <- c("ihme_loc_id", "sex", "year", "simulation")
  dt <- fread(f)
  dt <- dt[floor(year) == current_year]
  dt[, year := floor(year)]
  dt <- dt[, .SD, .SDcols = c(agesex_ids, "q_ch", "q_enn", "q_lnn", "q_pnn")]
  dt[, q_0 := 1 - (1-q_enn) * (1-q_lnn) * (1-q_pnn)]
  dt[, c("q_enn", "q_lnn", "q_pnn") := NULL]
  return(dt)
})

import_results <- cmpfun(function(country) {
  country_id <- loc_map[ihme_loc_id == country, location_id]
  hiv_free_lt <- load_hdf(paste0(hiv_free_dir, "/pre_scaled/hiv_free_lt_", country, ".h5"),
                          by_val = current_year)
  with_hiv_lt <- load_hdf(paste0(with_hiv_dir, "/pre_scaled/with_hiv_lt_", country, ".h5"),
                          by_val = current_year)
  agesex_results <- import_agesex(paste0(agesex_dir, "/", country_id, ".csv"))
  with_hiv_env <- load_hdf(paste0(env_dir, "/pre_scaled/with_hiv_env_", country, ".h5"),
                           by_val = current_year)
  return(list(hiv_free_lt = hiv_free_lt,
              with_hiv_lt = with_hiv_lt,
              agesex_results = agesex_results,
              with_hiv_env = with_hiv_env))
})

## First, check that all files exist
assertable::check_files(filenames = paste0("hiv_free_lt_", import_countries, ".h5"),
                        folder = paste0(hiv_free_dir, "/pre_scaled"))
assertable::check_files(filenames = paste0("with_hiv_lt_", import_countries, ".h5"),
                        folder = paste0(with_hiv_dir, "/pre_scaled"))
assertable::check_files(filenames = paste0("with_hiv_env_", import_countries, ".h5"),
                        folder = paste0(env_dir, "/pre_scaled"))
assertable::check_files(filenames = paste0(run_loc_ids, ".csv"),
                        folder = paste0(agesex_dir))

## Import all input data
results <- mclapply(import_countries, import_results, mc.cores = 5)
hiv_free_lt    <- rbindlist(lapply(1:length(import_countries), function(x) results[[x]]$hiv_free_lt))
with_hiv_lt    <- rbindlist(lapply(1:length(import_countries), function(x) results[[x]]$with_hiv_lt))
agesex_results <- rbindlist(lapply(1:length(import_countries), function(x) results[[x]]$agesex_results))
with_hiv_env   <- rbindlist(lapply(1:length(import_countries), function(x) results[[x]]$with_hiv_env))

rm(results)
gc()

## Prep hiv-free life tables (ax and qx)
hiv_free_lt <- merge(hiv_free_lt, loc_map[, list(ihme_loc_id, location_id)],
                     by = "ihme_loc_id")
hiv_free_lt[, ihme_loc_id := NULL]
setkeyv(hiv_free_lt, id_vars)
hiv_free_ax <- hiv_free_lt[, .SD, .SDcols = c(id_vars, "ax")]
hiv_free_lt[, c("ax", "qx") := NULL]

## Prep with-hiv life tables (ax and qx)
with_hiv_lt <- merge(with_hiv_lt, loc_map[, list(ihme_loc_id, location_id)],
                     by = "ihme_loc_id")
with_hiv_lt[, ihme_loc_id := NULL]
setkeyv(with_hiv_lt, id_vars)
with_hiv_ax <- with_hiv_lt[, .SD, .SDcols = c(id_vars, "ax")]
with_hiv_lt[, c("ax", "qx") := NULL]


###############################################################################################################
## Scale all results, except aggregate ZAF from subnational locations
## Exception: Don't scale HKG, MAC, and CHN mainland to CHN national

## Scale with-HIV mx, merge on ax values, and generate qx based off of mx and ax
with_hiv_lt_scaled <- scale_agg_lt_env(
  data = with_hiv_lt,
  lt_type = "with_hiv_lt",
  id_vars = id_vars,
  weight_pop = weight_pop
)

with_hiv_lt_scaled <- agg_lt_both_sexes(with_hiv_lt_scaled, with_hiv_ax, id_vars, weight_pop)
setkeyv(with_hiv_lt_scaled, id_vars)
gen_age_length(with_hiv_lt_scaled)
with_hiv_lt_scaled[, qx := mx_ax_to_qx(mx, ax, age_length)]

## Generate mx scalars from with-HIV results
setnames(with_hiv_lt, "mx", "pre_scaled_mx")
with_hiv_scalars <- merge(with_hiv_lt_scaled[sex != "both"],
                          with_hiv_lt[, .SD, .SDcols = c(id_vars, "pre_scaled_mx")],
                          by = id_vars)
with_hiv_scalars[, scalar := mx / pre_scaled_mx]

## Apply scalars proportionally to HIV-free results to ensure that it's scaled by the same relative amount
setnames(hiv_free_lt, "mx", "mx_prescale")
hiv_free_lt <- merge(hiv_free_lt,
                     with_hiv_scalars[, .SD, .SDcols = c(id_vars, "scalar")],
                     by = id_vars)
hiv_free_lt[, mx := mx_prescale * scalar]
hiv_free_lt[, c("scalar", "mx_prescale") := NULL]

## Assert that with-HIV mx > HIV-free mx
setnames(with_hiv_scalars, "mx", "with_hiv_mx")
hiv_free_lt <- merge(hiv_free_lt, with_hiv_scalars[, .SD, .SDcols = c(id_vars, "with_hiv_mx")], by = id_vars)
final_diff_mx <- assert_values(hiv_free_lt, "mx", "lte", hiv_free_lt[, with_hiv_mx], warn_only = T)
if(!is.null(final_diff_mx)) {
  final_diff_mx[, diff := with_hiv_mx - mx]
  if(min(final_diff_mx$diff) < -.00001) {
    stop("HIV-free LT mx values are larger than with-HIV LTs by over .00001")
  } else {
    warning("Replacing HIV-free mx to equal with-HIV mx where the difference is less than .00001")
    hiv_free_lt[mx > with_hiv_mx, mx := with_hiv_mx]
  }
}

hiv_free_lt[, with_hiv_mx := NULL]

## Aggregate to both-sexes
hiv_free_lt <- agg_lt_both_sexes(hiv_free_lt, hiv_free_ax, id_vars, weight_pop)

## Recalculate HIV-free qx
ltcore::gen_age_length(hiv_free_lt)
hiv_free_lt[, qx := mx_ax_to_qx(mx, ax, age_length)]

## Recalculate ax values if qx >= 1 or ax < 0
recalc_ax(hiv_free_lt, id_vars = id_vars)
recalc_ax(with_hiv_lt_scaled, id_vars = id_vars)

## Assert lifetable qx = 1 at terminal age, and less than 1 elsewhere
with_hiv_lt_scaled[age == 110, qx := 1]
assertable::assert_values(with_hiv_lt_scaled, "qx", "not_na")
assertable::assert_values(with_hiv_lt_scaled, "qx", "lte", test_val = 1)
assertable::assert_values(with_hiv_lt_scaled, "mx", "gte", test_val = 0)

hiv_free_lt[age == 110, qx := 1]
assertable::assert_values(hiv_free_lt, "qx", "not_na")
assertable::assert_values(hiv_free_lt, "mx", "gte", test_val = 0)


###############################################################################################################
## Swap in Age-sex results for under-5 qx values in with-HIV results
## But for comparison, we should keep it the same to make sure we're getting the same sim-level results (they didn't sub)

agesex_ids <- c("ihme_loc_id", "sex", "year", "simulation")

agesex_results <- melt(agesex_results, id.vars = agesex_ids,
                       measure.vars = grep("q_", names(agesex_results), value = T),
                       variable.name = "age_ch", value.name = "qx")

agesex_results[age_ch == "q_0", age := 0]
agesex_results[age_ch == "q_ch", age := 1]
agesex_results[, age_ch := NULL]
setnames(agesex_results, "simulation", "sim")

agesex_results <- merge(agesex_results, loc_map[, list(ihme_loc_id, location_id)],
                        by = "ihme_loc_id")
agesex_results[, ihme_loc_id := NULL]

swap_agesex_draws <- function(lt, agesex_results) {
  lt_u5 <- lt[age <= 1, list(location_id, sex, year, sim, age, ax, mx)]
  setnames(lt_u5, "mx", "mlt_mx")
  lt_rows <- nrow(lt_u5)
  
  ## Replace under-5 qx values with age-sex values
  lt_u5 <- merge(lt_u5, agesex_results, by = c("location_id", "sex", "year", "age", "sim"))
  if(lt_rows != nrow(lt_u5)) stop(paste0("LT under-5 used to have ", lt_rows, " rows, now has ", nrow(lt_u5)))
  
  ## Convert from qx and ax to mx space
  lt_u5[age == 0, age_length := 1]
  lt_u5[age == 1, age_length := 4]
  lt_u5[, mx := qx_ax_to_mx(qx, ax, age_length)]
  lt_u5[, age_length := NULL]
  
  assertable::assert_values(lt_u5, c("qx", "ax", "mx"), "not_na", quiet=T)
  assertable::assert_values(lt_u5, c("qx", "ax", "mx"), "gte", 0, quiet=T)
  assertable::assert_values(lt_u5, c("qx"), "lte", 1, quiet=T)
  
  ## Generate mx scalars between MLT output and age-sex, to scale HIV-free appropriately
  lt_u5[, as_scalar := mx / mlt_mx]
  lt_scalar <- lt_u5[, list(location_id, sex, year, sim, age, as_scalar)]
  
  lt_u5[, c("mlt_mx", "as_scalar") := NULL]
  
  ## Swap in new lt_u5 into the existing lifetable
  lt <- rbindlist(list(lt[age > 1,], lt_u5), use.names=T)
  return(list(lt_scalar, lt))
}

agesex_swap <- swap_agesex_draws(with_hiv_lt_scaled, agesex_results)

lt_scalar <- agesex_swap[[1]][, .SD, .SDcols = c(id_vars, "as_scalar")]
with_hiv_lt_scaled <- agesex_swap[[2]]

## Scale HIV-free mx by the same value as with-HIV results
hiv_free_lt <- merge(hiv_free_lt, lt_scalar, by = id_vars, all.x = T)
hiv_free_lt[age <= 1, mx := mx * as_scalar]
hiv_free_lt[age == 0, age_length := 1]
hiv_free_lt[age == 1, age_length := 4]
hiv_free_lt[age <= 1, qx := ltcore::mx_ax_to_qx(mx, ax, age_length)]
hiv_free_lt[, c("age_length", "as_scalar") := NULL]

rm(agesex_swap, agesex_results, lt_scalar)
gc()

## Output draw-level mx/ax/qx (Need to refactor reckoning to loop over this, and to do lifetable calculations in-line)
save_lt_cols <- c(id_vars, "mx", "ax", "qx")

setkey(hiv_free_lt, location_id)
setkey(with_hiv_lt_scaled, location_id)
lt_save_locs <- unique(hiv_free_lt[, location_id])

## Save results -- major time investment, can take up to an hour per file.
save_lt_env(hiv_free_lt[, .SD, .SDcols = save_lt_cols],
            locations = lt_save_locs,
            file_prefix = "lt_hiv_free/scaled/hiv_free_lt_")

save_lt_env(with_hiv_lt_scaled[, .SD, .SDcols = save_lt_cols],
            locations = lt_save_locs,
            file_prefix = "lt_with_hiv/scaled/with_hiv_lt_")


###############################################################################################################
## Aggregate, summarize, and output files

## Summarize LT results -- get upper and lower from draw-level results
gen_ci_lt <- function(lt, id_vars) {
  lt <- copy(lt)
  
  ## Merge on age group IDs here, to make generating summary qx values a lot easier
  lt <- merge(lt, age_map, by = "age", all.x=T)
  assertable::assert_values(lt, "age_group_id", "not_na", quiet=T)
  
  setkeyv(lt, id_vars)
  lt[, px := 1 - qx]
  
  ## Generate 45q15
  lt_45q15 <- lt[age >= 15 & age <= 55, list(qx = 1 - prod(px)), by = c(id_vars, "sim")]
  
  ## Generate 5q0
  lt_5q0 <- lt[age >= 0 & age <= 1, list(qx = 1 - prod(px)), by = c(id_vars, "sim")]
  
  ## Generate the lower/upper confidence intervals, as the mean will come from the mean lifetable
  lt_45q15 <- mortcore::summarize_data(lt_45q15, id_vars = id_vars, outcome_var = "qx",
                                       metrics = c("lower", "upper"))
  lt_45q15[, age_group_id := 199]
  lt_5q0 <- mortcore::summarize_data(lt_5q0, id_vars = id_vars, outcome_var = "qx",
                                     metrics = c("lower", "upper"))
  lt_5q0[, age_group_id := 1]
  
  ## Melt all LT variables long
  lt <- melt(lt[, .SD, .SDcols = c(id_vars, "age_group_id", "mx", "ax", "qx")],
             id = c(id_vars, "age_group_id"))
  setnames(lt, "variable", "lt_parameter")
  
  id_vars_agg <- c(id_vars, "age_group_id", "lt_parameter")
  setkeyv(lt, id_vars_agg)
  
  lt <- mortcore::summarize_data(lt,
                                 id_vars = id_vars_agg,
                                 outcome_var = "value",
                                 metrics = c("lower", "upper"),
                                 na.rm = T)
  
  ## Combine datasets
  lt_qx <- rbindlist(list(lt_5q0, lt_45q15))
  lt_qx[, lt_parameter := "qx"]
  lt_qx <- rbindlist(list(lt_qx, lt), use.names = T)
  return(lt_qx)
}

hiv_free_summary <- gen_ci_lt(hiv_free_lt, id_vars_base)
with_hiv_summary <- gen_ci_lt(with_hiv_lt_scaled, id_vars_base)

## Generate full mean lifetable
gen_mean_lt <- function(lt, id_vars) {
  setkeyv(lt, c(id_vars, "age"))
  lt_sum <- lt[, lapply(.SD, mean), .SDcols = c("mx", "ax", "qx"), by = c(id_vars, "age")]
  
  # merge age_end
  lt_sum <- merge(lt_sum, age_end_map, by = "age", all.x = T)
  assertable::assert_values(lt_sum, "age_end", "not_na")
  
  ## Generate full lifetable results
  lt_sum <- lifetable(data = lt_sum, preserve_u5 = 1, by_vars = c(id_vars, "age", "age_end"), cap_qx = T)
  
  ## Generate 5q0 and 45q15 from the mean lifetable
  lt_45q15 <- lt_sum[age >= 15 & age <= 55, list(qx = 1 - prod(px)), by = id_vars]
  lt_45q15[, age_group_id := 199]
  lt_5q0 <- lt_sum[age >= 0 & age <= 1, list(qx = 1 - prod(px)), by = id_vars]
  lt_5q0[, age_group_id := 1]
  
  lt_sum <- merge(lt_sum, age_map, by = "age", all.x=T)
  lt_sum[, age := NULL]
  lt_sum <- melt(lt_sum[, .SD, .SDcols = c(id_vars, "age_group_id", "mx", "ax", "qx", "ex")],
                 id = c(id_vars, "age_group_id"))
  setnames(lt_sum, c("variable", "value"), c("lt_parameter", "mean"))
  
  ## Combine datasets
  lt_qx <- rbindlist(list(lt_5q0, lt_45q15))
  lt_qx[, lt_parameter := "qx"]
  setnames(lt_qx, "qx", "mean")
  
  mean_lt <- rbindlist(list(lt_qx, lt_sum), use.names = T)
  
  return(mean_lt)
}

mean_hiv_free <- gen_mean_lt(hiv_free_lt, id_vars_base)
mean_with_hiv <- gen_mean_lt(with_hiv_lt_scaled, id_vars_base)


## Merge datasets together -- mean LT will have all lifetable-related variables
##  while summary (upper/lower) will only have mx/ax/qx
hiv_free_summary <- merge(hiv_free_summary, mean_hiv_free,
                          all.y = T, by = c(id_vars_base, "age_group_id", "lt_parameter"))
hiv_free_summary[, estimate_stage_id := 13]

with_hiv_summary <- merge(with_hiv_summary, mean_with_hiv,
                          all.y = T, by = c(id_vars_base, "age_group_id", "lt_parameter"))
with_hiv_summary[, estimate_stage_id := 12]

final_lt <- rbindlist(list(hiv_free_summary, with_hiv_summary), use.names=T)

## Output summary LT files
readr::write_csv(final_lt, paste0(master_dir, "/summary/intermediary/lt_", current_year, ".csv"))


###############################################################################################################
## Scale/agg envelope: Aggregate to both-sexes and age aggregates

## Cleanup all lifetable results to free up memory
rm(hiv_free_lt, with_hiv_lt, with_hiv_lt_scaled, hiv_free_ax, with_hiv_ax)
gc()

with_hiv_env <- merge(with_hiv_env, loc_map[, list(ihme_loc_id, location_id)],
                      by = "ihme_loc_id")
with_hiv_env[, ihme_loc_id := NULL]
with_hiv_env[sex == "male", sex_id := 1]
with_hiv_env[sex == "female", sex_id := 2]
with_hiv_env[, sex := NULL]

with_hiv_env <- merge(with_hiv_env, env_age_map, by = "age", all.x=T)
print(unique(with_hiv_env$age))

with_hiv_env[age == "95", age_group_id := 235]
with_hiv_env[age == "enn", age_group_id := 2]
with_hiv_env[age == "lnn", age_group_id := 3]
with_hiv_env[age == "pnn", age_group_id := 4]
with_hiv_env[age == "pna", age_group_id := 388]
with_hiv_env[age == "pnb", age_group_id := 389]
with_hiv_env[age == "cha", age_group_id := 238]
with_hiv_env[age == "chb", age_group_id := 34]

if (gbd_year > 2019) {
  with_hiv_env[age == 1, age_group_id := 238]
  with_hiv_env[age == 2, age_group_id := 34]
}

with_hiv_env[, age := NULL]

setnames(with_hiv_env, "year", "year_id")

## Run scaling/aggregation code
with_hiv_env <- scale_agg_lt_env(with_hiv_env, "with_hiv_env",
                                 id_vars = env_id_vars,
                                 weight_pop = weight_pop)

## Summary envelope:
final_env <- copy(with_hiv_env)
final_env <- mortcore::summarize_data(final_env,
                                      id_vars = env_save_vars,
                                      outcome_var = "deaths",
                                      metrics = c("mean", "lower", "upper"),
                                      na.rm = T)
final_env[, estimate_stage_id := 12]
readr::write_csv(final_env, paste0(master_dir, "/summary/intermediary/env_", current_year, ".csv"))

## Save draw-level envelope -- can take 45 minutes to an hour
setkey(with_hiv_env, location_id)
env_save_locs <- unique(with_hiv_env[, location_id])

save_lt_env(with_hiv_env[, .SD, .SDcols = c(env_id_vars, "deaths")],
            locations = env_save_locs,
            file_prefix = "env_with_hiv/scaled/with_hiv_env_")



