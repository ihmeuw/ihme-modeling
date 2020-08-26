###############################################################################################################
## Create initial MLT HIV-free and with-HIV life-tables and with-HIV envelope results
## Steps:
##        1. Rescale HIV CDR and generate HIV-free 5q0 and 45q15 estimates
##        2. Create standard lifetable (age-specific qx) by matching empirical lifetables with target 5q0 and 45q15
##              Non-ZAF target: HIV-free 5q0 and 45q15 (from step 1)
##              ZAF target: with-HIV 5q0 and 45q15 (straight from processes)
##        3. Smooth standard lifetables and adjust age-specific qx based on remaining difference between standard and target
##        4. Select optimal scalars to scale age-specific values proportionally, based on iterations over scalar values
##        5. Generate full lifetable off of post-stage 1 iterated qx results, including ax generation
##        6. Run stage 2 iterations to find optimal scalars to adjust mx to target 5q0 and 45q15 values
##              Non-ZAF target: with-HIV 5q0 and 45q15
##              ZAF target: HIV-free 5q0 and 45q15
##        7. Generate full stage 2 lifetable based off of mx, ax, and qx values
##        8. Generate with-HIV envelope based off of with-HIV lifetable, under-5 envelope, and other parameters

## Major outputs:
##        1. With-HIV envelope
##        2. With-HIV life table
##        3. HIV-free life table
##        4. Matched empirical lifetables (which ones were used to generate standard lifetable, and associated weights)

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
  library(argparse)
  library(mortdb, lib= "FILEPATH/r-pkg")
  library(mortcore, lib= "FILEPATH/r-pkg")
  source(paste0("FILEPATH/source_funcs.R"))

## Parse arguments
  parser <- ArgumentParser()
  parser$add_argument('--version_id', type="integer", required=TRUE,
                      help='The version_id for this run')
  parser$add_argument('--country', type="character", required=TRUE,
                      help='Country to run')
  parser$add_argument('--spectrum_name', type="character", required=TRUE,
                      help='Spectrum name of HIV inputs')
  parser$add_argument('--estimate_45q15_version', type="integer", required=TRUE,
                      help='45q15 version id')
  parser$add_argument('--estimate_5q0_version', type="integer", required=TRUE,
                      help='5q0 version id')
  parser$add_argument('--age_sex_estimate_version', type="integer", required=TRUE,
                      help='Age sex version id')
  parser$add_argument('--u5_envelope_version', type="integer", required=TRUE,
                      help='Under-5 envelope version id')
  parser$add_argument('--lt_empirical_data_version', type="integer", required=TRUE,
                      help='Life table empirical data version id')
  parser$add_argument('--gbd_year', type="integer", required=TRUE,
                      help='GBD Year')

  args <- parser$parse_args()
  print(args)
  lt_run_id <- args$version_id
  current_ihme <- args$country
  spectrum_name <- args$spectrum_name
  vers_45q15 <- args$estimate_45q15_version
  vers_5q0 <- args$estimate_5q0_version
  vers_age_sex <- args$age_sex_estimate_version
  vers_u5_env <- args$u5_envelope_version
  vers_lt_empir <- args$lt_empirical_data_version
  gbd_year <- args$gbd_year

## Set primary working directory
  data_dir <- "FILEPATH"
  input_dir <- "FILEPATH"
  out_dir <- "FILEPATH"

## Set input directories for all versioned inputs
  input_45q15_dir <- "FILEPATH"
  input_5q0_dir <- "FILEPATH"
  input_age_sex_dir <- "FILEPATH"
  input_u5_env_dir <- "FILEPATH"
  input_elt_dir <- "FILEPATH"

## Set general ID variables
  master_ids <- c("sim", "ihme_loc_id", "sex", "year", "age")
  master_ids_noage <- master_ids[master_ids != "age"]


##############################################################################################################
## All input files
  loc_map         <- fread(paste0(input_dir,"/lt_env_locations.csv"))
  age_map         <- fread(paste0(input_dir,"/age_map.csv"))
  child_hiv       <- fread(paste0(input_5q0_dir,"/data/hiv_covariate.csv"))
  mean_adult_hiv  <- fread(paste0(input_45q15_dir, "/data/hiv_covariate.csv"))
  mlt_db_map      <- fread(paste0(input_dir, "/mlt_db_categories.csv"))
  master_lts      <- fread(paste0(input_dir, "/master_lt_empirical.csv"))
  params_mx_95_no_hmd   <- data.table(read_dta(paste0(input_dir, "/par_age_95plus_mx.dta")))
  hmd_countries         <- data.table(read_dta(paste0(input_dir, "/hmdcountries.dta")))
  hiv_death_prop_range  <- fread(paste0(input_dir,"/hiv_deaths_prop_range_update.csv"))
  lt_match_map    <- fread(paste0(input_dir, "/lt_match_map.csv"))
  lt_weights      <- fread(paste0(input_dir, "/weights_all_levels.csv"))
  pops            <- fread(paste0(input_dir, "/population.csv"))
  modelpar_sims   <- fread(paste0(input_dir,"/modelpar_sim_", lt_run_id, ".csv"))
  params_qx_85    <- data.table(read_dta(paste0(input_dir,"/par_age_85plus_qx_alter.dta")))
  ax_scale        <- fread(paste0(input_elt_dir, "/ax_scaling.csv"))

  ## Country/sex -specific HIV Crude Death Rate Scalars
  ## Derived from graphs of approximated 45q15 and 5q0 values based on different scalar values
  ## Columns: ihme_loc_id, sex, ctry_adult_scalar, ctry_child_scalar
  hiv_country_cdr_scalars <- fread(paste0(input_dir, "/hiv_cdr_scalars.csv"))

  ## 110-plus mx values (parameter to multiply by ln 5m105 to get mx at age 110)
  params_mx_110 <- data.table(read_dta(paste0(input_dir,"/par_age_110plus_mx.dta")))

  ## Ax parameters (Constant and betas to generate ax based on qx and qx^2 values)
  params_ax_80 <- data.table(read_dta(paste0(input_dir, "/ax_par.dta")))

###############################################################################################################
## Set options for processing files and iterations based on location
if(!grepl("ZAF", current_ihme)) {
  stage_1_iter_option <- "hiv_free"
  stage_1_target <- "hiv_free_qx"
  stage_1_names <- c("calc_5q0", "calc_45q15")

  stage_2_iter_option <- "with_hiv"
  stage_2_target <- "entry_sims"
  stage_2_names <- c("entry_5q0", "entry_45q15")

  hiv_free_lt_name <- "stage_1_life_table"
  whiv_lt_name <- "stage_2_life_table"
} else {
  stage_1_iter_option <- "with_hiv_ZAF"
  stage_1_target <- "entry_sims"
  stage_1_names <- c("entry_5q0", "entry_45q15")

  stage_2_iter_option <- "hiv_free_ZAF"
  stage_2_target <- "hiv_free_qx"
  stage_2_names <- c("calc_5q0", "calc_45q15")

  hiv_free_lt_name <- "stage_2_life_table"
  whiv_lt_name <- "stage_1_life_table"
}

###############################################################################################################
## Import map files, assign ID values
## Location file
  current_location_id <- loc_map[ihme_loc_id == current_ihme, location_id]
  loc_map[, parent_ihme := substr(ihme_loc_id, 1, 3)]
  locations <- unique(loc_map[, ihme_loc_id])
  current_region <- loc_map[location_id == current_location_id, region_id]

## Create ID combinations to use with assert_ids
  sexes <- c("male", "female")
  years <- c(1950:gbd_year)
  sims <- c(0:999)
  ages <- unique(age_map$age_group_years_start)

## Child and adult sims
  sim_ids <- list(ihme_loc_id = current_ihme,
                  year = c(1950:gbd_year),
                  sex = c("male", "female"),
                  simulation = c(0:999))

  lt_sim_ids <- list(ihme_loc_id = current_ihme,
                     year = c(1950:gbd_year),
                     sex = c("male", "female"),
                     sim = c(0:999),
                     age = c(0, 1, seq(5, 110, 5)))


###############################################################################################################
## Import inputs from external processes

## 5q0 and 45q15 sims, Adult HIV CDR
  import_entry_sims <- function(location_id, current_ihme, sim_ids) {
    ## Import data
    child_sims <- fread(paste0(input_age_sex_dir, "/draws/", location_id, ".csv"),
                  select = c("ihme_loc_id", "year", "sex", "simulation", "q_u5"))
    child_sims <- child_sims[sex != "both", list(ihme_loc_id, year, sex, simulation, q_u5)]
    child_sims[, year := floor(year)]
    setnames(child_sims, "q_u5", "noshock_q5")

    sexes <- c("male", "female")
    files <- paste0("gpr_", current_ihme, "_", sexes, "_sim.csv")
    adult_sims <- import_files(filenames=files, folder= paste0(input_45q15_dir, "/draws"), FUN=fread,
                              multicore=T, mc.cores=2)
    adult_sims[, year := floor(year)]
    setnames(adult_sims, c("sim", "mort"), c("simulation", "v45q15"))
    adult_sims <- adult_sims[, list(ihme_loc_id, year, sex, simulation, v45q15)]

    ## Run assertions on datasets
    assert_colnames(adult_sims, c("ihme_loc_id", "year", "sex", "simulation", "v45q15"))
    assert_colnames(child_sims, c("ihme_loc_id", "year", "sex", "simulation", "noshock_q5"))

    assert_ids(adult_sims, sim_ids)
    assert_ids(child_sims, sim_ids)

    assert_values(adult_sims, colnames(adult_sims), "not_na")
    assert_values(adult_sims, c("v45q15"), "gte", 0)
    assert_values(child_sims, colnames(child_sims), "not_na")
    assert_values(child_sims, "noshock_q5", "gte", 0)

    ## Output these failures to flat files for diagnostics
    adult_failures <- assert_values(adult_sims, c("v45q15"), "lte", 1, warn_only=T)
    child_failures <- assert_values(child_sims, "noshock_q5", "lte", 1, warn_only=T)

    if(!is.null(adult_failures)) write_csv(adult_failures, paste0(data_dir, "/diagnostics/01_45q15_over_1_", current_ihme, ".csv"))
    if(!is.null(child_failures)) write_csv(child_failures, paste0(data_dir, "/diagnostics/01_5q0_over_1_", current_ihme, ".csv"))

    compiled_sims <- merge(adult_sims, child_sims, by=c("ihme_loc_id", "year", "sex", "simulation"))
    setnames(compiled_sims, c("simulation", "v45q15", "noshock_q5"), c("sim", "entry_45q15", "entry_5q0"))
    return(compiled_sims)
  }

  entry_sims <- import_entry_sims(current_location_id, current_ihme, sim_ids)

## Child HIV Crude Death Rate
  child_hiv <- unique(child_hiv[ihme_loc_id == current_ihme, list(ihme_loc_id, year, child_hiv_cdr)])
  child_hiv[, year := floor(year)]

  assert_ids(child_hiv, list(ihme_loc_id = current_ihme, year = years))
  assert_values(child_hiv, colnames(child_hiv), "not_na")

## Adult mean HIV Crude Death Rate
  mean_adult_hiv <- mean_adult_hiv[ihme_loc_id == current_ihme, list(ihme_loc_id, sex, year, adult_hiv_cdr)]
  mean_adult_hiv[, year := floor(year)]

  assert_ids(mean_adult_hiv, list(ihme_loc_id = current_ihme,
                           sex = c("male", "female"),
                           year = years))
  assert_values(mean_adult_hiv, colnames(mean_adult_hiv), "not_na")

## Combine Adult and Child HIV draws
  hiv_draws <- merge(mean_adult_hiv, child_hiv, by = c("ihme_loc_id", "year"))
  rm(mean_adult_hiv, child_hiv)


###############################################################################################################
## Import LT Database
  lt_option <- mlt_db_map[location_id == current_location_id, loc_indic]
  empir_lts <- select_lt_empirical(master_lts = master_lts, lt_type = lt_option, hiv_cdr = hiv_draws, location = current_ihme, loc_map = loc_map)

## Generate empirical qx values long by age
  empir_lts[, lx0 := as.double(lx0)]

## Population
  pops <- pops[ihme_loc_id == current_ihme & sex != "both"]
  pop_ages <- c(0, 1, seq(5, 95, 5))

  assert_values(pops, colnames(pops), "not_na")
  assert_ids(pops, list(sex = sexes,
                        year = years,
                        ihme_loc_id = current_ihme,
                        age = pop_ages))

## Under-5 envelope results
  u5_env <- fread(paste0(input_u5_env_dir, "/draws/", current_location_id, ".csv"))
  death_cols <- paste0("deaths", c("enn", "lnn", "pnn", 1:4))
  u5_env <- u5_env[, .SD, .SDcols = c("ihme_loc_id", "year", "sim", "sex", death_cols)]
  u5_env <- melt(u5_env,
                  id.vars = c("ihme_loc_id", "year", "sim", "sex"),
                  measure.vars = death_cols)
  setnames(u5_env, c("variable", "value"), c("age", "deaths"))
  u5_env[, age := gsub("deaths", "", as.character(age))]

  assert_values(u5_env, "deaths", "not_na")
  assert_ids(u5_env, list(sex = sexes,
                          year = years,
                          age = c("enn", "lnn", "pnn", 1:4),
                          ihme_loc_id = current_ihme,
                          sim = 0:999))


###############################################################################################################
## Import LT-specific inputs

## Country/sex -specific HIV Crude Death Rate Scalars
## Derived from graphs of approximated 45q15 and 5q0 values based on different scalar values
## Columns: ihme_loc_id, sex, ctry_adult_scalar, ctry_child_scalar
  hiv_country_cdr_scalars <- fread(paste0(input_dir, "/hiv_cdr_scalars.csv"))
  assert_ids(hiv_country_cdr_scalars, list(ihme_loc_id = locations, sex = sexes))

## HIV Deaths Prop Range
## This represents an upper bound of HIV-to-all-cause deaths that HIV should encompass
  setnames(hiv_death_prop_range, c("age_group", "upper"), c("age_group_name", "upper_prop"))
  hiv_death_prop_range[sex_id == 1, sex := "male"]
  hiv_death_prop_range[sex_id == 2, sex := "female"]
  hiv_death_prop_range <- hiv_death_prop_range[ihme_loc_id == current_ihme, list(ihme_loc_id, age_group_name, sex, upper_prop)]

  assert_values(hiv_death_prop_range, colnames(hiv_death_prop_range), "not_na")
  assert_values(hiv_death_prop_range, "upper_prop", "gte", 0)
  assert_values(hiv_death_prop_range, "upper_prop", "lte", 1)
  assert_ids(hiv_death_prop_range, list(ihme_loc_id = current_ihme,
                                        age_group_name = unique(hiv_death_prop_range$age_group_name),
                                        sex = sexes))

  hiv_death_prop_range <- data.table(dcast(hiv_death_prop_range, ihme_loc_id+sex ~ age_group_name, value.var = "upper_prop"))
  setnames(hiv_death_prop_range, c("15-59", "Under 5"), c("upper_adult", "upper_child"))

## Country HIV Groups
  hiv_ctry_groups <- loc_map[ihme_loc_id == current_ihme, list(ihme_loc_id, group)]

## No-HIV Country List
## This list contains all countries for which we assume 0 HIV
## This is all non-1A or 1B countries
## This is because 2A countries have VR including HIV that we trust, and want to fit directly towards
## And HIV-free gets regenerated in the Reckoning via direct subtraction using CIBA or ST-GPR results
  hiv_ctry_exceptions <- hiv_ctry_groups[grepl("2", group) & ihme_loc_id != "THA", ]
  hiv_ctry_exceptions[, exception := 1]

## Sex-specific HIV Ratio (scalars)
  hiv_sex_scalars <- data.table(sex = c("male","female"),
                                adult_scalar = c(1,1),
                                child_scalar = c(1,1))

## LT Match map
## Determines the number of closest empirical life tables to keep for weighted collapsing
  assert_colnames(lt_match_map, c("ihme_loc_id", "match"))
  assert_values(lt_match_map, colnames(lt_match_map), "not_na")
  lt_match_map <- lt_match_map[ihme_loc_id == current_ihme,]

## LT Weights
## Weights for all of the life tables
  setnames(lt_weights, "region", "region_cat")
  lt_weights[region_cat == "gbd", region_cat := "region"]

  highest_lag <- gbd_year - 1950
  current_max_lag <- max(abs(lt_weights$lag))
  weight_regions <- unique(lt_weights$region_cat)
  add_weights <- CJ(sex = c("male", "female"),
                    region_cat = weight_regions,
                    lag = c((-1 * highest_lag):(-1 + -1 * current_max_lag), (current_max_lag + 1):highest_lag),
                    weights = 0)
  lt_weights <- rbindlist(list(lt_weights, add_weights), use.names=T)

  assert_ids(lt_weights,
            list(sex = c("male", "female"),
                region_cat = weight_regions,
                lag = (-1 * highest_lag):highest_lag))
  assert_colnames(lt_weights, c("sex", "region_cat", "lag", "weights"))
  assert_values(lt_weights, colnames(lt_weights), "not_na")

## Model Parameter Sims
## Betas used to calculate the logit age-specific qx based on the differences in logit 5q0 and 45q15 between observed and standard life table
  modelpar_sims[sex_id == 1, sex:= "male"]
  modelpar_sims[sex_id == 2, sex:= "female"]
  modelpar_sims <- merge(modelpar_sims, age_map, all.x = TRUE)
  setnames(modelpar_sims, "age_group_years_start", "age")
  modelpar_sims = modelpar_sims[,.SD, .SDcol = c("sex", "age", "sim", "sim_difflogit5", "sim_difflogit45")]
  modelpar_ages <- c(0, 1, seq(5,105,5))

  assert_colnames(modelpar_sims, c("sex", "age", "sim", "sim_difflogit5", "sim_difflogit45"))
  assert_ids(modelpar_sims, list(sex = sexes,
                                 sim = sims,
                                 age = modelpar_ages))
  assert_values(modelpar_sims, colnames(modelpar_sims), "not_na")


## 85-plus qx regression values
  ages_qx <- seq(80, 100, 5)
  ages_ax <- seq(80, 105, 5)

  assert_colnames(params_qx_85, c("sex", "age", "par_vage", "par_logitqx80", "par_85cons"))
  setnames(params_qx_85, c("par_vage", "par_logitqx80", "par_85cons"), c("beta_age", "beta_logitqx80", "constant"))
  assert_ids(params_qx_85, list(age = ages_qx,
                                sex = sexes))
  assert_values(params_qx_85, colnames(params_qx_85), "not_na")

## 110-plus mx values (parameter to multiply by ln 5m105 to get mx at age 110)
  assert_colnames(params_mx_110, c("sex", "parlnmx"))
  assert_ids(params_mx_110, list(sex = sexes))
  assert_values(params_mx_110, colnames(params_mx_110), "not_na")

## Ax parameters (Constant and betas to generate ax based on qx and qx^2 values)
  ages_ax <- seq(80, 105, 5)
  assert_colnames(params_ax_80, c("sex", "age", "par_qx", "par_sqx", "par_con"))
  assert_ids(params_ax_80, list(age = ages_ax,
                                sex = sexes))
  assert_values(params_ax_80, colnames(params_ax_80), "not_na")

## HIV Age-specific Relative Risks (age 40 as reference category)
  import_hiv_rrs <- function(ihme_loc_id, spectrum_name) {
    rr_ages <- c(0, 1, seq(5, 80, 5))

    rr_filepath <- "FILEPATH"
    hiv_rrs <- fread(paste0(rr_filepath, "/locations/", ihme_loc_id, "_hiv_rr.csv"))
    hiv_rrs[age == 40, hivrr := 1]
    hiv_rrs <- hiv_rrs[year <= gbd_year, list(year, sex, sim, age, hivrr)]

    assert_values(hiv_rrs, colnames(hiv_rrs), "not_na", quiet = T)
    assert_ids(hiv_rrs,
               list(sex = sexes,
                    age = rr_ages,
                    sim = sims,
                    year = 1950:gbd_year),
               quiet = T)
    return(hiv_rrs)
  }

  hiv_rrs <- import_hiv_rrs(current_ihme, spectrum_name)
  setnames(hiv_rrs, "hivrr", "hiv_rr")

## 95+ mx values non-HMD country adjustment
  assert_colnames(params_mx_95_no_hmd, c("sex", "parmx"))
  is_current_hmd <- F
  if(current_ihme %in% unique(hmd_countries[hmd == 1, ihme_loc_id])) is_current_hmd <- T

  is_current_hmd <- T

###############################################################################################################
## Rescale HIV CDR Rates
  rescaled_hiv_cdr <- rescale_hiv_cdr(hiv_draws = hiv_draws,
                                      country_scalar = hiv_country_cdr_scalars,
                                      sex_scalar = hiv_sex_scalars,
                                      hiv_ctry_exceptions = hiv_ctry_exceptions,
                                      hiv_ctry_groups = hiv_ctry_groups)


###############################################################################################################
## Subtract HIV crude death rate from with-HIV 45q15 and 5q0 to get HIV-free 45q15 and 5q0
  hiv_free_qx <- subtract_hiv_mx(qx_draws = entry_sims,
                                 hiv_draws = rescaled_hiv_cdr,
                                 hiv_prop_range = hiv_death_prop_range,
                                 hiv_ctry_groups = hiv_ctry_groups)


###############################################################################################################
## Calculate standard lifetable using Mahalanobis distance matching, exclusion criteria, and weighted collapsing
  hiv_free_qx[, logit_calc_45q15 := logit_qx(calc_45q15)]
  hiv_free_qx[, logit_calc_5q0 := logit_qx(calc_5q0)]

  hiv_free_qx <- merge(hiv_free_qx, lt_match_map, by = c("ihme_loc_id"))
  setnames(hiv_free_qx, "match", "n_match")

  setkey(lt_weights, sex, lag, region_cat)

  print(system.time(stan_results <- mclapply(0:999, calc_stan_lt, mc.cores=5,
                                       empir_lt = empir_lts,
                                       weights = lt_weights,
                                       loc_map = loc_map)))

  stan_qx <- rbindlist(lapply(1:1000, function(x) stan_results[[x]]$stan_lt))
  entry_weights <- rbindlist(lapply(1:1000, function(x) stan_results[[x]]$lt_list))
  assert_ids(stan_qx, id_vars = lt_sim_ids)

  ## Get summary metrics from here, and remove original dataset
  summary_weights <- entry_weights[, list(mean_weight = mean(stan_weight),
                                        n_lts = length(stan_weight)),
                                 by = c("ref_ihme", "ref_year", "ihme_loc_id", "year", "sex", "life_table_category_id", "source_type_id")]

  rm(stan_results, entry_weights)
  gc()


###############################################################################################################
## Write out standard lifetable summaries before it's adjusted based on differences
  summary_stan_qx <- stan_qx[, list(mean = mean(stan_qx),
                                    lower = quantile(stan_qx, probs = .025, na.rm = T),
                                    upper = quantile(stan_qx, probs = .975, na.rm = T)),
                             by = c("ihme_loc_id", "year", "sex", "age")]
  write_csv(summary_stan_qx, paste0(out_dir, "/standard_lts/standard_", current_ihme, ".csv"))


###############################################################################################################
## Adjust age-specific standard qx values based on the difference between logit standard and input target 5q0 and 45q15
## HIV-free if non-ZAF, with-HIV if ZAF
  stage_1_target_dt <- copy(get(stage_1_target))
  setnames(stage_1_target_dt, stage_1_names, c("target_5q0", "target_45q15"))
  stan_qx <- adjust_stan_qx(stan_qx, stage_1_target_dt, modelpar_sims, id_vars = master_ids[master_ids != "age"])


###############################################################################################################
## Run iterations to adjust age-specific Standard LT qx to match with 5q0 and 45q15 of target qx file
## For all except for ZAF, this is comparing to HIV-free qx (calculated from entry sims)
## For ZAF, this is comparing to with-HIV entry qx.
  # 5q0
    stage_1_iterated_qx_5q0 <- qx_iterations(current_lt = stan_qx,
                                              target_qx = stage_1_target_dt,
                                              hiv_type = stage_1_iter_option,
                                              scalar_set_max = 0.6,
                                              scalar_set_min = 0.0001,
                                              num_iterations = 30,
                                              num_scalars = 10,
                                              by_vars = c("ihme_loc_id", "sex", "year", "sim"),
                                              age_group = "5q0")

  # 45q15
    stage_1_iterated_qx_45q15 <- qx_iterations(current_lt = stan_qx,
                                                target_qx = stage_1_target_dt,
                                                hiv_type = stage_1_iter_option,
                                                scalar_set_max = 0.99999,
                                                scalar_set_min = 0.0001,
                                                num_iterations = 30,
                                                num_scalars = 20,
                                                by_vars = c("ihme_loc_id", "sex", "year", "sim"),
                                                age_group = "45q15")

  # Combine
  stage_1_iterated_qx <- rbindlist(list(stage_1_iterated_qx_5q0, stage_1_iterated_qx_45q15), use.names=T)
  stage_1_iterated_qx[age == 110, qx := 1]
  stage_1_iterated_qx[, c("target_45q15", "target_5q0", "best_scalar") := NULL]


###############################################################################################################
## Create life table based on stage-1 results
  stage_1_life_table <- gen_stage_1_life_table(iter_qx = stage_1_iterated_qx[, .SD, .SDcols = c(master_ids, "qx")],
                                mx_110 = params_mx_110,
                                betas_qx_85 = params_qx_85,
                                ax_params = params_ax_80,
                                ax_scale = ax_scale)

  rm(stage_1_iterated_qx_5q0, stage_1_iterated_qx_45q15, stage_1_iterated_qx, stan_qx, stage_1_target_dt)
  gc()


###############################################################################################################
## Run stage 2 iterations
## For non-ZAF countries, adjust HIV-free mx to match target entry with-HIV 5q0 and 45q15
## For ZAF countries, adjust with-HIV mx to match target calculated (based on entry) HIV-free 5q0 and 45q15
  agg_qx <- function(dt, start_age, end_age, id_vars) {
    no_age_ids <- id_vars[id_vars != "age"]
    dt <- dt[age >= start_age & age < end_age, .SD, .SDcols = c(id_vars, "qx")]
    dt[, px := 1 - qx]
    dt <- dt[, list(qx = 1 - prod(px)), by = no_age_ids]
    return(dt)
  }

  stage_1_5q0 <- agg_qx(stage_1_life_table, 0, 5, master_ids)
  setnames(stage_1_5q0, "qx", "stage_1_5q0")
  stage_1_45q15 <- agg_qx(stage_1_life_table, 15, 60, master_ids)
  setnames(stage_1_45q15, "qx", "stage_1_45q15")

  stage_1_output <- merge(stage_1_life_table[, .SD, .SDcols = c(master_ids, "mx", "ax")], stage_1_5q0, by = master_ids_noage)
  stage_1_output <- merge(stage_1_output, stage_1_45q15, by = master_ids_noage)

  stage_1_output <- merge(stage_1_output, hiv_rrs, by = master_ids[master_ids != "ihme_loc_id"], all.x=T)
  stage_2_target_dt <- get(stage_2_target)[, .SD, .SDcols = c(master_ids_noage, stage_2_names)]

  stage_2_iterated_5q0 <- qx_iterations(current_lt = stage_1_output,
                                        target_qx = stage_2_target_dt,
                                        hiv_type = stage_2_iter_option,
                                        scalar_set_max = 100,
                                        scalar_set_min = 0.002,
                                        num_iterations = 50,
                                        num_scalars = 10,
                                        by_vars = c("ihme_loc_id", "sex", "year", "sim"),
                                        age_group = "5q0")

  stage_2_iterated_45q15 <- qx_iterations(current_lt = stage_1_output,
                                          target_qx = stage_2_target_dt,
                                          hiv_type = stage_2_iter_option,
                                          scalar_set_max = 50,
                                          scalar_set_min = 0.002,
                                          num_iterations = 50,
                                          num_scalars = 100,
                                          by_vars = c("ihme_loc_id", "sex", "year", "sim"),
                                          age_group = "45q15")

  stage_2_iterated_mx_qx <- rbindlist(list(stage_2_iterated_5q0, stage_2_iterated_45q15), use.names = T)
  assert_values(stage_2_iterated_mx_qx, "mx", "gte", 0, quiet = T)
  stage_2_iterated_mx_qx[, c("best_scalar", "target_45q15", "target_5q0") := NULL]


###############################################################################################################
## Generate stage 2 lifetable based off of mx, ax, and qx values
  gen_age_length(stage_2_iterated_mx_qx)
  stage_2_iterated_mx_qx[, qx := mx_ax_to_qx(mx, ax, age_length)]

  stage_2_life_table <- gen_stage_2_life_table(iter_mx_qx = stage_2_iterated_mx_qx,
                                               id_vars = master_ids)


###############################################################################################################
## Rename stage 1 and stage 2 outputs to the appropriate tables
## For non-ZAF, HIV-free LT comes from stage 1 iterations and with-HIV comes from stage 2
## For ZAF, HIV-free LT comes from stage 2 iterations and with-HIV comes from stage 1
  hiv_free_lt <- get(hiv_free_lt_name)
  whiv_lt <- get(whiv_lt_name)

  rm(list = c(hiv_free_lt_name, whiv_lt_name))
  gc()


###############################################################################################################
## Generate with-HIV envelope using with-HIV lifetable and under-5 envelope results
  id_vars <- c("sim", "ihme_loc_id", "year", "sex", "age")

  env_output_ids <- list(ihme_loc_id = current_ihme,
                         year = c(1950:gbd_year),
                         sex = c("male", "female"),
                         sim = c(0:999),
                         age = c("enn", "lnn", "pnn",
                                 as.character(c(1, seq(5, 95, 5)))))

  whiv_env <- gen_whiv_env(whiv_lt = whiv_lt,
                           population = pops,
                           u5_envelope = u5_env,
                           mx_params = params_mx_95_no_hmd,
                           hmd_indic = is_current_hmd,
                           env_ids = env_output_ids)


###############################################################################################################
## Write out HIV-Free and With-HIV life tables, With-HIV envelope, and Standard Life Tables

## Draw-level life tables and envelopes in HDF files indexed by year
  save_years <- c(min(hiv_free_lt[, year]):gbd_year)
  save_lt_cols <- c("ihme_loc_id", "year", "sex", "age", "sim", "mx", "ax", "qx")

  save_lt_env <- function(data, file_prefix) {
    filepath <- paste0(out_dir, "/", file_prefix, current_ihme, ".h5")
    file.remove(filepath)
    h5createFile(filepath)
    lapply(save_years, save_hdf,
                  data = data,
                  filepath = filepath,
                  by_var = "year",
                  level = 2)
    H5close()
  }

  save_lt_env(hiv_free_lt[, .SD, .SDcols = save_lt_cols],
              file_prefix = "lt_hiv_free/pre_scaled/hiv_free_lt_")
  save_lt_env(whiv_lt[, .SD, .SDcols = save_lt_cols],
              file_prefix = "lt_with_hiv/pre_scaled/with_hiv_lt_")
  save_lt_env(whiv_env, file_prefix = "env_with_hiv/pre_scaled/with_hiv_env_")

## Empirical DB weights as a csv (standard is saved earlier)
  write_csv(summary_weights, paste0(out_dir, "/standard_lts/", "weights_", current_ihme, ".csv"))

## Summary-level csv files
  save_summary_lt_env <- function(data, data_type, file_prefix) {
    id_vars <- c("ihme_loc_id", "year", "sex", "age")
    setkeyv(data, id_vars)
    if(data_type == "envelope") summary <- data[, list(mean = mean(deaths),
                                                       lower = quantile(deaths, probs=.025, na.rm=T),
                                                       upper = quantile(deaths, probs = .975, na.rm=T)),
                                                  by = id_vars]

    if(data_type == "life_table") summary <- data[, list(mean_mx = mean(mx),
                                                         lower_mx = quantile(mx, probs=.025, na.rm=T),
                                                         upper_mx = quantile(mx, probs = .975, na.rm=T),
                                                         mean_ax = mean(ax),
                                                         lower_ax = quantile(ax, probs=.025, na.rm=T),
                                                         upper_ax = quantile(ax, probs = .975, na.rm=T),
                                                         mean_qx = mean(qx),
                                                         lower_qx = quantile(qx, probs=.025, na.rm=T),
                                                         upper_qx = quantile(qx, probs = .975, na.rm=T)),
                                                    by = id_vars]

    write_csv(summary, paste0(out_dir, "/", file_prefix, current_ihme, ".csv"))
  }

  save_summary_lt_env(hiv_free_lt, "life_table", file_prefix = "lt_hiv_free/pre_scaled/sum_hiv_free_lt_")
  save_summary_lt_env(whiv_lt, "life_table", file_prefix = "lt_with_hiv/pre_scaled/sum_with_hiv_lt_")
  save_summary_lt_env(whiv_env, "envelope", file_prefix = "env_with_hiv/pre_scaled/sum_with_hiv_env_")


