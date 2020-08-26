###############################################################################################################
## Aggregate all life tables and envelope from lowest level to aggregated location/age/sex

###############################################################################################################
## Set up settings
  rm(list=ls())

  if (Sys.info()[1]=="Windows") {
    root <- "FILEPATH"
    user <- Sys.getenv("USERNAME")
  } else {
    root <- "FILEPATH"
    user <- Sys.getenv("USER")
  }

  library(rhdf5)
  library(parallel)
  library(data.table)
  library(assertable)
  library(haven)
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

  args <- parser$parse_args()
  shocks_addition_run_id <- args$shock_death_number_estimate_version
  current_year <- args$year
  gbd_year <- args$gbd_year

  master_ids <- c("location_id", "year_id", "sex_id", "age_group_id")
  draw_ids <- c(master_ids, "draw")

  ## Generate summarization and collapsing functions
  lower <- function(x) quantile(x, probs=.025, na.rm=T)
  upper <- function(x) quantile(x, probs=.975, na.rm=T)

###############################################################################################################
## Import map files, assign ID values
## Location file
  location_map <- fread(FILEPATH)
  run_countries <- unique(location_map$ihme_loc_id)

  set.seed(current_year)
  import_countries <- sample(run_countries, length(run_countries), replace = FALSE)
  if(!identical(sort(run_countries), sort(import_countries))) stop("Country resample failed")

  hiv_adjust_filenames <- FILEPATH
  hiv_adjust <- assertable::import_files(hiv_adjust_filenames,
                                    folder = lowest_dir,
                                    FUN = load_hdf,
                                    by_val = current_year,
                                    multicore = T,
                                    mc.cores = 2)
  setkeyv(hiv_adjust, draw_ids)

  hiv_adj_value_vars <- c("input_lt_hiv", "input_with_hiv_env", "input_spec_hiv", "output_no_hiv_env", "output_with_hiv_env", "output_hiv_specific")
  hiv_adjust <- hiv_adjust[, .SD, .SDcols = c(draw_ids, hiv_adj_value_vars)]

  hiv_adjust <- agg_results(hiv_adjust, value_vars = hiv_adj_value_vars,
                            id_vars = draw_ids,
                            agg_sex = T,
                            end_agg_level = 3,
                            tree_only= "CHN",
                            loc_scalars = F,
                            agg_hierarchy = T,
                            gbd_year = gbd_year)


  hiv_adjust <- melt(hiv_adjust, id.vars = draw_ids, variable.name = "hiv_measure", value.name = "deaths")
  hiv_adjust[hiv_measure == "input_lt_hiv", hiv_measure_type_id := 4]
  hiv_adjust[hiv_measure == "input_with_hiv_env", hiv_measure_type_id := 2]
  hiv_adjust[hiv_measure == "input_spec_hiv", hiv_measure_type_id := 5]
  hiv_adjust[hiv_measure == "output_hiv_specific", hiv_measure_type_id := 3]
  hiv_adjust[hiv_measure == "output_with_hiv_env", hiv_measure_type_id := 1]
  hiv_adjust <- hiv_adjust[!is.na(hiv_measure_type_id)]
  hiv_adjust[, hiv_measure := NULL]

  filepath <- FILEPATH
  if(file.exists(filepath)) file.remove(filepath)
  rhdf5::h5createFile(filepath)
  invisible(lapply(unique(hiv_adjust$location_id), save_hdf,
                   data = hiv_adjust,
                   filepath = filepath,
                   by_var = "location_id", level = 2))
  rhdf5::H5close()

  hiv_adjust_summary <- hiv_adjust[, list(mean = mean(deaths, na.rm = T),
                                          lower = lower(deaths),
                                          upper = upper(deaths)),
                                    by = c(master_ids, "hiv_measure_type_id")]
  write_csv(hiv_adjust_summary, FILEPATH)
