## AUTHOR
## Model Life Table set-up

## (1) Create file structure
## (2) Save inputs
## (3) MLT parameter estimation

###############################################################################################################

## Set up settings
  rm(list=ls())
  user <- Sys.getenv("USER")

## Source libraries and functions
  library(pacman)
  pacman::p_load(readr, RMySQL, data.table, assertable, haven, argparse)
  library(mortdb, lib = "FILEPATH")
  library(mortcore, lib = "FILEPATH")
  library(ltcore, lib = "FILEPATH")

###############################################################################################################
## Run options
  parser <- ArgumentParser()
  parser$add_argument('--version_id', type="integer", required=TRUE,
                      help='mlt life table estimate run')
  parser$add_argument('--mlt_envelope_version', type="integer", required=TRUE,
                      help='MLT envelope version')
  parser$add_argument('--lt_empirical_data_version', type="integer", required=TRUE,
                      help='Life table empirical data version id')
  parser$add_argument('--map_estimate_version', type="integer", required=TRUE,
                    help='Life table map estimate version')
  parser$add_argument('--spectrum_name', type="character", required=TRUE,
                      help='Spectrum version')
  parser$add_argument('--start', type="character", required=TRUE,
                      help='Starting step of run')
  parser$add_argument('--end', type="character", required=TRUE,
                      help='Ending step of run')
  parser$add_argument('--file_del', type="character", required=TRUE,
                      help='Delete files')
  parser$add_argument('--gbd_year', type="integer", required=TRUE,
                      help='GBD Year')
  parser$add_argument('--end_year', type="integer", required=TRUE,
                      help='End Year')
  parser$add_argument('--code_dir', type="character", required=TRUE,
                      help='Directory where MLT code is cloned')

  args <- parser$parse_args()
  mlt_lt_run_id <- args$version_id
  vers_lt_empir <- args$lt_empirical_data_version
  mlt_env_run_id <- args$mlt_envelope_version
  map_est_run_id <- args$map_estimate_version
  spectrum_name <- args$spectrum_name
  start <- args$start
  end <- args$end
  file_del <- as.logical(args$file_del)
  gbd_year <- args$gbd_year
  end_year <- args$end_year
  code_dir <- args$code_dir

  sexes <- c("male", "female")
  run_years <- c(1950:end_year)

  # source process-specific functions
  source(paste0(code_dir, "/mltgeneration/R/download_lt_empirical.R"))
  source(paste0(code_dir, "/mltgeneration/R/gen_hiv_scalars.R"))
  source(paste0(code_dir, "/parameter_funcs/gen_modelpar_sims_difflogit.R"))

###############################################################################################################
## Create Directory structure

  out_dir         <- paste0("FILEPATH", mlt_lt_run_id)
  diag_dir        <- paste0(out_dir, "/diagnostics")
  input_dir       <- paste0(out_dir, "/inputs")
  out_dir_lt_free <- paste0(out_dir, "/lt_hiv_free")
  out_dir_lt_whiv <- paste0(out_dir, "/lt_with_hiv")
  out_dir_env     <- paste0(out_dir, "/env_with_hiv")
  out_dir_summary <- paste0(out_dir, "/summary")
  out_dir_stan    <- paste0(out_dir, "/standard_lts")

## Change permissions to file structure
  system(paste0("chmod -R 777 ", out_dir))

###############################################################################################################
## Save parent versions

  parent_version_dt <- get_proc_lineage("mlt life table", "estimate", run_id = mlt_lt_run_id)
  parent_versions <- list()

  for(proc in c("age sex estimate", "45q15 estimate", "5q0 estimate", "life table empirical data", "population estimate", "u5 envelope estimate")) {
    parent_versions[[proc]] <- parent_version_dt[parent_process_name == proc & exclude == 0, parent_run_id]
  }
  
  # remove `_hiv_rr` from spectrum name in case it's included
  # gets added back into the file path later in this script
  parent_versions[["hiv spectrum estimate"]] <- gsub("_hiv_rr", "", spectrum_name)
  
  versions <- rbindlist(lapply(c("age sex estimate", "45q15 estimate", "5q0 estimate",
                                 "life table empirical data", "population estimate", "u5 envelope estimate"),
                               function(p) { data.table(process=p, version=parent_versions[[p]]) }), use.names=T)
  versions <- rbindlist(list(versions, data.table(process=c("mlt_lt_run_id", "mlt_env_run_id", "map_est_run_id"),
                                                  version=c(mlt_lt_run_id, mlt_env_run_id, map_est_run_id))), use.names=T)

  write_csv(versions, paste0(input_dir, "/versions.csv"))

###############################################################################################################
## Import and save maps and other metadata

## location list, see run all for creation
  locations     <- fread(paste0(input_dir, "/lt_env_locations.csv")) 
  run_countries <- locations[is_estimate == 1, ihme_loc_id]

## Find and save an age map (to save parallel jobs from querying the db)
  age_map <- get_age_map(type = "lifetable", gbd_year = gbd_year)
  write_csv(age_map, paste0(input_dir,"/age_map.csv"))

## Save mlt db category map, enforcing all countries except for ZAF to assume location-specific behavior
## For ZAF, we'll use 
  mlt_db_categories <- mortdb::get_locations(gbd_year = gbd_year)[, c("location_id", "ihme_loc_id")]
  mlt_db_categories[, loc_indic := "location-specific"]
  mlt_db_categories[ihme_loc_id %like% "ZAF", loc_indic := "ZAF"]
  write_csv(mlt_db_categories, paste0(input_dir, "/mlt_db_categories.csv"))

## Save country/sex-specific HIV CDR scalars
  group_1_scalars <- gen_hiv_scalars(run_id_45q15_est = parent_versions[["45q15 estimate"]])
  group_1_scalars <- group_1_scalars[ihme_loc_id %in% run_countries]
  hiv_cdr_scalars <- CJ(ihme_loc_id = run_countries[!run_countries %in% unique(group_1_scalars$ihme_loc_id)],
                        sex = c("male", "female"),
                        scalar = 1)
  hiv_cdr_scalars <- rbindlist(list(hiv_cdr_scalars, group_1_scalars), use.names = T)
  hiv_cdr_scalars <- hiv_cdr_scalars[, .(ihme_loc_id, sex, ctry_adult_scalar = scalar, ctry_child_scalar = scalar)]
  setorder(hiv_cdr_scalars, ihme_loc_id, sex)
  write_csv(hiv_cdr_scalars, paste0(input_dir, "/hiv_cdr_scalars.csv"))

## Get population
## Pop age ids is all lifetable ages from 'age_map', except switch to terminal age 95+
  pop_age_ids <- c(age_map[age_group_id <= 32, age_group_id], 235)
  pop_ages <- c(0, 1, seq(5, 95, 5))
  pops <- get_mort_outputs(model_name = "population",
                           model_type = "estimate",
                           run_id = parent_versions[["population estimate"]],
                           age_group_ids = pop_age_ids,
                           sex_ids = c(1:3),
                           year_ids = run_years,
                           location_set_id = 21,
                           gbd_year = gbd_year)

  pops[, c("run_id", "ihme_loc_id", "lower", "upper", "upload_population_estimate_id") := NULL]

  pops <- merge(pops, age_map[age_group_id <= 32, list(age_group_id, age_group_years_start)], by="age_group_id", all.x=T)
  pops[age_group_id == 235, age_group_years_start := 95]
  pops <- merge(pops, locations[, list(ihme_loc_id, location_id)], by="location_id")
  pops[sex_id == 1, sex := "male"]
  pops[sex_id == 2, sex := "female"]
  pops[sex_id == 3, sex := "both"]

  setnames(pops, c("age_group_years_start", "year_id", "mean"), c("age", "year", "population"))
  pops[, c("age_group_id", "sex_id") := NULL]

  assert_values(pops, colnames(pops), "not_na")
  assert_ids(pops, list(sex = c(sexes, "both"),
                        year = run_years,
                        age = pop_ages,
                        ihme_loc_id = unique(locations$ihme_loc_id)))
  write_csv(pops, paste0(input_dir, "/population.csv"))

  ## Import master LTs using download_lt_empirical function from modeling functions
  master_lts <- download_lt_empirical(run_id = parent_versions[["life table empirical data"]], gbd_year = gbd_year)
  write_csv(master_lts, paste0(input_dir, "/master_lt_empirical.csv"))

  ## copies of constant files into run-specific input folder
  constant_dir <- "FILEPATH"
  hiv_dir <- "FILEPATH"

  file.copy(paste0(constant_dir,"/par_age_85plus_qx_alter.dta"), paste0(input_dir,"/par_age_85plus_qx_alter.dta"))
  file.copy(paste0(constant_dir,"/par_age_110plus_mx.dta"), paste0(input_dir,"/par_age_110plus_mx.dta"))
  file.copy(paste0(constant_dir,"/ax_par.dta"), paste0(input_dir,"/ax_par.dta"))
  file.copy(paste0(constant_dir,"/par_age_95plus_mx.dta"), paste0(input_dir,"/par_age_95plus_mx.dta"))
  file.copy(paste0(constant_dir,"/hmdcountries.dta"), paste0(input_dir,"/hmdcountries.dta"))
  file.copy(paste0(constant_dir,"/test_weights_all_levels.csv"), paste0(input_dir, "/weights_all_levels.csv"))
  file.copy(paste0(hiv_dir,"/hiv_deaths_prop_range_update_", spectrum_name,".csv"), paste0(input_dir,"/hiv_deaths_prop_range_update.csv"))

###############################################################################################################
## Remove Existing Files
  if (file_del) {
    if (start<= 1 & end >= 1) {
      print("Deleting 01 output")
      system(paste0("perl -e 'unlink <",out_dir_lt_free,"/pre_scaled/hiv_free_lt_*.h5>' "))
      system(paste0("perl -e 'unlink <",out_dir_lt_free,"/pre_scaled/sum_hiv_free_lt_*.csv>' "))
      system(paste0("perl -e 'unlink <",out_dir_lt_whiv,"/pre_scaled/with_hiv_lt_*.h5>' "))
      system(paste0("perl -e 'unlink <",out_dir_lt_whiv,"/pre_scaled/sum_with_hiv_lt_*.csv>' "))
      print("Deleting envelope")
      system(paste0("perl -e 'unlink <",out_dir_env,"/pre_scaled/with_hiv_env_*.h5>' "))
      system(paste0("perl -e 'unlink <",out_dir_env,"/pre_scaled/sum_with_hiv_env_*.csv>' "))
      print("Deleting extra files")
      system(paste0("perl -e 'unlink <",out_dir_stan,"/standard_*.csv>' "))
      system(paste0("perl -e 'unlink <",out_dir_stan,"/weights_*.csv>' "))
      system(paste0("perl -e 'unlink <",diag_dir,"/final_*.csv>' "))
    }
    if (start<= 2 & end >= 2) {
      print("Deleting 02 output")
      system(paste0("perl -e 'unlink <",out_dir_lt_free,"/scaled/hiv_free_lt_*.h5>' "))
      system(paste0("perl -e 'unlink <",out_dir_lt_whiv,"/scaled/with_hiv_lt_*.h5>' "))
      print("Deleting envelope")
      system(paste0("perl -e 'unlink <",out_dir_env,"/scaled/with_hiv_env_*.h5>' "))
      print("Deleting summary files")
      system(paste0("perl -e 'unlink <",out_dir_summary,"/intermediary/lt_*.csv>' "))
      system(paste0("perl -e 'unlink <",out_dir_summary,"/intermediary/env_*.csv>' "))
    }
  }

##############################################################################################################
## MLT parameter estimation

  modelpar_sims <- gen_modelpar_sim(elt_version = vers_lt_empir, mlt_version = mlt_lt_run_id, gbd_year = gbd_year)
  fwrite(modelpar_sims, paste0(input_dir, "/modelpar_sim_", mlt_lt_run_id, ".csv"))

###############################################################################################################
## Check that required files exist

  if(start <= 1 & end >= 1) {
    check_files("FILEPATH", "/data/hiv_covariate.csv"))
    check_files("FILEPATH", "/data/hiv_covariate.csv"))
    check_files(paste0(run_countries, "_hiv_rr.csv"),
                folder = paste0("FILEPATH",
                                parent_versions[["hiv spectrum estimate"]], "_s1_hiv_rr/locations"))
  }
