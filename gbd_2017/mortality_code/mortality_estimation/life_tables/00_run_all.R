
###############################################################################################################
## Set up settings
  rm(list=ls())

## Source libraries and functions
  library(readr) 
  library(RMySQL)
  library(data.table)
  library(assertable) 
  library(slackr)
  library(haven)
  library(mortdb)
  library(mortcore)
  library(ltcore)
  source(paste0("get_population.R"))

  proj <- "proj_mortenvelope"

  code_dir <- CODE_DIR
  source(paste0(code_dir, "/mltgeneration/R/download_lt_empirical.R"))
  source(paste0(code_dir, "/mltgeneration/R/gen_hiv_scalars.R"))

###############################################################################################################
## Run options
  new_run <- T 
  run_comment <- "comment" 
  spectrum_name <- "spectrum"
  start <- 1
  end <- 3
  launch_reckoning <- T
  
  file_del <- F
  test_submission <- F # Do you want to just print the qsub statement rather than actually submitting the jobs? Recommended to run with file_del = F
  restart <- F # Whether to relaunch some 01 jobs for various cluster errors

  gbd_year <- 2017
  sexes <- c("male", "female")
  run_years <- c(1950:gbd_year)

  if(new_run == T & start != 1) stop(paste0("You are attempting to start a new run without starting at step 1. ",
                                            "This means your code will break since the input files won't exist. ",
                                            "Please consider either starting at 1, or doing a re-run of a previous model"))
  

###############################################################################################################
## Additional run setup: Versioning and flat-file saves (to avoid parallel SQL queries)
  mlt_lt_run_id <- gen_new_version("mlt life table", "estimate", comment= run_comment)
  mlt_env_run_id <- gen_new_version("mlt death number", "estimate", comment = run_comment)
  map_est_run_id <- gen_new_version("life table map", "estimate", comment = run_comment)

  parent_versions <- get_best_versions(c("age sex estimate", "45q15 estimate", "5q0 estimate", "life table empirical data", "population estimate", "u5 envelope estimate"))
  parent_versions[["life table map estimate"]] <- map_est_run_id
  env_parent_versions <- copy(parent_versions)
  env_parent_versions[["mlt life table estimate"]] <- mlt_lt_run_id
 
  map_est_parent_versions <- list()
  map_est_parent_versions[["life table empirical data"]] <- parent_versions[["life table empirical data"]]
 
  parent_versions[["hiv spectrum estimate"]] <- spectrum_name


###############################################################################################################
## Create Directory structure
## Folder structure:
## Parent: /FILEPATH/5_lifetables/v###
## Main children: LT HIV-free and with-HIV, and with-HIV envelope
## Within each: pre_scaled, scaled
## Separate: Summary folder containing all required summary metrics
  out_dir <- FILEPATH
  diag_dir <- paste0(out_dir,"/diagnostics")
  input_dir <- paste0(out_dir,"/inputs")
  out_dir_lt_free <- paste0(out_dir,"/lt_hiv_free")
  out_dir_lt_whiv <- paste0(out_dir,"/lt_with_hiv")
  out_dir_env <- paste0(out_dir,"/env_with_hiv")
  out_dir_summary <- paste0(out_dir,"/summary") 
  out_dir_stan <- paste0(out_dir,"/standard_lts")


###############################################################################################################
## Import and save maps and other metadata
## Save flat locations file (to save parallel jobs from querying the db)
  locations <- data.table(get_locations(level = "estimate", gbd_year = gbd_year, hiv_metadata = T))
  locations <- locations[, list(ihme_loc_id, location_id, region_id, super_region_id, parent_id, local_id, group)]
  
  parents <- data.table(get_locations(level = "all", gbd_year = gbd_year))
  parents <- parents[location_id %in% unique(locations$parent_id), list(location_id, ihme_loc_id)]
  setnames(parents, c("location_id", "ihme_loc_id"), c("parent_id", "parent_ihme"))
  locations <- merge(locations, parents, by = "parent_id", all.x=T)

## Set the countries to run -- order them so that the countries with the most matches (longest-running) are run first to avoid long-tail issues
  lt_match_map <- fread(paste0("match_specs_gbd2017.csv"))
  lt_match_map <- lt_match_map[, list(match = mean(match)), by = "ihme_loc_id"]
  assert_values(lt_match_map, colnames(lt_match_map), "not_na")

  ## Now, set all matches to 100 to allow weights to handle most country prioritization etc.
  lt_match_map[, match := 100]

  lt_match_map <- merge(lt_match_map, locations, by = "ihme_loc_id", all.y = T)
  assert_values(lt_match_map, "match", "not_na")
  setorder(lt_match_map, -match)
  run_countries <- lt_match_map$ihme_loc_id

  write_csv(locations, paste0(input_dir,"/lt_env_locations.csv"))
  write_csv(lt_match_map[, .(ihme_loc_id, match)], paste0(input_dir, "/lt_match_map.csv"))

## Find and save an age map (to save parallel jobs from querying the db)
  age_map <- data.table(get_age_map("lifetable")) ## Does this need others for envelope etc.?
  write_csv(age_map, paste0(input_dir,"/age_map.csv"))

## Save mlt db category map, enforcing all countries except for ZAF to assume location-specific behavior
  mlt_db_categories <- fread(paste0("mlt_db_categories.csv"))
  mlt_db_categories[loc_indic != "ZAF", loc_indic := "location-specific"]
  write_csv(mlt_db_categories, paste0(input_dir, "/mlt_db_categories.csv"))

## Save master lifetable weights
  lt_weights <- data.table(read_dta(paste0("weights_all levels.dta"), encoding = "latin1"))
  
  lt_weights_parent <- lt_weights[region == "country"]
  lt_weights_parent[, region := "parent_subnational"]
  lt_weights_parent[, weights := weights * 1] ## For now, hold the parent_subnational weights constant to avoid swallowing the country-specific

  lt_weights <- rbindlist(list(lt_weights, lt_weights_parent), use.names = T)

  lt_weights[region == "country", weights := weights * 25]

  lt_weights[region == "country" & abs(lag) <= 1, weights := weights * 15]
  lt_weights[region == "country" & abs(lag) >= 2 & abs(lag) <= 4, weights := weights * 3]

  write_csv(lt_weights, paste0(input_dir, "/weights_all_levels.csv"))

## Save country/sex-specific HIV CDR scalars
  group_1_scalars <- gen_hiv_scalars(run_id_45q15_est = parent_versions[["45q15 estimate"]])

  hiv_cdr_scalars <- CJ(ihme_loc_id = run_countries[!run_countries %in% unique(group_1_scalars$ihme_loc_id)],
                        sex = c("male", "female"),
                        scalar = 1)
  hiv_cdr_scalars <- rbindlist(list(hiv_cdr_scalars, group_1_scalars), use.names = T)
  hiv_cdr_scalars <- hiv_cdr_scalars[, .(ihme_loc_id, sex, ctry_adult_scalar = scalar, ctry_child_scalar = scalar)]
  setorder(hiv_cdr_scalars, ihme_loc_id, sex)

  write_csv(hiv_cdr_scalars, paste0(input_dir, "/hiv_cdr_scalars.csv"))

  # file.copy(paste0(root, "/WORK/02_mortality/02_inputs/05_lifetables/gbd", gbd_year, "_hiv_cdr_scalars.csv"),
  #           paste0(input_dir, "/hiv_cdr_scalars.csv"))
  pop_age_ids <- c(age_map[age_group_id <= 32, age_group_id], 235) # Subset to through 95+ age groups
  pop_ages <- c(0, 1, seq(5, 95, 5))
  pops <- get_population(location_id = -1, age_group_id = pop_age_ids, sex_id = c(1:3), year_id = run_years,
                         location_set_id = 21, status = "recent")

  pops[, run_id := NULL]
  
  pops <- merge(pops, age_map[age_group_id <= 32, list(age_group_id, age_group_years_start)], by="age_group_id", all.x=T)
  pops[age_group_id == 235, age_group_years_start := 95]
  pops <- merge(pops, locations[, list(ihme_loc_id, location_id)], by="location_id")
  pops[sex_id == 1, sex := "male"]
  pops[sex_id == 2, sex := "female"]
  pops[sex_id == 3, sex := "both"]
  
  setnames(pops, c("age_group_years_start", "year_id"), c("age", "year"))
  pops[, c("age_group_id", "sex_id") := NULL]
    
  assert_values(pops, colnames(pops), "not_na")
  assert_ids(pops, list(sex = c(sexes, "both"),
                        year = run_years,
                        age = pop_ages,
                        ihme_loc_id = unique(locations$ihme_loc_id)))
  write_csv(pops, paste0(input_dir, "/population.csv"))

  ## Import master LTs using download_lt_empirical function from modeling functions
  master_lts <- download_lt_empirical(run_id = parent_versions[["life table empirical data"]])
  write_csv(master_lts, paste0(input_dir, "/master_lt_empirical.csv"))

###############################################################################################################
## Submit Jobs

## Set code_dir
  setwd(paste0(code_dir,"/.."))

## Launch primary model life table generation code
  if(start <= 1 & end >= 1) {
    run_counter <- 1

    if(restart == T) {
      launch_countries <- assertable::check_files(paste0("hiv_free_lt_", run_countries, ".h5"), folder=paste0(out_dir_lt_free, "/pre_scaled"), continual=F, warn_only = T)
      launch_countries <- gsub("hiv_free_lt_", "", launch_countries)
      launch_countries <- gsub("\\.h5", "", launch_countries)
    } else {
      launch_countries <- run_countries
    }

    for(country in launch_countries) {
      qsub(paste0("gen_mlts_", country), 
           paste0(code_dir, "/01_gen_lts.R"),
           slots = 7, 
           pass = list(mlt_lt_run_id, country,
                       parent_versions[["hiv spectrum estimate"]],
                       parent_versions[["45q15 estimate"]], 
                       parent_versions[["5q0 estimate"]], 
                       parent_versions[["age sex estimate"]],
                       parent_versions[["u5 envelope estimate"]],
                       parent_versions[["life table empirical data"]],
                       gbd_year), 
           proj = proj,
           shell = "r_shell_singularity_3421.sh",
           submit = !test_submission, log=T)
      run_counter <- run_counter + 1
      if(run_counter == 50) {
        print("Sleeping 1 second for scheduler")
        Sys.sleep(1)
        run_counter <- 1
      }
    }
    print("Waiting 30 minutes before checking for 01_lt results")
    Sys.sleep(60*30)

    assertable::check_files(paste0("hiv_free_lt_", run_countries, ".h5"), folder=paste0(out_dir_lt_free, "/pre_scaled"), continual=T, sleep_time = 120, sleep_end = 90, warn_only = T)
    assertable::check_files(paste0("with_hiv_lt_", run_countries, ".h5"), folder=paste0(out_dir_lt_whiv, "/pre_scaled"), continual=T, sleep_end = 15, warn_only = T)

    ## Auto-retry failures of country-specific results if still missing results after 2 hrs and 30 minutes
    launch_countries <- assertable::check_files(paste0("with_hiv_env_", run_countries, ".h5"), folder=paste0(out_dir_env, "/pre_scaled"), continual=T, sleep_end = 15, warn_only = T)
    if(length(launch_countries) > 0) {
      launch_countries <- gsub("with_hiv_env_", "", launch_countries)
      launch_countries <- gsub("\\.h5", "", launch_countries)

      for(country in launch_countries) {
        system(paste0("qdel gen_mlts_", country))
        qsub(paste0("gen_mlts_", country), 
             paste0(code_dir, "/01_gen_lts.R"),
             slots = 7, 
             pass = list(mlt_lt_run_id, country,
                         parent_versions[["hiv spectrum estimate"]],
                         parent_versions[["45q15 estimate"]], 
                         parent_versions[["5q0 estimate"]], 
                         parent_versions[["age sex estimate"]],
                         parent_versions[["u5 envelope estimate"]],
                         parent_versions[["life table empirical data"]],
                         gbd_year), 
             proj = proj,
             shell = "r_shell_singularity_3421.sh",
             submit = !test_submission, log=T)
        run_counter <- run_counter + 1
        if(run_counter == 50) {
          print("Sleeping 1 second for scheduler")
          Sys.sleep(1)
          run_counter <- 1
        }
      }

      Sys.sleep(60*15)

      assertable::check_files(paste0("hiv_free_lt_", run_countries, ".h5"), folder=paste0(out_dir_lt_free, "/pre_scaled"), continual=T, sleep_time = 120, warn_only = T)
      assertable::check_files(paste0("with_hiv_lt_", run_countries, ".h5"), folder=paste0(out_dir_lt_whiv, "/pre_scaled"), continual=T, warn_only = T)
      assertable::check_files(paste0("with_hiv_env_", run_countries, ".h5"), folder=paste0(out_dir_env, "/pre_scaled"), continual=T)
    }
}

## Launch scaling and aggregation code (parallel by year)
  if(start <= 2 & end >= 2) {
    for(year in run_years) {
      qsub(paste0("scale_agg_mlt_", year), 
           paste0(code_dir, "/02_scale_results.R"),
           # slots = 8, 
           slots = 10,
           hostgroup = "c2",
           pass = list(mlt_lt_run_id, 
                       year, 
                       parent_versions[["age sex estimate"]],
                       gbd_year), 
           proj = proj,
           shell = "r_shell_singularity_3421.sh",
           submit = !test_submission, log=T)
    }
    print("Waiting 60 minutes before checking for 02_scale results")
    Sys.sleep(60*60)

    assertable::check_files(paste0("hiv_free_lt_", run_years, ".h5"), folder=paste0(out_dir_lt_free, "/scaled"), continual=T, sleep_time = 120, warn_only = T)
    assertable::check_files(paste0("with_hiv_lt_", run_years, ".h5"), folder=paste0(out_dir_lt_whiv, "/scaled"), sleep_time = 300, sleep_end = 120, continual=T, warn_only = T)
	
	## Auto-retry failures of LT envelope process after 7 hours
    fail_years <- assertable::check_files(paste0("with_hiv_env_", run_years, ".h5"), folder=paste0(out_dir_env, "/scaled"), continual=T, sleep_end = 180, warn_only = T)
    if(length(fail_years) > 0) {
      fail_years <- gsub("with_hiv_env_", "", fail_years)
      relaunch_years <- gsub("\\.h5", "", fail_years)


      for(year in relaunch_years) {
        system(paste0("qdel scale_agg_mlt_", year))
  		  qsub(paste0("scale_agg_mlt_", year), 
  			   paste0(code_dir, "/02_scale_results.R"),
  			   # slots = 8, 
  			   slots = 10,
  				 hostgroup = "c2",
  			   pass = list(mlt_lt_run_id, 
  						   year, 
  						   parent_versions[["age sex estimate"]],
  						   gbd_year), 
  			   proj = proj,
  			   shell = "r_shell_singularity_3421.sh",
  			   submit = !test_submission, log=T)
  	  }

      print("Waiting 45 minutes before relaunch file check")
      Sys.sleep(60 * 45)
      assertable::check_files(paste0("hiv_free_lt_", run_years, ".h5"), folder=paste0(out_dir_lt_free, "/scaled"), continual=T, sleep_time = 120)
      assertable::check_files(paste0("with_hiv_lt_", run_years, ".h5"), folder=paste0(out_dir_lt_whiv, "/scaled"), sleep_end = 120, continual=T)
      assertable::check_files(paste0("with_hiv_env_", run_years, ".h5"), folder=paste0(out_dir_env, "/scaled"), sleep_end = 120, continual=T)
    }
  }

## Launch upload code
  ## For now, call the env run ID the same as the LT run ID (until it gets its own db entry)

  if(start <= 3 & end >= 3) {
    qsub(paste0("mlt_compile_upload"), 
         paste0(code_dir, "/03_compile_upload.R"),
         slots = 15, 
         pass = list(mlt_lt_run_id, 
                     mlt_env_run_id, 
                     map_est_run_id, 
                     gbd_year), 
         proj = proj,
         shell = "r_shell_singularity_3421.sh",
         submit = !test_submission, log=T)
  }
  