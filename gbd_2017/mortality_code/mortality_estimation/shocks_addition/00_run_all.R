###############################################################################################################

###############################################################################################################
## Set up settings
  rm(list=ls())
  
  if (Sys.info()[1]=="Windows") {
    user <- Sys.getenv("USERNAME")
  } else {
    user <- Sys.getenv("USER")
  }
  
## Source libraries and functions
  library(readr) # For check_files, import_files, etc.
  library(RMySQL)
  library(data.table) # 1.10.4
  library(assertable) # For check_files, import_files, etc.
  library(slackr)
  library(haven)
  library(mortdb)
  library(mortcore)
  library(ltcore)
  source(paste0("get_population.R"))

  proj <- ""

  code_dir <- FILEPATH

###############################################################################################################
## Run options
  new_run <- T # Do you want to generate a new run_id and folder structure etc.? Only if you want to start.
  run_comment <- "Comment" # Only used if generating a new run or uploading
  start <- 1
  end <- 4
  
  file_del <- F
  test_submission <- F # Do you want to just print the qsub statement rather than actually submitting the jobs? Recommended to run with file_del = F
  assert_upload <- T # Do you want to make sure there aren't finalizer results already uploaded to the db for the runs of interest?
  mark_best <- F

  gbd_year <- 2017
  gbd_round_id <- 5
  sexes <- c("male", "female")
  run_years <- c(1950:gbd_year)

  if(new_run == T & start != 1) stop(paste0("You are attempting to start a new run without starting at step 1. ",
                                            "This means your code will break since the input files won't exist. ",
                                            "Please consider either starting at 1, or doing a re-run of a previous model"))

  shell_script <- "r_shell_singularity_mort.sh"
  
###############################################################################################################
## Create Directory structure

db_connection <- dbConnect(RMySQL::MySQL())
sql_command <- paste0("SELECT shock_version_id FROM cod.shock_version WHERE gbd_round_id = ", gbd_round_id, " AND shock_version_status_id = 1;")
v_id <- dbGetQuery(db_connection, sql_command)
version_shock_aggregator <- as.integer(v_id[1, 'shock_version_id'])
print(paste0("using shock_aggregator output version: ", version_shock_aggregator))
dbDisconnect(db_connection)

run_comment <- paste0("Shocks Addition: ", run_comment, " -- Shock aggregator version ", version_shock_aggregator)

version_no_shock_death_number <- get_proc_version(model_name = "no shock death number", model_type = "estimate", run_id = "recent_completed")
version_no_shock_life_table <- get_proc_version(model_name = "no shock life table", model_type = "estimate", run_id = "recent_completed")
version_age_sex <- get_proc_version(model_name = "age sex", model_type = "estimate", run_id = "best") 

# get population run_id mapped to no shock death number run to map to this run
no_shock_death_number_lineage <- get_proc_lineage(model_name = "no shock death number", model_type = "estimate", run_id = version_no_shock_death_number)
version_population <- no_shock_death_number_lineage[parent_process_name == "population estimate", parent_run_id]

version_shock_death_number <- gen_new_version(model_name = "with shock death number",
                                              model_type = "estimate",
                                              comment = run_comment)

version_shock_life_table <- gen_new_version(model_name = "with shock life table",
                                            model_type = "estimate",
                                            comment = run_comment)

## Generate shocks addition run -- since this should be new each time.
  reckoning_final_rows <- get_mort_outputs("no shock death number", "estimate", run_id = version_no_shock_death_number, estimate_stage_id = 5, location_id = 1, year_id = gbd_year)
  
  out_dir <- FILEPATH

  input_dir <- paste0(out_dir,"/inputs")
  out_dir_wshock_lt <- paste0(out_dir,"/lt_wshock")
  out_dir_wshock_dn <- paste0(out_dir,"/env_wshock")
  out_dir_nshock_lt <- paste0(out_dir,"/lt_no_shock")
  out_dir_nshock_dn <- paste0(out_dir,"/env_no_shock")
  out_dir_nhiv_lt <- paste0(out_dir,"/lt_no_hiv")
  out_dir_nhiv_dn <- paste0(out_dir,"/env_no_hiv")
  out_dir_summary <- paste0(out_dir,"/summary")
  out_dir_logs <- paste0(out_dir, "/logs")

###############################################################################################################
## Import and save maps and other metadata
## Save flat locations file (to save parallel jobs from querying the db)
  lowest_locations <- setDT(get_locations(level = "lowest", gbd_year = gbd_year))
  
  run_countries <- lowest_locations$ihme_loc_id

  all_locations <- setDT(get_locations(level = "all", gbd_year = gbd_year))
  sdi_locations <- setDT(get_locations(level = "all", gbd_type = "sdi", gbd_year = gbd_year))
  region_sdi_locations <- rbindlist(list(all_locations[level <= 2], sdi_locations[level == 0]))
  all_locations <- rbindlist(list(all_locations, sdi_locations[level == 0]), use.names = T)

  pop_age_ids <- c(age_map[age_group_id <= 32, age_group_id], 235, 2, 3, 4) # Subset to through 95+ age groups, and include NN population
  pops <- get_mort_outputs("population", "estimate",
                           age_group_id = pop_age_ids, sex_id = c(1:3), year_id = run_years,
                           location_id = unique(all_locations$location_id), run_id = input_pop_run)

  pops <- pops[, .(location_id, sex_id, year_id, age_group_id, population = mean)]
  pops[, run_id := NULL]

  assert_values(pops, colnames(pops), "not_na")
  assert_ids(pops, list(sex_id = c(1:3),
                        year_id = run_years,
                        age_group_id = pop_age_ids,
                        location_id = unique(all_locations$location_id)))

  write_csv(pops, paste0(input_dir, "/population.csv"))


###############################################################################################################
## Submit Jobs

## Set code_dir
  setwd(paste0(code_dir))

## Launch mx rescaling code
  if(start <= 1 & end >= 1) {
    array_qsub(paste0("rescale_mx"), 
               paste0(code_dir, "/01_rescale_mx.R"),
               slots = 2,
               num_tasks = nrow(lowest_locations),  
               hostgroup = "c2", 
               pass = list(version_shock_death_number,
                           version_no_shock_death_number,
                           gbd_year), 
               proj = proj,
               shell = shell_script,
               submit = !test_submission, log=T)

    print("Waiting 5 minutes before checking for 01 results")
    Sys.sleep(60*5)

    ## Auto-retry failures of country-specific results if still missing results after 2 hrs and 30 minutes
    launch_countries <- assertable::check_files(paste0("mx_ax_", run_countries, ".h5"), folder=paste0(out_dir, "/lowest_outputs"), continual=T, sleep_end = 45, warn_only = T)
    if(length(launch_countries) > 0) {
      launch_countries <- gsub("mx_ax_", "", launch_countries)
      launch_countries <- gsub("\\.h5", "", launch_countries)

      run_counter <- 0
      for(country in launch_countries) {
        qsub(paste0("rescale_mx_", country), 
             paste0(code_dir, "/01_rescale_mx.R"),
             slots = 2, 
             pass = list(country, 
                         version_shock_death_number,
                         version_no_shock_death_number,
                         gbd_year), 
             proj = proj,
             shell = shell_script,
             submit = !test_submission, log=T)
        run_counter <- run_counter + 1
        if(run_counter == 50) {
          print("Sleeping 1 second for scheduler")
          Sys.sleep(1)
          run_counter <- 1
        }
      }

      Sys.sleep(60*15)

      assertable::check_files(paste0("mx_ax_", run_countries, ".h5"), folder=paste0(out_dir, "/lowest_outputs"), continual=T, sleep_end = 30)
    }
}

## Launch scaling and aggregation code (parallel by year)
  if(start <= 2 & end >= 2) {
    for(year in run_years) {
      qsub(paste0("agg_finalizer_", year), 
           paste0(code_dir, "/02_agg_results.R"),
           slots = 17,
           hostgroup = "c2",
           pass = list(year, 
                       version_shock_death_number,
                       gbd_year), 
           proj = proj,
           shell = shell_script,
           submit = !test_submission, log=T)
    }
    print("Waiting 60 minutes before checking for 02_scale results")
    Sys.sleep(60*60)

    assertable::check_files(paste0("summary_env_", run_years, ".csv"), folder=out_dir_nhiv_dn, continual=T, sleep_time = 180, sleep_end = 180, warn_only = T)
    assertable::check_files(paste0("summary_env_", run_years, ".csv"), folder=out_dir_nshock_dn, continual=T, sleep_time = 180, sleep_end = 180, warn_only = T)
	
	## Auto-retry failures of aggregation process after 7 hours
    fail_years <- assertable::check_files(paste0("summary_env_", run_years, ".csv"), folder=out_dir_wshock_dn, continual=T, sleep_time = 180, sleep_end = 180, warn_only = T)
    
    if(length(fail_years) == 0) {
      fail_years <- assertable::check_files(paste0("summary_env_", run_years, ".csv"), folder=out_dir_nshock_dn, continual=F, warn_only = T)
    }

    if(length(fail_years) > 0) {
      fail_years <- gsub("summary_env_", "", fail_years)
      relaunch_years <- gsub("\\.csv", "", fail_years)
      for(year in relaunch_years) {
       qsub(paste0("agg_finalizer_", year), 
             paste0(code_dir, "/02_agg_results.R"),
             slots = 17,
             hostgroup = "c2",
             pass = list(year, 
                         version_shock_death_number,
                         gbd_year), 
             proj = proj,
             shell = shell_script,
             submit = !test_submission, log=T)
      }
      print("Waiting 30 minutes before relaunch file check")
      Sys.sleep(60 * 30)

      assertable::check_files(paste0("summary_env_", run_years, ".csv"), folder=out_dir_nhiv_dn, continual=T, sleep_time = 180, sleep_end = 30, warn_only = T)
      assertable::check_files(paste0("summary_env_", run_years, ".csv"), folder=out_dir_nshock_dn, continual=T, sleep_time = 180, sleep_end = 20, warn_only = T)
      assertable::check_files(paste0("summary_env_", run_years, ".csv"), folder=out_dir_wshock_dn, continual=T, sleep_time = 180, sleep_end = 20, warn_only = T)
    }
  }

if(start <= 3 & end >= 3) {
    for(year in run_years) {
      qsub(paste0("sub_agg_loc_shocks_", year), 
           paste0(code_dir, "/03_agg_loc_shocks.R"),
           slots = 4,
           hostgroup = "c2",
           pass = list(year, 
                       version_shock_death_number,
                       version_shock_aggregator,
                       gbd_year), 
           proj = proj,
           shell = "r_shell_singularity_3501.sh",
           submit = !test_submission, log=T)
    }
    print("Waiting 2 minutes before checking for 03_sub_agg_loc_shocks")
    Sys.sleep(60*2)

    fail_years <- assertable::check_files(paste0("summary_env_agg_", run_years, ".csv"), folder=out_dir_wshock_dn, continual=T, sleep_time = 30, sleep_end = 30, warn_only = T)
    
    if(length(fail_years) == 0) {
      fail_years <- assertable::check_files(paste0("summary_env_agg_", run_years, ".csv"), folder=out_dir_wshock_dn, continual=F, warn_only = T)
    }

    if(length(fail_years) > 0) {
        fail_years <- gsub("summary_env_agg_", "", fail_years)
        relaunch_years <- gsub("\\.csv", "", fail_years)
        for(year in relaunch_years) {
         qsub(paste0("sub_agg_loc_shocks_", year), 
               paste0(code_dir, "/03_agg_loc_shocks.R"),
               slots = 4,
               hostgroup = "c2",
               pass = list(year, 
                           version_shock_death_number,
                           version_shock_aggregator,
                           gbd_year), 
               proj = proj,
               shell = "FILEPATH/r_shell_singularity_3501.sh",
               submit = !test_submission, log=T)
      }
    }
}

  if(start <= 4 & end >= 4) {
    array_qsub(paste0("resave_wshock"), 
               paste0(code_dir, "/04_resave_wshock_csv.R"),
               slots = 2,
               num_tasks = nrow(all_locations),  
               pass = list(version_shock_death_number,
                           gbd_year), 
               proj = proj,
               shell = shell_script,
               submit = !test_submission, log=T)

    print("Waiting 5 minutes before checking for 03 resave results")
    Sys.sleep(5 * 60)
    fail_locs <- assertable::check_files(paste0("env_", unique(all_locations$location_id), ".csv"), folder=paste0(out_dir_wshock_dn, "/final_formatted"), continual=T, warn_only = T, sleep_end = 30)

    if(length(fail_locs) > 0) {
      fail_locs <- gsub("env_", "", fail_locs)
      relaunch_locs <- gsub("\\.csv", "", fail_locs)
      relaunch_locs <- unique(all_locations[location_id %in% relaunch_locs, ihme_loc_id])
      for(loc in relaunch_locs) {
       qsub(paste0("resave_wshock_", loc), 
             paste0(code_dir, "/04_resave_wshock_csv.R"),
             slots = 2,
             hostgroup = "c2",
             pass = list(loc, 
                         version_shock_death_number,
                         gbd_year), 
             proj = proj,
             shell = shell_script,
             submit = !test_submission, log=T)
      }
      Sys.sleep(5 * 60)
      assertable::check_files(paste0("env_", unique(all_locations$location_id), ".csv"), folder=paste0(out_dir_wshock_dn, "/final_formatted"), continual=T, warn_only = T, sleep_end = 15)
    }

    qsub(paste0("finalizer_upload"), 
         paste0(code_dir, "/04_compile_upload.R"),
         slots = 10, 
         hostgroup = "c2", 
         pass = list(version_shock_death_number, 
                     version_shock_life_table,
                     version_no_shock_death_number,
                     version_no_shock_life_table, 
                     gbd_year), 
         proj = proj,
         shell = shell_script,
         submit = !test_submission, log=T)
  }
