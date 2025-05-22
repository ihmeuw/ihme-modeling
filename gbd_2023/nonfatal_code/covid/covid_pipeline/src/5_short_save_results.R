## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 5b_short_save_results.R
## Description: Format short_covid intermediate outputs and save to EPI database.
##              Numbered comments correspond to documentation on this HUB page:
##              ADDRESS
## Contributors: NAME
## --------------------------------------------------------------------- ----


## Environment Prep ---------------------------------------------------- ----

# Init NF COVID repo
# Dynamically set user specific repo base where nf_covid is located
.repo_base <-
  strsplit(
    whereami::whereami(path_expand = TRUE),
    "nf_covid"
  )[[1]][1]

.nf_repo <- file.path(.repo_base, 'nf_covid/')

source(file.path(.repo_base, 'FILEPATH/utils.R'))
source(file.path(roots$k, 'FILEPATH/get_location_metadata.R'))
source(file.path(roots$k, 'FILEPATH/save_results_epi.R'))

## --------------------------------------------------------------------- ----


## Data Processing Functions ------------------------------------------- ----
main <- function(output_version, measure, location_set_id, release_id, mark_as_best, save_incidence, all_gbd_estimation_years, db_description) {
  
  if (save_incidence) {
    measure_ids <- c(5, 6)
  } else {
    measure_ids <- c(5)
  }

  # Step 1
  locs <- get_location_metadata(location_set_id = location_set_id, release_id = release_id)
  
  # Expected locations to have files created
  expected_locs <- locs[most_detailed == 1, .(location_id, location_ascii_name)]
  
  i_base <- paste0(get_core_ref('data_output', 'stage_1'), output_version, 
                   'FILEPATH')
  o_base <- paste0(get_core_ref('data_output', 'final'), output_version, '/')
  
  
  # Step 2
  # For locations that did not successfully produce results in stage_1, create empty
  # files into the final output directory with draws of 0, using an example location 
  # as the template
  successful_locs <- .get_successful_locs_short_covid(
    output_version = output_version, 
    stage = 'stage_1',
    expected_locs = expected_locs)
  
  sucessful_loc_prefix <- paste0(successful_locs$location_ascii_name[1], '_',
                                 successful_locs$location_id[1])
  # Incidence
  inc <- fread(
    file.path(
      o_base,
      paste0(measure, "_", get_core_ref(paste0(measure, "_me_id"))),
      paste0(sucessful_loc_prefix, "_6.csv")
    )
  )
  
  # Prevalence
  prev <- fread(
    file.path(
      o_base,
      paste0(measure, "_", get_core_ref(paste0(measure, "_me_id"))),
      paste0(sucessful_loc_prefix, "_5.csv")
    )
  )

  # Identify set difference between between most detailed locations and successful locations
  # From stage_1
  unsucessful_locs <- locs[most_detailed == 1 & 
                             location_id %ni% successful_locs[, location_id], 
                           location_id]
  
  # Generate the unsucessful locations
  for (loc_id in unsucessful_locs) {
    
    row <- locs[location_id == loc_id,]
    
    # Finalize dataset
    i <- copy(inc)
    i[, location_id := loc_id]
    i[, (roots$draws) := 0]
    p <- copy(prev)
    p[, location_id := loc_id]
    p[, (roots$draws) := 0]
    
    # Save to final location
    fwrite(i, paste0(o_base, measure, '_', get_core_ref(paste0(measure, '_me_id')),
                            '/', row$location_ascii_name, '_', row$location_id, '_6.csv'))
    fwrite(p, paste0(o_base, measure, '_', get_core_ref(paste0(measure, '_me_id')),
                            '/', row$location_ascii_name, '_', row$location_id, '_5.csv'))
    
    rm(row, loc_id, i, p)
  }
  
  rm(inc, prev)

  
  cat('Saving results to epi database...\n')
  ## Call save_results_epi ----------------------------------------------- ----
  
  # Ensure db_description text is wrapped in quotes
  db_description <- paste0("'", db_description, "'")
  
  # Step 3
  #  years <- roots$all_gbd_estimation_years
  years <- all_gbd_estimation_years
  if (measure == 'asymp') {
    save_results_epi(
      input_dir = file.path(
        o_base,
        paste0(
          measure, "_", get_core_ref(paste0(measure, "_me_id"))
        )
      ),
      input_file_pattern = "{location_ascii_name}_{location_id}_{measure_id}.csv",
      modelable_entity_id = get_core_ref(paste0(measure, "_me_id")),
      description = db_description,
      year_id = years,
      sex_id = c(1, 2),
      measure_id = c(5, 6),
      metric_id = 3,
      n_draws = 1000,
      release_id = 16,
      mark_best = TRUE,
      birth_prevalence = FALSE,
      bundle_id = 9263, # using asymp bundle id
      crosswalk_version_id = 35450
    ) # using asymp cv_id
  } else {
    save_results_epi(
      input_dir = file.path(
        o_base,
        paste0(
          measure, "_",
          get_core_ref(paste0(measure, "_me_id"))
        )
      ),
      input_file_pattern = "{location_ascii_name}_{location_id}_{measure_id}.csv",
      modelable_entity_id = get_core_ref(paste0(measure, "_me_id")),
      description = db_description,
      year_id = years,
      sex_id = c(1, 2),
      measure_id = measure_ids,
      metric_id = 3,
      n_draws = 1000,
      release_id = release_id,
      mark_best = mark_as_best,
      birth_prevalence = F,
      bundle_id = 9263, # using asymp buncle id
      crosswalk_version_id = 35450
    ) # using asymp cv_id
  }
  
  ## --------------------------------------------------------------------- ----
  
}

## --------------------------------------------------------------------- ----
# Location_Set_id
# Release_id

# To_best

# All_gbd_estimation_years
# Save_incidence


## Run All ------------------------------------------------------------- ----
if (!interactive()){
  begin_time <- Sys.time()
  
  parser <- argparse::ArgumentParser()
  parser$add_argument(
    "--output_version",
    type = "character"
  )
  parser$add_argument(
    "--measure",
    type = "character"
  )
  parser$add_argument(
    "--location_set_id",
    type = "integer"
  )
  parser$add_argument(
    "--release_id",
    type = "integer"
  )
  parser$add_argument(
    "--mark_as_best",
    type = "logical"
  )
  parser$add_argument(
    "--save_incidence",
    type = "logical"
  )
  parser$add_argument(
    "--all_gbd_estimation_years",
    type = "character"
  )
  parser$add_argument(
    "--db_description",
    type = "character"
  )
  
  args <- parser$parse_args()
  print(args)
  list2env(args, envir = environment())
  
  all_gbd_estimation_years <- as.vector(as.numeric(unlist(strsplit(all_gbd_estimation_years, ","))))


  cat("Using inputs:\n output_version: ", output_version, 
      "\n measure: ", measure, 
      "\n location_set_id: ", location_set_id, 
      "\n release_id: ", release_id, 
      "\n mark_as_best: ", mark_as_best, 
      "\n save_incidence: ", save_incidence, 
      "\n all_gbd_estimation_years: ", all_gbd_estimation_years, 
      "\n db_description: ", db_description, "\n\n")
  
  # Run main function
  main(
    output_version = output_version,
    measure = measure,
    location_set_id = location_set_id,
    release_id = release_id,
    mark_as_best = mark_as_best,
    save_incidence = save_incidence,
    all_gbd_estimation_years = all_gbd_estimation_years,
    db_description = db_description
  )
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
} else {
  begin_time <- Sys.time()
  

  # Command Line Arguments
  # output_version <- '2022-04-02.01gbd'
  output_version <- '2024-08-07-test'
  measure <- 'mild' # asymp, mild, moderate, hospital, icu
  location_set_id <- 35
  release_id <- 16
  mark_as_best <- TRUE
  save_incidence <- TRUE
  all_gbd_estimation_years <- c(1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022, 2023)
  # db_description <- '2022-04-02.01gbd'
  db_description <- 'Run_2024-08-07-test'
  
  cat("Using inputs:\n output_version:", output_version, 
      "\n measure:", measure, 
      "\n location_set_id:", location_set_id, 
      "\n release_id:", release_id, 
      "\n mark_as_best:", mark_as_best, 
      "\n save_incidence:", save_incidence, 
      "\n all_gbd_estimation_years:", all_gbd_estimation_years, 
      "\n db_description:", db_description, "\n\n")
  
  # Run main function
  main(
    output_version = output_version,
    measure = measure,
    location_set_id = location_set_id,
    release_id = release_id,
    mark_as_best = mark_as_best,
    save_incidence = save_incidence,
    all_gbd_estimation_years = all_gbd_estimation_years,
    db_description = db_description
  )
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
}
## --------------------------------------------------------------------- ----