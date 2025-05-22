## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 7b_long_save_results.R
## Description: Format long_covid intermediate outputs and save to EPI database.
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

source(file.path(.repo_base, 'FILEPATH/utils.R'))
source(file.path(roots$k, 'FILEPATH/get_location_metadata.R'))
source(file.path(roots$k, 'FILEPATH/save_results_epi.R'))

## --------------------------------------------------------------------- ----

## Data Processing Functions ------------------------------------------- ----

main <- function(output_version, measure, location_set_id, release_id, mark_as_best, save_incidence, all_gbd_estimation_years, db_description) {

  if (save_incidence | measure %in% c('any', 'midmod_any', 'hospital_any', 'icu_any')) {
    measure_ids <- c(5, 6)
  } else {
    measure_ids <- c(5)
  }

  # Step 1
  locs <- get_location_metadata(location_set_id=location_set_id, 
                                release_id=release_id)
  
  # Expected locations to have files created
  expected_locs <- locs[most_detailed == 1, .(location_id, location_ascii_name)]
  
  i_base <- file.path(
    get_core_ref("data_output", "stage_2"),
    output_version,
    "stage_2"
  )
  
  o_base <- file.path(
    get_core_ref("data_output", "final"),
    output_version
  )
  
  me_dir <- paste0(measure, "_", get_core_ref(paste0(measure, "_me_id")))
  
  # Step 2
  # For locations that did not successfully produce results create empty
  # files into the final output directory with draws of 0, using an example location 
  # as the template
  successful_locs <- .get_successful_locs_long_covid(output_version = output_version, 
                                                     me_dir = me_dir,
                                                     expected_locs = expected_locs)[successful == TRUE]
  
  # Do not proceed if no successful locations produced
  if (nrow(successful_locs) == 0){
    stop('No successful locations produced. Exiting...')
  }

  sucessful_loc_prefix <- paste0(successful_locs$location_ascii_name[1], '_',
                                 successful_locs$location_id[1])
  
  # Incidence
  inc <- fread(
    file.path(
      o_base,
      me_dir,
      paste0(sucessful_loc_prefix, "_6.csv")
    )
  )
  
  # Prevalence
  prev <- fread(
    file.path(
      o_base,
      me_dir,
      paste0(sucessful_loc_prefix, "_5.csv")
    )
  )
  
  # Identify set difference between between most detailed locations and successful locations
  unsucessful_locs <- setdiff(expected_locs$location_id, successful_locs$location_id)

  # Generate the unsucessful locations
  for (loc_id in unsucessful_locs) {
    cat('failed locations:\n', unsucessful_locs)
    
    row <- locs[location_id == loc_id]
    
    # Finalize dataset
    i <- copy(inc)
    i[, location_id := loc_id]
    i[, (roots$draws) := 0]
    p <- copy(prev)
    p[, location_id := loc_id]
    p[, (roots$draws) := 0]
    
    # Save to final location
    fwrite(
      i,
      file.path(
        o_base,
        me_dir,
        paste0(row$location_ascii_name, "_", row$location_id, "_6.csv")
      )
    )
    
    fwrite(
      p,
      file.path(
        o_base,
        me_dir,
        paste0(row$location_ascii_name, "_", row$location_id, "_5.csv")
      )
    )
    
    rm(row, loc_id, i, p)
    
  }

  
  cat('Saving results to epi database...\n')
  ## Call save_results_epi ----------------------------------------------- ----
  
  # Ensure db_description text is wrapped in quotes
  db_description <- paste0("'", db_description, "'")
  
  # Step 3
  years <- all_gbd_estimation_years
  
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
    measure_id = measure_ids,
    metric_id = 3,
    n_draws = 1000,
    release_id = release_id,
    mark_best = mark_as_best, # decomp_step = 'iterative',
    birth_prevalence = F,
    bundle_id = 9263, # using asymp bundle id
    crosswalk_version_id = 35450
  ) # using asymp cv_id
  
  ## --------------------------------------------------------------------- ----
}

## --------------------------------------------------------------------- ----

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
  
  # Parse estimation_years from comma separated character to integer vector
  all_gbd_estimation_years <- as.vector(as.numeric(unlist(strsplit(all_gbd_estimation_years, ","))))

  cat(paste0('Saving [', measure, '] results from run [', output_version, 
             ']...\n'))
  cat(paste0('Output located in ', get_core_ref('data_output', 'final'),
             output_version, '/', '\n\n'))
  
  
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
  output_version <- 'test'
  measure <- 'fatigue' # cognitive_mild, cognitive_severe, fatigue, 
                # respiratory_mild, respiratory_moderate, respiratory_severe, 
                # cognitive_mild_fatigue, cognitive_severe_fatigue, cognitive_mild_respiratory_mild, 
                # cognitive_mild_respiratory_moderate, cognitive_mild_respiratory_severe, 
                # cognitive_severe_respiratory_mild, cognitive_severe_respiratory_moderate, 
                # cognitive_severe_respiratory_severe, fatigue_respiratory_mild, fatigue_respiratory_moderate, 
                # fatigue_respiratory_severe, cognitive_mild_fatigue_respiratory_mild, cognitive_mild_fatigue_respiratory_moderate, 
                # cognitive_mild_fatigue_respiratory_severe, cognitive_severe_fatigue_respiratory_mild, 
                # cognitive_severe_fatigue_respiratory_moderate, cognitive_severe_fatigue_respiratory_severe
  
  location_set_id <- 35
  release_id <- 16
  mark_as_best <- FALSE
  save_incidence <- FALSE
  all_gbd_estimations_years <- roots$all_gbd_estimation_years
  db_description <- 'test'
  
  
  cat(paste0('Saving [', measure, '] results from run [', output_version, 
             ']...\n'))
  cat(paste0('Output located in ', get_core_ref('data_output', 'final'),
             output_version, '/', '\n\n'))
  
  
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
