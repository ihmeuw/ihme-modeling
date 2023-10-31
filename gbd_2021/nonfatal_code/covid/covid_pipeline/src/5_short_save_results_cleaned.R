## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 5b_short_save_results.R
## Description: Format short_covid intermediate outputs and save to EPI database.
##              Numbered comments correspond to documentation on this HUB page:
##              ADDRESS/5b_short_save_results.R
## Contributors: 
## --------------------------------------------------------------------- ----


## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

# Init NF COVID repo
if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}
.nf_repo <- paste0(.repo_base, 'FILEPATH')

source(paste0(.repo_base, '	FILEPATH/utils.R'))
source(paste0(roots$'ROOT', 'FILEPATH/get_location_metadata.R'))
source(paste0(roots$'ROOT', 'FILEPATH/save_results_epi.R'))

## --------------------------------------------------------------------- ----


## Data Processing Functions ------------------------------------------- ----
main <- function(output_version, measure, location_set_id, release_id, mark_as_best, save_incidence, all_gbd_estimation_years, db_description) {
  
  if (save_incidence) {
    measure_ids <- c(5, 6)
  } else {
    measure_ids <- c(5)
  }

  # Step 1
  locs <- get_location_metadata(location_set_id=location_set_id, release_id=release_id)[, c('location_id', 'location_ascii_name',
                                                                   'region_id', 'region_name', 'super_region_name',
                                                                   'super_region_id', 'most_detailed')]
  
  i_base <- 'FILEPATH'
  o_base <- 'FILEPATH
  
  
  # Step 2
  # Temporary while we're getting errored locations and incomplete inputs
  # inc <- fread(paste0(o_base, measure, '_', get_core_ref(paste0(measure, '_me_id')), '/Afghanistan_160_6.csv'))
  # prev <- fread(paste0(o_base, measure, '_', get_core_ref(paste0(measure, '_me_id')), '/Afghanistan_160_5.csv'))
  inc <- fread(paste0(o_base, measure, '_', get_core_ref(paste0(measure, '_me_id')), '/Albania_43_6.csv'))
  prev <- fread(paste0(o_base, measure, '_', get_core_ref(paste0(measure, '_me_id')), '/Albania_43_5.csv'))
  for (loc_id in unique(locs[most_detailed==1]$location_id
                        )[unique(locs[most_detailed==1]$location_id) %ni% 
                          unique(.get_successful_locs(output_version, 'stage_1')$location_id)]) {
    
    row <- locs[location_id == loc_id,]
    
    # Finalize dataset
    i <- copy(inc)
    i[, location_id := loc_id]
    i[, (roots$draws) := 0]
    p <- copy(prev)
    p[, location_id := loc_id]
    p[, (roots$draws) := 0]
    
    # Save to final location
    fwrite(i, 'FILEPATH'))
    fwrite(p, 'FILEPATH'))
    
    rm(row, loc_id, i, p)
  }
  rm(inc, prev)

  
  cat('Saving results to epi database...\n')
  ## Call save_results_epi ----------------------------------------------- ----
  
  # Step 3
  #  years <- roots$all_gbd_estimation_years
  years <- all_gbd_estimation_years
  if (measure == 'asymp') {
    save_results_epi(input_dir = 'FILEPATH',
                     input_file_pattern = '{location_ascii_name}_{location_id}_{measure_id}.csv', 
                     modelable_entity_id = get_core_ref(paste0(measure, '_me_id')),
                     description = db_description,
                     year_id = years,
                     sex_id = c(1, 2), measure_id = c(5,6), metric_id = 3,
                     n_draws = 1000, release_id = release_id,
                     mark_best = mark_as_best, #decomp_step = 'iterative', 
                     birth_prevalence = F, bundle_id = 9263, # using asymp buncle id
                     crosswalk_version_id = 35450) # using asymp cv_id
  } else {
    save_results_epi(input_dir = 'FILEPATH',
                     input_file_pattern = '{location_ascii_name}_{location_id}_{measure_id}.csv', 
                     modelable_entity_id = get_core_ref(paste0(measure, '_me_id')),
                     description = db_description,
                     year_id = years,
                     sex_id = c(1, 2), measure_id = measure_ids, metric_id = 3,
                     n_draws = 1000, release_id = release_id,
                     mark_best = mark_as_best, # decomp_step = 'iterative', 
                     birth_prevalence = F, bundle_id = 9263, # using asymp buncle id
                     crosswalk_version_id = 35450) # using asymp cv_id
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
  
  # Command Line Arguments
  output_version <- as.character(commandArgs()[8])
  measure <- as.character(commandArgs()[9])
  location_set_id <-as.numeric(commandArgs()[10]) 
  release_id <-as.numeric(commandArgs()[11]) 
  mark_as_best <- as.logical(commandArgs()[12]) 
  save_incidence <- as.logical(commandArgs()[13]) 
  all_gbd_estimation_years_str <- as.character(commandArgs()[14])
  all_gbd_estimation_years <- as.vector(as.numeric(unlist(strsplit(all_gbd_estimation_years_str, ","))))
  db_description <- as.character(commandArgs()[15])
  
  cat(paste0('Saving [', measure, '] results from run [', output_version, 
             ']...\n'))
  cat(paste0('Output located in ', get_core_ref('data_output', 'final'),
             output_version, '/', '\n\n'))
  
  
  # Run main function
  main(output_version, measure, location_set_id, release_id, mark_as_best,
       save_incidence, all_gbd_estimation_years, db_description)
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
} else {
  begin_time <- Sys.time()
  

  # Command Line Arguments
  output_version <- '2022-04-02.01gbd'
  measure <- 'icu' # asymp, mild, moderate, hospital, icu
  location_set_id <-35
  release_id <- 9
  mark_as_best <- TRUE
  save_incidence <- FALSE
  all_gbd_estimation_years <- c(1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022)
  db_description <- '2022-04-02.01gbd'
  
  
  cat(paste0('Saving [', measure, '] results from run [', output_version, 
             ']...\n'))
  cat(paste0('Output located in ', get_core_ref('data_output', 'final'),
             output_version, '/', '\n\n'))
  
  
  # Run main function
  main(output_version, measure, location_set_id, release_id, mark_as_best,
       save_incidence, all_gbd_estimation_years, db_description)
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
}
## --------------------------------------------------------------------- ----