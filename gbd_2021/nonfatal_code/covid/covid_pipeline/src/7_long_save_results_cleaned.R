## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 7b_long_save_results.R
## Description: Format long_covid intermediate outputs and save to EPI database.
##              Numbered comments correspond to documentation on this HUB page:
##              ADDRESS/7b_long_save_results.R
## Contributors: 
## --------------------------------------------------------------------- ----


## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

.repo_base <- '../'

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0('ROOT', 'FILEPATH/get_location_metadata.R'))
source(paste0('ROOT', 'FILEPATH/save_results_epi.R'))

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
  
  i_base <- paste0('FILEPATH')
  o_base <- paste0('FILEPATH')
  
  
  # Step 2
  # Temporary while we're getting errored locations and incomplete inputs
  # inc <- fread(paste0('FILEPATH', '/Afghanistan_160_6.csv'))
  # prev <- fread(paste0('FILEPATH', '/Afghanistan_160_5.csv'))
  inc <- fread(paste0('FILEPATH', '/Albania_43_6.csv'))
  prev <- fread(paste0('FILEPATH', '/Albania_43_5.csv'))
  
  locs_done <- data.table(table(.get_successful_locs(output_version, 'final')$location_id))
  setnames(locs_done, c('V1', 'N'), c('loc_id', 'count'))
  locs_done <- locs_done[count==max(count)]
  
  
  for (loc_id in unique(locs[most_detailed==1]$location_id
                        [unique(locs[most_detailed==1]$location_id) %ni% 
                          unique(locs_done$loc_id)])) {
    cat('failed locations:\n', unique(locs[most_detailed==1]$location_id
                                      [unique(locs[most_detailed==1]$location_id) %ni% 
                                          unique(locs_done$loc_id)]))
#    BREAK
    row <- locs[location_id == loc_id,]
    
    # Finalize dataset
    i <- copy(inc)
    i[, location_id := loc_id]
    i[, (roots$draws) := 0]
    p <- copy(prev)
    p[, location_id := loc_id]
    p[, (roots$draws) := 0]
    
    # Save to final location
    fwrite(i, 'FILEPATH',
                            '/', row$location_ascii_name, '_', row$location_id, '_6.csv'))
    fwrite(p, 'FILEPATH',
                            '/', row$location_ascii_name, '_', row$location_id, '_5.csv'))
    
    rm(row, loc_id, i, p)
    
  }
  rm(inc, prev)
  
  
  cat('Saving results to epi database...\n')
  ## Call save_results_epi ----------------------------------------------- ----
  
  # Step 3
#  years <- roots$all_gbd_estimation_years
  years <- all_gbd_estimation_years
  save_results_epi(input_dir = paste0('FILEPATH'),
                   input_file_pattern = '{location_ascii_name}_{location_id}_{measure_id}.csv', 
                   modelable_entity_id = get_core_ref(paste0(measure, '_me_id')),
                   description = db_description,
                   year_id = years,
                   sex_id = c(1, 2), measure_id = measure_ids, metric_id = 3,
                   n_draws = 1000, release_id = release_id,
                  mark_best = mark_as_best, # decomp_step = 'iterative',
                   birth_prevalence = F, bundle_id = 9263, # using asymp bundle id
                   crosswalk_version_id = 35450) # using asymp cv_id
  
  ## --------------------------------------------------------------------- ----
}

## --------------------------------------------------------------------- ----

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
  output_version <- '2022_02_23.02'
  measure <- 'fatigue' # cognitive_mild, cognitive_severe, fatigue, 
                # respiratory_mild, respiratory_moderate, respiratory_severe, 
                # cognitive_mild_fatigue, cognitive_severe_fatigue, cognitive_mild_respiratory_mild, 
                # cognitive_mild_respiratory_moderate, cognitive_mild_respiratory_severe, 
                # cognitive_severe_respiratory_mild, cognitive_severe_respiratory_moderate, 
                # cognitive_severe_respiratory_severe, fatigue_respiratory_mild, fatigue_respiratory_moderate, 
                # fatigue_respiratory_severe, cognitive_mild_fatigue_respiratory_mild, cognitive_mild_fatigue_respiratory_moderate, 
                # cognitive_mild_fatigue_respiratory_severe, cognitive_severe_fatigue_respiratory_mild, 
                # cognitive_severe_fatigue_respiratory_moderate, cognitive_severe_fatigue_respiratory_severe
  
  
  cat(paste0('Saving [', measure, '] results from run [', output_version, 
             ']...\n'))
  cat(paste0('Output located in ', get_core_ref('data_output', 'final'),
             output_version, '/', '\n\n'))
  
  
  # Run main function
  main(output_version, measure)
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
}
## --------------------------------------------------------------------- ----


#.secret_launch_qsub(type = 'save_results', queue = 'all.q', output_version = "2022-02-23.02", duration = 'short')
#.secret_launch_qsub(type = 'save_results', queue = 'all.q', output_version = '2022-02-23.02', duration = 'long')
#.secret_launch_qsub(type = 'save_results', queue = 'all.q', output_version = '2022-02-23.02', duration = 'long', outcome = c('fatigue'))
#.secret_launch_qsub(type = 'save_results', queue = 'all.q', output_version = '2021-09-12.01', duration = 'long', outcome = c('any', 'midmod_any', 'hospital_any', 'icu_any'))
# locs <- c(44973, 44912, 44950, 44914, 44965, 44937, 44960, 44919, 44939, 44934, 50559, 71, 44850, 546, 35511, 35506, 35495, 89, 367, 60133, 4923, 60135, 4910, 4920, 4926, 44717, 44734, 44758, 44747, 44651, 44653, 44674, 44662, 44664, 44655, 44670, 44675, 44660, 44671, 44677, 44667, 44668, 44673, 44659, 44656, 44665, 44657, 44666, 44663, 44765, 44770, 44774, 44764, 44769, 44767, 44771, 44775, 44777, 44776, 44761, 44766, 44762, 44759, 44784, 44789, 44790, 44791, 44782, 44779, 44785, 44781, 44792, 44783, 44786, 44778, 44787, 44788, 44709, 44708, 44703, 44715, 44705, 44704, 44707, 44711, 44712, 44710, 44702, 44713, 44706, 44692, 44684, 44682, 44690, 44679, 44691, 44685, 44681)
#.secret_launch_qsub(type = 'short', queue = 'all.q', output_version = '2022-02-23.02', locs = locs)
#locs <- c(37, 41, 44916, 44925, 44930, 44933, 44941, 44943, 44945, 44948, 44954, 44957, 44961, 44962, 44966, 54)
locs <- c(527, 568, 572)
locs <- c(523:573)
locs <- c(525, 526, 528, 541, 544, 547)
#.secret_launch_qsub(type = 'long', queue = 'all.q', output_version = '2022-02-23.02', locs = locs)
#.secret_launch_qsub(type = 'diagnostics', queue = 'long.q', output_version = '2021-08-01.02')
#save_results_epi(input_dir = 'FILEPATH',
#                 input_file_pattern = '{location_ascii_name}_{location_id}_{measure_id}.csv',
#                 modelable_entity_id = 26816,
#                 description = "Run 2021-06-04.03",
#                 year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022),
#                 sex_id = c(1, 2), measure_id = c(5, 6), metric_id = 3,
#                 n_draws = 1000, gbd_round_id = 7,
#                 decomp_step = 'iterative', mark_best = T,
#                 birth_prevalence = F, bundle_id = 9263, # using asymp bundle id
#                 crosswalk_version_id = 35450) # using asymp cv_id
