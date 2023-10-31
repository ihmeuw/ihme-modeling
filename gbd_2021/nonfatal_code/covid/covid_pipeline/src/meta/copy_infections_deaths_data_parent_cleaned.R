## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: copy_infections_deaths_data_parent.R
## Description: Parent script to launch location-specific copy/paste workers
## Contributors:
## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

# Init NF COVID repo
if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}
.nf_repo <- 'FILEPATH'

source(paste0('FILEPATH'))

## --------------------------------------------------------------------- ----
source(paste0(roots$'ROOT', 'FILEPATH/get_location_metadata.R'))

## Functions ----------------------------------------------------------- ----

main <- function(version_tag, location_set) {
#  version_tag_save <- paste0(version_tag, '.2')
  # Set input and output directory bases
  version_tag_save <- version_tag
  
  cov_dir <- paste0('FILEPATH')
  inf_death_out_dir <- paste0('FILEPATH')
  hsp_icu_out_dir <- paste0('FILEPATH')
  
  
  # Ensure output directory exists
  .ensure_dir(inf_death_out_dir)
  .ensure_dir(hsp_icu_out_dir)  
  
  # Pull covid team files
#  input_files <- list.files('FILEPATH', full.names = F, recursive = F)
  
  locations <- get_location_metadata(location_set_id = location_set, release_id = 9)
  locs <- locations$location_id[locations$most_detailed==1]

  cat('Launching children\n')
  #.ensure_dir(paste0('FILEPATH'))
  
  for (loc_id in locs) {
    cat(loc_id)
    qsub <- paste0('FILEPATH', loc_id, '_e.txt ',
    '[QSUB COMMANDS]',
    version_tag, ' ', loc_id)
    system(qsub)

    rm(loc_id, qsub)
  }

} 
## --------------------------------------------------------------------- ----


## Run all ------------------------------------------------------------- ----
if (!interactive()){
  begin_time <- Sys.time()
  
  
  # Command line arguments
  #version_tag <- as.character(commandArgs()[8])
  version_tag <- '2022_04_27.02'
  location_set <- 39
  
  # Run main function
  main(version_tag, location_set)
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
} else {
  begin_time <- Sys.time()
  
  
  # Command line arguments
#  version_tag <- '2021_07_12.09'
#  version_tag <- '2021_07_14.11'
#  version_tag <- '2021_07_26.02'
#  version_tag <- '2022_02_23.02'
#  version_tag <- '2022_03_29.06'
  version_tag <- '2022_04_27.02'
  location_set <- 39
  
  
  # Run main function
  main(version_tag, location_set)
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
}
## --------------------------------------------------------------------- ----