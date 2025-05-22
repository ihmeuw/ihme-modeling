## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: copy_infections_deaths_data_parent.R
## Description: Parent script to launch location-specific copy/paste workers
## Contributors: NAME, NAME, NAME
## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----

# Dynamically set user specific repo base where nf_covid is located
.repo_base <-
  strsplit(
    whereami::whereami(path_expand = TRUE),
    "nf_covid"
  )[[1]][1]

source(file.path(.repo_base, 'FILEPATH/utils.R'))

## Functions ----------------------------------------------------------- ----
## --------------------------------------------------------------------- ----
source(paste0(roots$k, 'FILEPATH/get_location_metadata.R'))

## Functions ----------------------------------------------------------- ----
main <- function(version_tag, loc_set_id) {

  # Set input and output directory bases
  version_tag_save <- version_tag
  
  out_dir <- file.path(
    'FILEPATH', version_tag_save)
  
  # Ensure output directory exists
  .ensure_dir(out_dir)

  locations <- get_location_metadata(location_set_id = loc_set_id, release_id = 16)
  locs <- locations$location_id[locations$most_detailed==1]
  
  cat('Launching children\n')
  .ensure_dir(paste0('FILEPATH', Sys.info()['user'], 'FILEPATH'))
  
  error_logs <- paste0("FILEPATH", Sys.info()["user"], "FILEPATH")
  output_logs <- paste0("FILEPATH", Sys.info()["user"], "FILEPATH")
  
  for (loc_id in locs) {
    cat(loc_id)
    
    command <- paste0(
      "sbatch --mem=5G -c 1 -t 01:00:00 -C archive -D ./",
      " -p ", "all.q",
      " -A ", "proj_nfrqe",
      " -o ", file.path(output_logs, paste0("prep_input_files_", loc_id, "_o.txt")),
      " -e ", file.path(error_logs, paste0("prep_input_files_", loc_id, "_e.txt")),
      " -J ", paste0("prep_input_files_", loc_id),
      " FILEPATH/execRscript.sh",
      " -i FILEPATH/latest.img",
      " -s ", file.path(.nf_repo, "src/meta/copy_infections_deaths_data_child.R "),
      " --version_tag ", version_tag,
      " --loc_id ", loc_id
    )
    
    system(command)
    
    rm(loc_id, command)
  }
  
} 
## --------------------------------------------------------------------- ----


## Run all ------------------------------------------------------------- ----
if (!interactive()){
  begin_time <- Sys.time()
  
  
  # Command line arguments
  #version_tag <- as.character(commandArgs()[8])
  version_tag <- '2024_09_06.01'
  loc_set_id <- 35
  
  # Run main function
  main(version_tag, loc_set_id)
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
} else {
  begin_time <- Sys.time()
  
  
  # Command line arguments
  #  version_tag <- '2021_07_12.09'
  #  version_tag <- '2021_07_14.11'
  #  version_tag <- '2021_07_26.02'
  #  version_tag <- '2022_02_23.02'
  #  version_tag <- '2022_03_29.03'
  #  version_tag <- '2022_04_01.01'
  #  version_tag <- '2022_04_07.01'
  version_tag <- '2024_09_06.01'
  loc_set_id <- 35
  # 39 for FHS, 35 for GBD
  
  
  # for Lancet submission '2021_06_24.01'
  # for forecasting and final GBD results out to 2022 '2021_07_14.11'
  # for forecasting out to 2023 '2021_07_26.02'
  # for interim results to test pipeline updates and compare newer results to previous run (waiting on final mortality scalars) '2022_02_23.04'
  # for final GBD results and JAMA submission '2022_04_01.01' (2022_03_29.03 has issues with deaths in 4 locs)
  # for early FHS results '2022_04_05.01'
  # for final FHS results '2022_07_12.01'
  
  # Run main function
  main(version_tag, loc_set_id)
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
}
## --------------------------------------------------------------------- ----
