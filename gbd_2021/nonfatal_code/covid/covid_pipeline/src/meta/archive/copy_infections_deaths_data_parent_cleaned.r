## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: copy_infections_deaths_data_parent.R
## Description: Parent script to launch location-specific copy/paste workers
## Contributors: 
## --------------------------------------------------------------------- ----


## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

source(paste0(.repo_base, 'FILEPATH/utils.R'))

## --------------------------------------------------------------------- ----


## Functions ----------------------------------------------------------- ----

main <- function(version_tag) {
  
  # Set input and output directory bases
  cov_dir <- paste0('FILEPATH', version_tag, 'FILEPATH')
  inf_death_out_dir <- paste0('FILEPATH', 
                              'FILEPATH', version_tag, 'FILEPATH')
  hsp_icu_out_dir <- paste0('FILEPATH', 
                            'FILEPATH', version_tag, 'FILEPATH')
  
  
  # Ensure output directory exists
  .ensure_dir(inf_death_out_dir)
  .ensure_dir(hsp_icu_out_dir)  
  
  # Pull covid team files
  input_files <- list.files(cov_dir, full.names = F, recursive = F)

#  input_files <- list('572.rds', '565.rds', '53597.rds', '53556.rds', '54558.rds', '53552.rds', '53550.rds', '53541.rds')
#  input_files <- list('572.rds')
  
  cat('Launching children\n')
  .ensure_dir(paste0('FILEPATH', Sys.info()['user'], 'FILEPATH'))
  
  for (f in input_files) {
    
    loc_id <- gsub('.rds', '', f)
    
    if (!file.exists(paste0('FILEPATH', version_tag, 'FILEPATH', loc_id, '_prop_asymp.feather')) |
        !file.exists(paste0('FILEPATH', version_tag, 'FILEPATH', loc_id, '_infecs.feather')) |
        !file.exists(paste0('FILEPATH', version_tag, 'FILEPATH', loc_id, '_deaths.feather')) |
        !file.exists(paste0('FILEPATH', version_tag, 'FILEPATH/hospital_admit_', loc_id, '.feather')) |
        !file.exists(paste0('FILEPATH', version_tag, 'FILEPATH/icu_admit_', loc_id, '.feather')) |
        !file.exists(paste0('FILEPATH', version_tag, 'FILEPATH/community_deaths_', loc_id, '.feather'))) {
      qsub <- paste0('qsub -e FILEPATH', Sys.info()['user'], 'FILEPATH/cpy_inf_dth_', loc_id, '_e.txt ',
                     '-o FILEPATH', Sys.info()['user'], 'FILEPATH/cpy_inf_dth_', loc_id, '_o.txt ',
                     '-N cpy_', loc_id, ' -l archive=TRUE -q all.q -P proj_nfrqe ',
                     '-l fthread=20 -l m_mem_free=100G -l h_rt=00:45:00 ',
                     'FILEPATH/execRscript.sh ', 
                     '-i FILEPATH ', 
                     '-s FILEPATH/copy_infections_deaths_data_child.R ',
                     version_tag, ' ', loc_id)
  #    '-m a -M USERNAME@uw.edu ',
      
      system(qsub)
    }
    
    rm(f, loc_id, qsub)
    
  }
  
  
}

## --------------------------------------------------------------------- ----


## Run all ------------------------------------------------------------- ----
if (!interactive()){
  begin_time <- Sys.time()
  
  
  # Command line arguments
  version_tag <- as.character(commandArgs()[8])
  
  
  # Run main function
  main(version_tag)
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
} else {
  begin_time <- Sys.time()
  
  
  # Command line arguments
  version_tag <- '2021_07_12.09'
  # for Lancet submission '2021_06_24.01'
  # for forecasting and final GBD results '2021_07_12.09'
  
  
  # Run main function
  main(version_tag)
  
  
  end_time <- Sys.time()
  cat(paste0('\nExecution time: ', end_time - begin_time, ' sec.\n'))
  
}
## --------------------------------------------------------------------- ----