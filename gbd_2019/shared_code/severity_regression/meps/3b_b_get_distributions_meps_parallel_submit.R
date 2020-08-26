########################################################################
### Project:  MEPS Severity Analysis
### Purpose:  Submits 3b_b code that calculates severity proportions
###           by cause.
########################################################################

library(data.table)

## set path - use all relative paths from here on

  j <- 'FILEPATH'

# set up parallelization
slots<- 4
mem <- slots*2
shell <- paste0(j, "FILEPATH.sh")
script <- paste0(j, "FILEPATH/3b_c_get_distributions_meps_parallel_do.R")
project <- '-P PROJECT '
sge_output_dir <- '-o /FILEPATH -e /FILEPATH '

## get list of bootstrapped files completed
filelist_boot <- list.files(path=paste0(j,"FILEPATH/"), pattern = "csv", full.names = F)
boot_completed_causes <- substring(filelist_boot, 2, nchar(filelist_boot)-4)


for (cause in boot_completed_causes){
  job_name<- paste0('-N ',cause)
  sys_sub<- paste0('qsub -cwd ', project, sge_output_dir, job_name, ' -pe multi_slot ',slots, ' -l mem_free=', mem, 'G')
  system(paste(sys_sub, shell, script, cause))
  print(paste(sys_sub, shell, script, cause))
}

print("All causes submitted")



