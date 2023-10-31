library(R.utils)

source("/FILEPATH/get_demographics.R")

user <- Sys.getenv("LOGNAME")
code.dir <- "/FILEPATH/"

#slots <- 1
decomp_step <- 'iterative'
gbd_round <- 7
demographics <- get_demographics(gbd_team="epi")
locations <- unlist(demographics$location_id, use.names=F)

n_jobs <- length(locations)

## Save the parameters as a csv so then you can index the rows to find the appropriate parameters
param_map <- expand.grid(location_id = locations)
write.csv(param_map, paste0("/FILEPATH/01_asymp_adjust_params_", decomp_step, ".csv"), row.names=F)

## Step 1: Submit jobs as an array
rshell <- "/FILEPATH/execRscript.sh -s"
rscript <- paste0(code.dir, "01_asymp_adjust.R")

code_command <- paste0(rshell, " ", rscript, " /FILEPATH/01_asymp_adjust_params_iterative.csv ", decomp_step)

threads <- 4
memory <- "1.0G"
time <- "00:12:00"
njobs <- length(locations)
jobname <- "ihd_asymp_adjust"
errors <- paste0(" -o /FILEPATH/ -e /FILEPATH/")
#sys.sub <- paste0("qsub -P proj_cvd ", errors , " -N ", jobname, " ", "-pe multi_slot ", slots)
sys.sub <- paste0("qsub -P proj_cvd ", errors , " -cwd -N ", jobname, " -l fthread=", threads,
                  " -l m_mem_free=", memory, " -l h_rt=", time, " -q all.q -l archive=TRUE", ' -t 1:',njobs )


full_command <- paste(sys.sub, code_command)
print(full_command)
system(full_command)

## Step 2: Submit 1 job to save adjusted results post-MI IHD model
rscript <- paste0(code.dir, "02_asymp_save.R")
code_command <- paste0(rshell, " ", rscript, " ", decomp_step)

threads2 <- 20
memory2 <- "100G"
time2 <- "03:00:00"
jobname2 <- "asymp_save"
sys.sub <- paste0("qsub -P proj_cvd ", errors , " -cwd -N ", jobname2, " -l fthread=", threads2,
                  " -l m_mem_free=", memory2, " -l h_rt=", time2, " -q all.q -hold_jid ihd_asymp_adjust ")

full_command <- paste0(sys.sub, code_command)

print(full_command)
system(full_command)

## Step 3: Submit 1 job holding on all step 2 jobs
rscript <- paste0(code.dir, "03_asymp_finalizer.R")
code_command <- paste0(rshell, " ", rscript)


full_command <- paste0("qsub -l m_mem_free=0.1G -l fthread=1 -l h_rt=00:10:00 -q all.q -P proj_cvd -N asymp_finalize ",
                       "-hold_jid asymp_save ",
                       "-o /FILEPATH ",
                       "-e /FILEPATH ",
                       code_command)



print(full_command)
system(full_command)

