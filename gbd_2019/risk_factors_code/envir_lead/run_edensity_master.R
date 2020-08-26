##############################################################################################
# Project: RF: Lead Exposure
# Purpose: Launch edensity to calculate IQ shifts
##############################################################################################

rm(list=ls())

# versioning (see lead_backcast_master.R)
output.version <- 1 # gbd17 first versions; initial 2019 run stored here too (oops)
output.version <- 2 # gbd17
output.version <- 3 # 2019 decomp step 1; run with 100 draws & years 1990, 2000, 2017
output.version <- 4 # 2019 decomp step 2
output.version <- 5 # 2019 decomp step 3 (edit 1/3/2020: no changes made in step 4, so this is the final output.version for GBD 2019)

run_id <- 83492 # envir_lead_exp st-gpr run id, update as needed

input_dir <- file.path("FILEPATH")

calc.script <- "SCRIPT"
r.shell <- "SHELL"

files <- list.files(input_dir)

# resources
threads <- 20
memory <- "30G"
runtime <- "10:00:00"

# job settings
project <- " -P ADDRESS"
sge.output.dir <- "FILEPATH"
queue <- "all.q"

# parameters
years <- 1990:2019
draws <- 1000

## launch jobs
## parallelize by year & location when running for all years (otherwise will take waaay too much memory)
for (year in years) {
  for (file in files) {
    jname <- paste0("edensity_",run_id,"_loc_",gsub(".csv", "", file),"_yr_",year)
    sys.sub <- paste0("qsub -N ", jname, project, sge.output.dir, " -l fthread=", threads, " -l m_mem_free=", memory, " -l h_rt=", runtime, " -l archive=TRUE -q ", queue)
    args <- paste(file, output.version, run_id, threads, year, draws)
    
    job <- paste(sys.sub, r.shell, calc.script, args)
    system(job)
    print(job)
  }
}

## END
