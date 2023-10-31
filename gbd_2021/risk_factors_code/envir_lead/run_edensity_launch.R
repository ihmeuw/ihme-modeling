##############################################################################################
# Project: RF: Lead Exposure
# Purpose: Launch edensity to calculate IQ shifts
##############################################################################################

rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  jpath <- "J:/"
  hpath <- "H:/"
} else {
  jpath <- "FILEPATH"
  hpath <- "FILEPATH"
}

## libraries
library(data.table)
library(magrittr)

## versioning
run_id <- NUMBER_ID # envir_lead_exp st-gpr run id, update as needed

# input version (update to current best backcast version - see lead_backcast_launch.R)
input_version <- VERSION_NUMBER # GBD 2020 final

# output version; usually this is the same as the input version - unless we need to re-run ONLY the edensity script (and not the backcast
  # script) for whatever reason, in which case we will use the same input version but a new output version
rerun <- T # set to T if we are using the same input version (i.e. the same backcast output) but new output version; F otherwise
rerun_version <- 15 # saving all years 1990-2022 (associated input_version is 13)
if (rerun) {
  output_version <- rerun_version
} else {
  output_version <- input_version
}

input_dir <- file.path("FILEPATH", input_version, run_id, "FILEPATH")

calc_script <- "FILEPATH/run_edensity_calc.R"
r_shell <- "FILEPATHs/execRscript.sh -s"

files <- list.files(input_dir)

# resources
threads <- 20
memory <- "70G"
runtime <- "24:00:00"

# job settings
project <- " -P proj_erf "
sge_dir <- "-o FILEPATH -e FILEPATH"
queue <- "long.q"

# parameters
years <- 1990:2022
draws <- 1000

## launch jobs
## parallelize by year & location
parameters <- expand.grid(year_id = years, file = files) %>% data.table
write.csv(parameters, "FILEPATH/edensity_parameters.csv", row.names = F) # this will be read in in the calc script to get the year and file

jname <- paste0("lead_edensity_", run_id)
sys_sub <- paste0("qsub -N ", jname, project, sge_dir, " -l fthread=", threads, " -l m_mem_free=", memory,
                  " -l h_rt=", runtime, " -l archive=TRUE -q ", queue, " -t 1:", nrow(parameters), " -tc 500") # -tc flag limits to 500 concurrent tasks
args <- paste(input_version, output_version, run_id, threads, draws)

job <- paste(sys_sub, r_shell, calc_script, args)
system(job)

## END
