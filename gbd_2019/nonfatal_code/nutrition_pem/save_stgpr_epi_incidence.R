# rm(list=ls())


# --------------

Sys.umask(mode = 002)

os <- .Platform$OS.type
my_libs <- "FILEPATH"

library(data.table)
library(magrittr)
library(ggplot2)


# source scripts
source("FILEPATH"  )


## Move to parallel script
## Getting normal QSub arguments
args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- ifelse(is.na(args[1]), "FILEPATH", args[1])

## Retrieving array task_id
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
param_map <- fread(param_map_filepath)

input_dir <- param_map[task_id, input_dir]
input_file_pattern <- param_map[task_id, input_file_pattern]
me <- param_map[task_id, me]


###########################################################################################

save_results_epi(input_dir = input_dir, 
                 input_file_pattern = input_file_pattern, 
                 modelable_entity_id = me, description="GBD19 Final Results", 
                 measure_id = c(5,6), 
                 decomp_step = "step4", 
                 birth_prevalence = T,
                 mark_best = T)
