#NCC Save Cod

## Empty the environment
rm(list = ls())

## Load functions and packages
source("FILEPATH")
library(data.table)

gbd_round_id <- ADDRESS
decomp_step <- 'step2'
draws_path <- 'FILEPATH'
my_desc <- "correct age restrict, GBD 2020, Decomp 2 Preliminary model, Identical to GBD 2019 final model"

save_results_cod(input_dir = draws_path, 
                 input_file_pattern = "{location_id}.csv", 
                 cause_id = ADDRESS, 
                 description = my_desc , 
                 mark_best = TRUE,
                 gbd_round_id = gbd_round_id,
                 decomp_step = decomp_step)