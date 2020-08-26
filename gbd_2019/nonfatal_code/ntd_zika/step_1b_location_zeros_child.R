### ======================= BOILERPLATE ======================= ###
rm(list = ls())

os <- .Platform$OS.type
if (os=="windows") {
  ADDRESS <-"FILEPATH"
} else {
  ADDRESS <-"FILEPATH"
}

library(data.table)
library(stringr)
library(argparse)
source("FILEPATH"))

## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--params_dir", type = "character")
parser$add_argument("--draws_dir", type = "character")
parser$add_argument("--interms_dir", type = "character")
parser$add_argument("--logs_dir", type = "character")
parser$add_argument("--location_id", type = "character")
args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

### ======================= MAIN ======================= ###
zero_draws <- gen_zero_draws(ADDRESS, location_id, measure_id = c(5, 6))
birth_prev <- copy(zero_draws[age_group_id == ADDRESS1 & measure_id %in% c(5, 6)])
birth_prev[, age_group_id := ADDRESS2]
zero_draws <- rbind(zero_draws, birth_prev)
write.csv(zero_draws, paste0("FILEPATH"), row.names=FALSE)