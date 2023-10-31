##############################
## Purpose: Compiling
## Details: Compile stage1, stage2, and gpr results
###############################

library(data.table)
library(readr)
library(assertable)
library(mortcore, lib = 'FILEPATH')
library(mortdb, lib = 'FILEPATH')

rm(list=ls())

if (interactive()){
  user <- Sys.getenv('USER')
  version_id <- x
  loop <- x
} else {
  user <- Sys.getenv('USER')
  parser <- argparse::ArgumentParser()
  parser$add_argument('--version_id', type = 'integer')
  parser$add_argument('--loop', type = 'integer')
  args <- parser$parse_args()
  version_id <- args$version_id
  loop <- args$loop
}

## Compile results
input_dir <- paste0("FILEPATH")
stage1_dir <- paste0("FILEPATH")
stage2_dir <- paste0("FILEPATH")
gpr_dir <- paste0("FILEPATH")

compiled_data <- assertable::import_files(list.files(input_dir, 
                             full.names = T, 
                             pattern = "FILEPATH"), 
                             FUN=fread)
compiled_stage1 <- assertable::import_files(list.files(stage1_dir, 
                               full.names = T, 
                               pattern = "FILEPATH"), 
                               FUN=fread)
compiled_stage2 <- assertable::import_files(list.files(stage2_dir, 
                               full.names = T, 
                               pattern = "FILEPATH"), 
                               FUN=fread)
compiled_gpr <- assertable::import_files(list.files(gpr_dir, 
                            full.names = T, 
                            pattern = "FILEPATH"), 
                            FUN=fread)

compiled_gpr[is.na(year), year := year_id + 0.5]
compiled_gpr[,c('V1', 'year_id') := NULL]

readr::write_csv(compiled_data, paste0("FILEPATH"))
readr::write_csv(compiled_stage1, paste0("FILEPATH"))
readr::write_csv(compiled_stage2, paste0("FILEPATH"))
readr::write_csv(compiled_gpr, paste0("FILEPATH"))

mortdb::send_slack_message(
  message=paste0('*Loop 2 finished: version ', version_id, '*'), 
  channel='#f', 
  botname='FertBot', 
  icon=':hatched_chick:'
)
