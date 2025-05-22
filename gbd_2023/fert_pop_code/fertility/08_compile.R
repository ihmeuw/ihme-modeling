##############################
## Purpose: Compiling
## Details: Compile stage1, stage2, and gpr results
###############################

library(data.table)
library(readr)
library(assertable)
library(mortcore)
library(mortdb)

rm(list=ls())

if (interactive()){
  user <- "USERNAME"
  version_id <- "Run id"
  loop <- 2
} else {
  user <- "USERNAME"
  parser <- argparse::ArgumentParser()
  parser$add_argument('--version_id', type = 'integer')
  parser$add_argument('--loop', type = 'integer')
  args <- parser$parse_args()
  version_id <- args$version_id
  loop <- args$loop
}

## Compile results
input_dir <- "FILEPATH"
stage1_dir <- "FILEPATH"
stage2_dir <- "FILEPATH"
gpr_dir <- "FILEPATH"

compiled_data <- assertable::import_files(list.files(input_dir, 
                             full.names = T, 
                             pattern = 'age_.[0-9]_data.csv'), 
                             FUN=fread)
compiled_stage1 <- assertable::import_files(list.files(stage1_dir, 
                               full.names = T, 
                               pattern = 'age_.[0-9]_stage1_results.csv'), 
                               FUN=fread)
compiled_stage2 <- assertable::import_files(list.files(stage2_dir, 
                               full.names = T, 
                               pattern = 'age_.[0-9]_stage2_results.csv'), 
                               FUN=fread)
compiled_gpr <- assertable::import_files(list.files(gpr_dir, 
                            full.names = T, 
                            pattern = 'gpr_[A-Z]{3}[_0-9A-Z]*.csv'), 
                            FUN=fread)

compiled_gpr[is.na(year), year := year_id + 0.5]
compiled_gpr[,c('V1', 'year_id') := NULL]

readr::write_csv(compiled_data, paste0(input_dir, '/compiled_input_data.csv'))
readr::write_csv(compiled_stage1, paste0(stage1_dir, '/compiled_stage1_results.csv'))
readr::write_csv(compiled_stage2, paste0(stage2_dir, '/compiled_stage2_results.csv'))
readr::write_csv(compiled_gpr, paste0(gpr_dir, '/compiled_gpr_results.csv'))

mortdb::send_slack_message(
  message=paste0('*Loop 2 finished: version ', version_id, '*'), 
  channel='#f', 
  botname='FertBot', 
  icon=':hatched_chick:'
)
