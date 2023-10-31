##############################
## Purpose: Calculate TFR
## Details: Use age-specific estimates to calculate TFR
###############################

library(data.table)
library(mortdb, lib = 'FILEPATH')
library(mortcore, lib = 'FILEPATH')
rm(list=ls())

if (interactive()) {
  user <- Sys.getenv('USER')
  version_id <- x
  gbd_year <- x
  year_start <- x
  year_end <- x
  loop <- x
  loc <- ''
} else {
  user <- Sys.getenv('USER')
  parser <- argparse::ArgumentParser()
  parser$add_argument('--version_id', type = 'character')
  parser$add_argument('--gbd_year', type = 'integer')
  parser$add_argument('--year_start', type = 'integer')
  parser$add_argument('--year_end', type = 'integer')
  parser$add_argument('--loop', type = 'integer')
  parser$add_argument('--loc', type = 'character')
  args <- parser$parse_args()
  version_id <- args$version_id
  gbd_year <- args$gbd_year
  year_start <- args$year_start
  year_end <- args$year_end
  loop <- args$loop
  loc <- args$loc
}

input_dir <- paste0("FILEPATH")
gpr_dir <- paste0("FILEPATH")


tfr_draws <- assertable::import_files(list.files(gpr_dir, full.names = T, 
                                                 pattern = paste0("FILEPATH")), 
                                      FUN=fread)
tfr_draws <- tfr_draws[, tfr := 5*sum(fert), by=c('sim', 'year')]

tfr_summary <- tfr_draws[, .(mean = mean(tfr), lower = quantile(tfr, 0.025), 
                             upper = quantile(tfr, 0.975)), 
                         by=c('ihme_loc_id', 'year')][,age := 'tfr']

readr::write_csv(tfr_draws, paste0("FILEPATH"))
readr::write_csv(tfr_summary, paste0("FILEPATH"))
