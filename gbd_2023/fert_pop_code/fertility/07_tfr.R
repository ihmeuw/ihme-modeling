##############################
## Purpose: Calculate TFR
## Details: Use age-specific estimates to calculate TFR
###############################

library(data.table)
library(mortdb)
library(mortcore)
rm(list=ls())

if (interactive()) {
  user <- "USERNAME"
  version_id <- "Run id"
  gbd_year <- 2020
  year_start <- 1950
  year_end <- 2022
  loop <- 2
  loc <- 'input location'
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

input_dir <- "FILEPATH"
gpr_dir <- "FILEPATH"

tfr_draws <- assertable::import_files(list.files(gpr_dir, full.names = T, 
                                                 pattern = paste0('gpr_', loc, '_[0-9]*_sim.csv')), 
                                      FUN=fread)
tfr_draws <- tfr_draws[, tfr := 5*sum(fert), by=c('sim', 'year')]

tfr_summary <- tfr_draws[, .(mean = mean(tfr), lower = quantile(tfr, 0.025), 
                             upper = quantile(tfr, 0.975)), 
                         by=c('ihme_loc_id', 'year')][,age := 'tfr']

readr::write_csv(tfr_draws, paste0(gpr_dir, '/gpr_', loc, '_TFR_sim.csv'))
readr::write_csv(tfr_summary, paste0(gpr_dir, '/gpr_', loc, '_TFR.csv'))
