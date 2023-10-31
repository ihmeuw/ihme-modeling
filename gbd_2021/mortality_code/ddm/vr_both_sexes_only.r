# Purpose: 1) Prep VR data for graphing purposes

rm(list=ls())
library(data.table); library(haven); library(argparse); library(readr);
library(readstata13); library(assertable); library(plyr); library(DBI);
library(mortdb);
library(mortcore)
library(ggplot2)
if (Sys.info()[1] == "Linux") {
  root <- "FILEPATH"
  user <- Sys.getenv("USER")
} else {
  root <- "FILEPATH"
  user <- Sys.getenv("USERNAME")
  code_dir <- paste0("FILEPATH")
}

parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of DDM')
args <- parser$parse_args()
version_id <- args$version_id

main_dir <- paste0("FILEPATH")
deaths <- data.table(read_dta(paste0("FILEPATH")))

deaths <- deaths[, list(ihme_loc_id, year, sex, source_type)]

deaths[, n := seq(.N), by = c('ihme_loc_id', 'year', 'source_type')]
deaths[, maximum := max(n), by = c('ihme_loc_id', 'year', 'source_type')]
vr_both_sex_only <- deaths[maximum != 3, list(ihme_loc_id, year, sex, source_type)]

# Verify that the loc-year-source combinations with less than 3 observations are observations that are both sex only (i.e. no both/male, both/females, males/females)
vr_both_sex_only[, available_sexes := lapply(.SD, function(x) paste(x, collapse = ',')), .SDcols = c('sex'), by = c('ihme_loc_id', 'year', 'source_type')]
vr_both_sex_only <- vr_both_sex_only[available_sexes == "both"]
vr_both_sex_only <- vr_both_sex_only[, c('sex', 'available_sexes') := NULL]

write_csv(vr_both_sex_only, paste0("FILEPATH"))

# DONE
