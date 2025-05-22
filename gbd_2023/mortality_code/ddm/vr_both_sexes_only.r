
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
parser$add_argument('--with_shock', type="integer", required=TRUE,
                    help='Dummy for with shock')
args <- parser$parse_args()
version_id <- args$version_id
with_shock <- args$with_shock

main_dir <- paste0("FILEPATH")

input_file <- ifelse(
  with_shock == 1,
  paste0("FILEPATH"),
  paste0("FILEPATH")
)
deaths <- data.table(read_dta(input_file))

deaths <- deaths[, list(ihme_loc_id, year, sex, source_type)]

deaths[, n := seq(.N), by = c('ihme_loc_id', 'year', 'source_type')]
deaths[, maximum := max(n), by = c('ihme_loc_id', 'year', 'source_type')]
vr_both_sex_only <- deaths[maximum != 3, list(ihme_loc_id, year, sex, source_type)]

# Verify that the loc-year-source combinations with less than 3 observations are observations that are both sex only
vr_both_sex_only[, available_sexes := lapply(.SD, function(x) paste(x, collapse = ',')), .SDcols = c('sex'), by = c('ihme_loc_id', 'year', 'source_type')]
vr_both_sex_only <- vr_both_sex_only[available_sexes == "both"]
vr_both_sex_only <- vr_both_sex_only[, c('sex', 'available_sexes') := NULL]

output_file <- ifelse(
  with_shock == 1,
  paste0("FILEPATH"),
  paste0("FILEPATH")
)
write_csv(vr_both_sex_only, output_file)
