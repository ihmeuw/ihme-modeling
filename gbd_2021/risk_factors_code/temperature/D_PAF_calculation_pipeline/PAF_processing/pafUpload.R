## LOAD DEPENDENCIES -----------------------------------------------------
source("/FILEPATH/save_results_risk.R")
library(argparse)


## IMPORT ARGUMENTS ------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--paf_version", help = "string giving paf version",
                    default = '15.0', type = "character")
parser$add_argument("--outDir", help = "directory to pull data from and write it to",
                    default = "/FILEPATH/", type = "character")
parser$add_argument("--year_start", help = "first of years to evaluate",
                    default = 1990, type = "integer")
parser$add_argument("--year_end", help = "last of years to evaluate",
                    default = 2022, type = "integer")
parser$add_argument("--release_id", help = "release ID to upload for",
                    default = 10, type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)


# Switch release 10 to 9 so save_results_risk can pull correct gbd_round_id
if (release_id == 10) release_id = 9


input_dir.heat <- paste0(outDir, "version_", paf_version, "/heat")
input_dir.cold <- paste0(outDir, "version_", paf_version, "/cold")
mei.heat       <- 20263
mei.cold       <- 20262
years          <- year_start:year_end

#paf_version <- '16.0: As 15.0 but with 2022 added'

save_results_risk(input_dir = input_dir.heat,
                  input_file_pattern = "{location_id}_{year_id}.csv",
                  modelable_entity_id = mei.heat,
                  year_id = years,
                  n_draws=1000,
                  description = paste0("HEAT PAFs: ", paf_version),
                  risk_type = "paf",
                  measure=4,
                  mark_best=T,
                  release_id=release_id)

save_results_risk(input_dir = input_dir.cold,
                  input_file_pattern = "{location_id}_{year_id}.csv",
                  modelable_entity_id = mei.cold,
                  year_id = years,
                  n_draws=1000,
                  description = paste0("COLD PAFs: ", paf_version),
                  risk_type = "paf",
                  measure=4,
                  mark_best=T,
                  release_id=release_id)
