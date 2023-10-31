### Prep data for ST-GPR

## Setup
user <- Sys.info()["user"]
library(data.table)
library(lme4)
library(ggplot2)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
## Arguments
if (interactive()) {
  release <- "2020_05_11.10"
  path <- file.path("FILEPATH", release)
  lsvid <- 664
  snapshot_version <- "best"
  model_inputs_version <- "best"
  rake <- F
  use_INDIVIDUAL_NAME_smooth <- F
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument("--lsvid", type = "integer", required = TRUE, help = "location set version id to use")
INTERNAL_COMMENT)
  parser$add_argument("--rake", action = "store_true", default = FALSE, help = "Enable raking (off by default)")
  parser$add_argument("--snapshot-version", default = "best", help = "Version of snapshot. Defaults to 'best'. Pass a full YYYY_MM_DD.VV or 'latest' if you provide")
  parser$add_argument("--model-inputs-version", default = "best", help = "Version of model-inputs. Defaults to 'best'. Pass a full YYYY_MM_DD.VV or 'latest' if you provide")
  parser$add_argument("--use-INDIVIDUAL_NAME-smooth", action = "store_true", default = FALSE, help = "Enable INDIVIDUAL_NAME smooth (off by default)")
  args <- parser$parse_args()

  lsvid <- args$lsvid
  path <- args$outputs_directory
  rake <- args$rake
  snapshot_version <- args$snapshot_version
  model_inputs_version <- args$model_inputs_version
  use_INDIVIDUAL_NAME_smooth <- args$use_INDIVIDUAL_NAME_smooth
}


## Paths
dir <- file.path("FILEPATH", run_date, ".", run_version)
dt_path <- file.path(dir, "data_smooth.csv")
out_path <- file.path(dir, "st_gpr_input.csv")
code_dir <- file.path("FILEPATH", user, "FILEPATH")

## Functions
source(file.path("FILEPATH/get_location_metadata.R"))

## Tables
# hierarchy <- get_location_metadata(location_set_id = 111, location_set_version_id = lsvid)
source(file.path("FILEPATH/get_covariate_estimates.R"))
sdi_dt <- get_covariate_estimates(
  covariate_id = 881,
  age_group_id = 22,
  location_id = -1,
  year_id = 2020,
  sex_id = 3,
  gbd_round_id = 7,
  decomp_step = "step3",
  status = "best"
)

## Code
# Prep data
dt <- fread(dt_path)
dt[, date := as.Date(date)]
dt <- merge(dt, sdi_dt[, .(location_id, mean_value)], all.x = T)
dt[, sex_id := 3]
dt[, age_group_id := 22]
setnames(dt, c("daily_total", "mean_value"), c("val", "cv_sdi"))
dt[, variance := sd(diff(val), na.rm = T), by = location_id]
out_dt <- dt[, .(date, location_id, sex_id, age_group_id, val, cv_sdi, variance)]
out_dt <- out_dt[!is.na(val)]

write.csv(out_dt, out_path, row.names = F)
