### Fit testing per capita to prevalence

## Setup
Sys.umask("0002")
Sys.setenv(MKL_VERBOSE = 0)
suppressMessages(library(data.table))
setDTthreads(1)
library(lme4)

## install ihme.covid (always use newest version)
tmpinstall <- system(SYSTEM_COMMAND)
.libPaths(c(tmpinstall, .libPaths()))
devtools::install_github("ihmeuw/ihme.covid", upgrade = "never", quiet = T)
##

## Arguments
if (interactive()) {
  user <- Sys.info()["user"]
  code_dir <- file.path("FILEPATH", user, "covid-beta-inputs/")
  
  release <- "latest"
  lsvid <- 681
  output_dir <- file.path("FILEPATH", release)
  prev_version <- "2020_05_20.02"
  prev_dir <- file.path("FILEPATH", prev_version)
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument("--lsvid", type = "integer", required = TRUE, help = "location set version id to use")
INTERNAL_COMMENT)
  parser$add_argument("--seir-covariates-version", default = "best", help = "Version of seir-covariates. Defaults to 'best'. Pass a full YYYY_MM_DD.VV or 'latest' if you provide")
  args <- parser$parse_args()
  
  code_dir <- ihme.covid::get_script_dir()
  
  lsvid <- args$lsvid
  output_dir <- args$outputs_directory
  seir_covariates_version <- args$seir_covariates_version
  release <- basename(output_dir)
}

## Paths
input_test_path <- file.path(output_dir, "data_smooth.csv")
input_prev_path <- file.path(prev_dir, "infec_sero_prev_summ.csv")
out_data_path <- file.path(output_dir, "adjusted_testing.csv")
plot_path <- file.path(output_dir, "prev_residuals.pdf")

## Functions
source(file.path("FILEPATH/get_location_metadata.R"))
source(file.path(code_dir, "FILEPATH/forecast_all_locs.R"))
source(file.path(code_dir, "FILEPATH/diagnostics.R"))
source(file.path(code_dir, "utils.R"))

## Tables
hierarchy <- get_location_metadata(location_set_id = 111, location_set_version_id = lsvid)
pop <- get_populations(hierarchy)

## Prep data
message("Prepping data...")
# Testing
test_dt <- fread(input_test_path)
test_dt[, date := as.Date(date)]
test_dt[, test_pc := daily_total / pop * 1e5]
test_dt <- impute_children(test_dt, hierarchy)
# Prevalence
prev_dt <- fread(input_prev_path)
prev_dt[, date := as.Date(date)]
# Combine
dt <- merge(test_dt[!is.na(test_pc), .(location_id, location_name, date, test_pc, pop)], prev_dt[, .(location_id, date, prev_mean)], by = c("location_id", "date"))
dt[, pop := NULL]
dt <- merge(dt, pop, "location_id")
dt[, prev := prev_mean / pop * 1e4]
min_dt <- dt[, .(min_date = min(date)), by = location_id]
dt <- merge(dt, min_dt)
dt[, days := date - min_date]

## Fit models
message("Fitting models...")
# Simple linear model
fit1 <- lm(test_pc ~ 1 + prev, dt)
dt[, prev_coef_fe := coef(fit1)[2]]
# Mixed model
fit2 <- lmer(test_pc ~ 1 + prev + (1 + prev | location_id), dt)
coef_dt <- as.data.table(coef(fit2)$location_id, keep.rownames = T)
setnames(coef_dt, c("rn", "prev"), c("location_id", "prev_coef_re"))
coef_dt[, location_id := as.integer(location_id)]
dt <- merge(dt, coef_dt[, .(location_id, prev_coef_re)])

## Predict residual after removing prevalence effect
dt[, resid_fe := test_pc - prev * prev_coef_fe]
dt[, resid_re := test_pc - prev * prev_coef_re]
f_dt1 <- forecast_all_locs(dt[, .(location_name, location_id, date, test_pc, resid_fe, pop)], "resid_fe")
f_dt2 <- forecast_all_locs(dt[, .(location_name, location_id, date, resid_re, pop)], "resid_re")
dt <- merge(f_dt1, f_dt2)

# Check for missing locations
missing_locs <- setdiff(hierarchy[most_detailed == 1]$location_id, unique(dt$location_id))
if (length(missing_locs) > 0) {
  message(paste0("Missing: ", paste(hierarchy[location_id %in% missing_locs]$location_name, collapse = ", ")))
}

# Pick the winner and save
message("Saving the mixed model results...")
dt[, testing_adj := fcast_resid_re]
out_dt <- dt[, .(location_name, location_id, date, testing_adj, test_pc, fcast_resid_fe, fcast_resid_re)]
write.csv(out_dt, out_data_path, row.names = F)

## Plot
message("Plotting...")
plot_prev_adj(out_data_path, hierarchy, plot_path)

message(paste("Done! Output directory:", output_dir)) 