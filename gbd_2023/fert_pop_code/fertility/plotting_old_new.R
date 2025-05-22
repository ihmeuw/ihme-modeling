#################################
## Purpose: Prep data and estimates for plotting
## Details: Reads in data and model outputs for 3 versions
##          Saves data, parameters, and adjustments
##          Outputs get used by plot_loc_specific_old_new
#################################

rm(list = ls())

library(argparse)
library(data.table)
library(ggplot2)
library(RColorBrewer)
library(haven)
library(mortcore)
library(mortdb)
library(stringr)
library(readr)

if (interactive()) {
  username <- Sys.getenv("USER")
  root <- "/home/j/"
  version1 <- "Recent run id"
  version2 <- "Bested run id"
  version3 <- "Bested run id: previous GBD year"
  loop <- 2
  gbd_year <- 2022
} else {
  username <- Sys.getenv("USER")
  root <- "/home/j/"
  parser <- argparse::ArgumentParser()
  parser$add_argument("--loop", type = "integer")
  parser$add_argument("--version1", type = "integer")
  parser$add_argument("--version2", type = "integer")
  parser$add_argument("--version3", type = "integer")
  parser$add_argument("--gbd_year", type = "integer")
  args <- parser$parse_args()
  loop <- args$loop
  version1 <- args$version1
  version2 <- args$version2
  version3 <- args$version3
  gbd_year <- args$gbd_year
}

## Notify channel estimation is complete
mortdb::send_slack_message(
  message = paste0(
    "Fertility estimation finished \n",
    "Version: ", version1
  ),
  channel = "#fertility",
  botname = "StorkBot",
  icon = "https://www.boswells.co.uk/images/products/large/38961.jpg",
  url = T
)

## global options
plot_young_old <- T

## setting directories
loc_map <- get_locations(level = "all", gbd_year = gbd_year)[, .(
  ihme_loc_id,
  location_name,
  level, parent_id
)]
wpp_dir <- "FILEPATH"
loc_map2 <- get_locations(level = "all", gbd_year = gbd_year)[, .(
  ihme_loc_id,
  location_id,
  region_name
)]
graph_dir <- "FILEPATH"

## functions
inv_logit <- function(x) {
  return(exp(x) / (1 + exp(x)))
}

## reading in data
for (v in 1:3) {
  version <- get(paste0("version", v))
  old_code <- file.exists("FILEPATH")
  if (!old_code) {
    base_dir <- "FILEPATH"
    stage1 <- fread("FILEPATH/compiled_stage1_results.csv")
    stage2 <- fread("FILEPATH/compiled_stage2_results.csv")
    data <- fread("FILEPATH/compiled_input_data.csv")
    gpr <- fread("FILEPATH/compiled_gpr_results.csv")
    amp <- assertable::import_files(list.files("FILEPATH"),
      pattern = "gpr_input", full.names = T
    )
    params <- assertable::import_files(list.files("FILEPATH"),
      pattern = "params", full.names = T
    )

    amp <- unique(amp[, .(ihme_loc_id, age, mse)])
    params <- params[, .(ihme_loc_id, dd, zeta, beta, scale)]
    if (nrow(params) == 9486) {
      params[, age := rep(seq(10, 50, 5), each = length(unique(params$ihme_loc_id)))]
    } else {
      params[, age := rep(seq(15, 45, 5), each = length(unique(params$ihme_loc_id)))]
    }
    data[, method_name := paste0(method_name, "_", source_name)]
    stage12 <- merge(stage1, stage2[, .(ihme_loc_id, year_id, age, stage2_pred)],
      by = c("ihme_loc_id", "year_id", "age")
    )
    stage12 <- merge(stage12, data[, .(
      ihme_loc_id, year_id, nid, age, asfr_data,
      outlier, logit_asfr_data, adjustment,
      ref_source, adjusted_asfr_data, variance,
      method_name
    )],
    by = c("ihme_loc_id", "year_id", "age"), all = T
    )

    setnames(
      stage12, c(
        "adjusted_asfr_data", "variance", "stage1_pred",
        "method_name", "adjustment"
      ),
      c(
        "adjusted_logit_asfr_data", "data_var", "stage1_pred_no_re",
        "source_type", "adjustment_factor"
      )
    )
  } else if (file.exists("FILEPATH")) {
    base_dir <- "FILEPATH"
    stage12 <- fread("FILEPATH/compiled_stage1and2.csv")
    gpr <- fread("FILEPATH/compiled_summary_gpr.csv")
    amp <- assertable::import_files(list.files("FILEPATH"),
      pattern = "results", full.names = T
    )
    params <- assertable::import_files(list.files("FILEPATH"),
      pattern = "params", full.names = T
    )

    stage12[, method_name := id]
    amp <- unique(amp[!is.na(mse), .(ihme_loc_id, age, mse)])
    setnames(params, "lambda", "beta")
    params <- params[, .(ihme_loc_id, dd, zeta, beta, scale)]
    params[, age := rep(seq(15, 45, 5), each = length(unique(params$ihme_loc_id)))]
  } else {
    stop(paste0("Missing outputs for version ", version))
  }

  stage12[, version := version]
  params[, version := version]
  if (v != 3) data[, version := version]
  gpr[, version := version]
  amp[, version := version]

  if (v != 3) assign(paste0("data_v", v), copy(data))
  assign(paste0("gpr_v", v), copy(gpr))
  assign(paste0("stage12_v", v), copy(stage12))
  assign(paste0("amp_v", v), copy(amp))
  assign(paste0("params_v", v), copy(params))
}

wpp <- fread(paste0(wpp_dir, "TFR5.csv"))
wpp <- wpp[, .(ihme_loc_id = location, year_id = as.numeric(date_start), value)]
wpp <- wpp[year_id <= floor(max(gpr_v1$year))]
wpp_asfr <- fread(paste0(wpp_dir, "ASFR5.csv"))
wpp_asfr <- wpp_asfr[, .(ihme_loc_id = location, year_id = as.numeric(date_start), age = age_start, value)]
wpp_asfr <- wpp_asfr[year_id <= floor(max(gpr_v1$year))]

stage12 <- rbind(stage12_v1, stage12_v2, stage12_v3, fill = T)
gpr <- rbind(gpr_v1, gpr_v2, gpr_v3, fill = T)

stage12[, category := "other"]
stage12[, data := !is.na(asfr_data)]
stage12[, mse := NA]
stage12[, c(
  "age_group_id", "age_group_name", "covariate_id",
  "covariate_name_short", "fem_edu", "location_id", "lower_bound",
  "model_version_id", "location_name", "sex", "sex_id", "upper_bound",
  "super_region_id", "region_name"
) := NULL]
bounds <- fread("FILEPATH/logit_bounds.csv")
stage12 <- merge(stage12, bounds, by = "age", all.x = T)
stage12 <- merge(stage12, loc_map2[, .(ihme_loc_id, region_name)], by = "ihme_loc_id")
stage12[, region := region_name]
stage12[, stage1_pred := stage1_pred_no_re]

# Remove reference categorization from other versions
stage12[version != (version1), ref_source := NA_real_]

## don't graph outliers twice
stage12[outlier == 1, adjusted_logit_asfr_data := NA]
stage12[outlier == 1, adjustment_factor := NA]

## get data adjustments
adj <- stage12[
  !is.na(asfr_data) & outlier == 0 & version == version1,
  .(ihme_loc_id, year_id, age, nid, adjustment_factor, source_type)
]

## format amp and params
amp <- rbind(amp_v1, amp_v2, amp_v3)
params <- rbind(params_v1, params_v2, params_v3)

## plotting version1 parameters
params <- merge(params, amp, by = c("ihme_loc_id", "age", "version"))

## data prep
stage12 <- stage12[, .(
  ihme_loc_id, year_id, region_name, age, asfr_data,
  stage1_pred, stage2_pred, upper_bound, lower_bound,
  outlier, adjusted_logit_asfr_data, adjustment_factor,
  source_type, region, version, nid, ref_source
)]

stage12[, stage1_pred := inv_logit(stage1_pred)]
stage12[, stage1_pred := stage1_pred * (upper_bound - lower_bound) + lower_bound]

stage12[, stage2_pred := inv_logit(stage2_pred)]
stage12[, stage2_pred := stage2_pred * (upper_bound - lower_bound) + lower_bound]

stage12[, adjusted_asfr_data := inv_logit(adjusted_logit_asfr_data)]
stage12[, adjusted_asfr_data := adjusted_asfr_data * (upper_bound -
  lower_bound) + lower_bound]
stage12[adjustment_factor == 0, adjusted_asfr_data := asfr_data]

stage12[, c("upper_bound", "lower_bound") := NULL]

stage12 <- unique(stage12, by = c(
  "ihme_loc_id", "year_id", "version", "age",
  "region_name", "stage1_pred", "stage2_pred",
  "nid", "source_type"
))

data_v1 <- unique(data_v1, by = c(
  "ihme_loc_id", "year_id", "version", "age",
  "source_name", "method_name", "asfr_data",
  "adjusted_asfr_data"
))

gpr[, type := "gpr"]
gpr[, year_id := floor(year)]

if (plot_young_old == F) gpr <- gpr[!age %in% c(10, 50)]

gpr$age <- as.character(gpr$age)
stage12$age <- as.character(stage12$age)
data_v1$age <- as.character(data_v1$age)

g <- merge(gpr, stage12, by = c("ihme_loc_id", "year_id", "age", "version"), all = T)
g <- melt.data.table(g,
  measure.vars = c(
    "stage1_pred", "stage2_pred",
    "asfr_data", "mean", "adjusted_asfr_data"
  ),
  id.vars = c(
    "ihme_loc_id", "year_id", "region_name", "age",
    "lower", "upper", "outlier", "source_type",
    "version", "nid", "ref_source"
  ),
  variable.name = "type",
  value.name = "asfr"
)

g <- g[type != "mean", lower := NA]
g <- g[type != "mean", upper := NA]

g[type == "mean", type := "loop2"]
g <- g[!is.na(asfr)]
g[type != "asfr_data", outlier := NA]
g[outlier == 1, type := "outliered_data"]

## adding wpp tfr
wpp <- merge(wpp, loc_map2, by = "ihme_loc_id", all.x = T)
wpp <- wpp[!is.na(ihme_loc_id)]
wpp[, age := "tfr"]
wpp[, version := "WPP"]
wpp[, type := "loop2"]

setnames(wpp, "value", "asfr")

g <- rbind(g, wpp, fill = T)

## adding wpp asfr
wpp_asfr[, version := "WPP"]
wpp_asfr[, type := "loop2"]
setnames(wpp_asfr, "value", "asfr")

g <- rbind(g, wpp_asfr, fill = T)

g[
  type == "asfr_data" & grepl("sbh", source_type, ignore.case = T),
  type := "split_sbh"
]
g[
  type == "adjusted_asfr_data" & grepl("sbh", source_type, ignore.case = T),
  type := "adjusted_split_sbh"
]

setnames(g, "ref_source", "reference")

g <- merge(g, loc_map, by = "ihme_loc_id")

# Add plotting data for ages 10 and 50 if not present
if (nrow(g[type == "asfr_data" & (age == 10 | age == 50)]) == 0) {
  old_young_data <- fread("FILEPATH/old_young_model_data.csv")

  old_young_data <- merge(old_young_data, loc_map2, by = "ihme_loc_id")
  old_young_data <- merge(old_young_data, loc_map, by = "ihme_loc_id")

  setnames(old_young_data, "source_name", "source_type")
  old_young_data[grepl("CBH", method_name), source_type := paste0("CBH_", source_type)]
  old_young_data[
    grepl("Dir-Unadj", method_name),
    source_type := paste0(method_name, "_", source_type)
  ]
  old_young_data[!grepl("CBH", method_name) & source_type == "Other", source_type := "Unknown_Other"]

  old_young_data <- old_young_data[
    ,
    .(
      version = version1,
      age = as.character(age),
      lower = NA_real_,
      upper = NA_real_,
      outlier, source_type,
      type = "adjusted_asfr_data",
      asfr = asfr_data,
      reference = 0,
      nid, year_id,
      location_name, level, region_name,
      parent_id, location_id, ihme_loc_id
    )
  ]
  old_young_data[outlier == 1, type := "outliered_data"]

  g <- rbind(g, old_young_data)
}

# compare latest years
current_old <- g[version %in% c(version1, version3) & type == "loop2"]
current_old[, max_yr := max(year_id), by = "version"]
current_old <- current_old[year_id == min(max_yr)]
current_old <- unique(current_old[, .(ihme_loc_id, location_name, level, year_id, age, version, asfr)])
current_old[version == version1, version := "new"]
current_old[version == version3, version := "old"]
current_old <- dcast(current_old, ... ~ version, value.var = "asfr")
current_old[, pct_diff := (`new` - `old`) / `old` * 100]
current_old[, abs_pct_diff := abs(`new` - `old`) / `old` * 100]
current_old <- current_old[order(-abs_pct_diff)]

current_best <- g[version %in% c(version1, version2) & type == "loop2"]
current_best[, max_yr := max(year_id), by = "version"]
current_best <- current_best[year_id == min(max_yr)]
current_best <- unique(current_best[, .(ihme_loc_id, location_name, level, year_id, age, version, asfr)])
current_best[version == version1, version := "new"]
current_best[version == version2, version := "best"]
current_best <- dcast(current_best, ... ~ version, value.var = "asfr")
current_best[, pct_diff := (`new` - `best`) / `best` * 100]
current_best[, abs_pct_diff := abs(`new` - `best`) / `best` * 100]
current_best <- current_best[order(-abs_pct_diff)]

# Output files

write_csv(g, paste0(graph_dir, "/plotting_data.csv"))

write_csv(params, paste0(graph_dir, "/plotting_params.csv"))

write_csv(adj, paste0(graph_dir, "/plotting_adj.csv"))

write_csv(current_old, paste0(graph_dir, "/compare_", version1, "_", version3, ".csv"))
write_csv(current_best, paste0(graph_dir, "/compare_", version1, "_", version2, ".csv"))
