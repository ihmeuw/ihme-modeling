#######################################################################
##                                                                   ##
## Purpose: Aggregates, compiles, and saves final stillbirth results ##
##                                                                   ##
#######################################################################

rm(list = ls())
Sys.umask(mode = "0002")

library(assertable)
library(data.table)
library(readr)
library(stringr)

library(mortcore)
library(mortdb)

user <- Sys.getenv("USER")

# Get settings
args <- commandArgs(trailingOnly = TRUE)
new_settings_dir <- args[1]

if (interactive()) { # when interactive, only one definition can be run at a time
  version_estimate <- 999
  main_std_def <- "28_weeks"
  new_settings_dir <- "FILEPATH"
}

load(new_settings_dir)
list2env(new_settings, envir = environment())

source(transformation_functions.R)

locs <- mortdb::get_locations(level = "estimate", gbd_year = gbd_year)

##################
## Read in Data ##
##################

## Get file names and test all expected scaled files are present
gpr_files <- list.files(path = "FILEPATH")
scaled_files <- list.files(path = "FILEPATH")

parent_locs <- fread("FILEPATH")

scaled_files_test <- as.data.table(scaled_files)
scaled_files_test[, scaled_files := substr(scaled_files, 5, 7)]
scaled_files_test <- unique(scaled_files_test)

scaled_files_missing <- parent_locs[!(loc %in% scaled_files_test$scaled_files)]

if (nrow(scaled_files_missing) > 0) stop (paste0("Raking/Aggregating failed for ", print(scaled_files_missing$loc)))

## Read in files
gpr_files <- gpr_files[!(gpr_files %in% scaled_files)]

gpr_files <- "FILEPATH"
scaled_files <- paste(paste0(estimate_dir, version_estimate, "/scaled/"), scaled_files, sep = "")

gpr_files <- c(gpr_files, scaled_files)
gpr_files <- gpr_files[gpr_files %like% paste0("sim_", main_std_def)]

gpr <- rbindlist(lapply(gpr_files, FUN = fread), fill = T)

## Clean data table
gpr[, V1 := NULL]

gpr[, year := floor(year)]
gpr[, ihme_loc_id := stringr::str_remove(ihme_loc_id, "[b]")]
gpr[, ihme_loc_id := stringr::str_remove_all(ihme_loc_id, "[']")]

## Check for missing files
missing_locs <- c()
missing_locs <- locs[!(ihme_loc_id %in% unique(gpr$ihme_loc_id))]
if (nrow(missing_locs) > 0) stop ("Files are missing.")

## Merge on location_id, dropping ihme_loc_id
locations <- mortdb::get_locations(
  level = "estimate",
  gbd_year = gbd_year
)
locations <- locations[, list(ihme_loc_id, location_id),]

gpr <- merge(
  gpr,
  locations,
  by = "ihme_loc_id",
  all.x = TRUE
)

setnames(gpr, "year", "year_id")

## Read in births and neonatal mortality rate
bir_nmr <- fread(
  "FILEPATH"
)
bir_nmr <- bir_nmr[, list(location_id, year_id, births, q_nn_med)]
bir_nmr <- bir_nmr[year_id <= year_end]

gpr <- merge(
  gpr,
  bir_nmr,
  by = c("location_id", "year_id"),
  all.x = TRUE
)

assertable::assert_values(gpr, colnames = c("births", "q_nn_med"), test = "not_na")

## Calculate stillbirth rate and number of stillbirths
if (model == "SBR/NMR") {

  gpr[, mort := lograt_to_sbr(mort, q_nn_med)] ## calculate SBR
  assertable::assert_values(gpr, colnames = "mort", test = "not_na")

  gpr[, sb_number := sbr_to_sb(mort, births)] ## calculate number of stillbirths
  assertable::assert_values(gpr, colnames = "sb_number", test = "not_na")

} else if (model == "SBR + NMR") {

  gpr[, mort := exp(mort) - q_nn_med] ## calculate SBR
  assertable::assert_values(gpr, colnames = "mort", test = "not_na")

  gpr[, sb_number := sbr_to_sb(mort, births)] ## calculate number of stillbirths
  assertable::assert_values(gpr, colnames = "sb_number", test = "not_na")

} else if (model == "SBR") {

  gpr[, mort := exp(mort)] ## calculate SBR
  assertable::assert_values(gpr, colnames = "mort", test = "not_na")

  gpr[, sb_number := sbr_to_sb(mort, births)] ## calculate number of stillbirths
  assertable::assert_values(gpr, colnames = "sb_number", test = "not_na")

}

gpr <- gpr[, c("ihme_loc_id", "mort", "q_nn_med") := NULL]

#############################
## Aggregating Stillbirths ##
#############################

## Run the aggregation function
gpr[, age_group_id := 22]
gpr[, sex_id := 3]

agg_draws <- mortcore::agg_results(
  data = gpr,
  value_vars = c("births", "sb_number"),
  id_vars = c("location_id", "year_id", "sex_id", "age_group_id", "sim"),
  gbd_year = gbd_year,
  agg_sdi = include_sdi_locs
)

## Calculate SBR
agg_draws[, sbr := sb_number / (births + sb_number)]
if (nrow(agg_draws[is.na(sbr), ]) != 0) stop ("Missing sbr")

aroc <- copy(agg_draws)

## Calculate SBR/NMR
nmr <- mortdb::get_mort_outputs(
  model_name = "no shock life table",
  model_type = "estimate",
  run_id = parents[["no shock life table estimate"]],
  life_table_parameter_ids = 3,
  estimate_stage_ids = 5,
  gbd_year = gbd_year,
  sex_ids = 3,
  year_ids = year_start:year_end,
  age_group_ids = 42
)

setnames(nmr, "mean", "nmr")

agg_draws <- merge(
  agg_draws,
  nmr[, c("location_id", "year_id", "nmr")],
  by = c("location_id", "year_id"),
  all.x = TRUE
)

agg_draws[, sbr_nmr := sbr/nmr]
if (nrow(agg_draws[is.na(sbr_nmr), ]) != 0) stop ("Missing sbr/nmr")

## Save
readr::write_csv(
  agg_draws,
  "FILEPATH"
)

###############
## Summarize ##
###############

## Create a summary file
setkey(agg_draws, location_id, year_id, births)
summary <- agg_draws[,
  list(
    sbr_mean = mean(sbr, na.rm = TRUE),
    sbr_lower = quantile(sbr, probs = c(.025), na.rm = TRUE),
    sbr_upper = quantile(sbr, probs = c(.975), na.rm = TRUE),
    sb_mean = mean(sb_number, na.rm = TRUE),
    sb_lower = quantile(sb_number, probs = c(.025), na.rm = TRUE),
    sb_upper = quantile(sb_number, probs = c(.975), na.rm = TRUE),
    sbr_nmr_mean = mean(sbr_nmr, na.rm = TRUE),
    sbr_nmr_lower = quantile(sbr_nmr, probs = c(.025), na.rm = TRUE),
    sbr_nmr_upper = quantile(sbr_nmr, probs = c(.975), na.rm = TRUE)),
  by = key(agg_draws)
]

readr::write_csv(
  summary,
  "FILEPATH"
)

##############################
## Calculate Percent Change ##
##############################

years_pc <- data.table(
  year_start = c("1990", "1990", "2015"),
  year_end = c("2020", "2021", "2021")
)

for (y in 1:nrow(years_pc)) {

  pc_start <- years_pc[y]$year_start
  pc_end <- years_pc[y]$year_end

  ## SBR
  sbr_start <- aroc[year_id == pc_start, c("location_id", "sim", "sbr")]
  sbr_end <- aroc[year_id == pc_end, c("location_id", "sim", "sbr")]

  setnames(sbr_start, "sbr", "sbr_start")
  setnames(sbr_end, "sbr", "sbr_end")

  sbr_pc <- merge(
    sbr_start,
    sbr_end,
    by = c("location_id", "sim"),
    all = TRUE
  )

  sbr_pc[, sbr_pc := ((sbr_end - sbr_start)/sbr_start) * 100]

  setkey(sbr_pc, location_id)
  pc_summary <- sbr_pc[,
                       list(
                         sbr_pc_mean = mean(sbr_pc, na.rm = TRUE),
                         sbr_pc_lower = quantile(sbr_pc, probs = c(.025), na.rm = TRUE),
                         sbr_pc_upper = quantile(sbr_pc, probs = c(.975), na.rm = TRUE)),
                       by = key(sbr_pc)
  ]

  readr::write_csv(
    pc_summary,
    "FILEPATH"
  )

  ## SB
  sb_start <- aroc[year_id == pc_start, c("location_id", "sim", "sb_number")]
  sb_end <- aroc[year_id == pc_end, c("location_id", "sim", "sb_number")]

  setnames(sb_start, "sb_number", "sb_start")
  setnames(sb_end, "sb_number", "sb_end")

  sb_pc <- merge(
    sb_start,
    sb_end,
    by = c("location_id", "sim"),
    all = TRUE
  )

  sb_pc[, sb_pc := ((sb_end - sb_start)/sb_start) * 100]

  setkey(sb_pc, location_id)
  pc_summary <- sb_pc[,
                      list(
                        sb_pc_mean = mean(sb_pc, na.rm = TRUE),
                        sb_pc_lower = quantile(sb_pc, probs = c(.025), na.rm = TRUE),
                        sb_pc_upper = quantile(sb_pc, probs = c(.975), na.rm = TRUE)),
                      by = key(sb_pc)
  ]

  readr::write_csv(
    pc_summary,
    "FILEPATH"
  )

}

####################
## Calculate AROC ##
####################

## Get annualized rates of change at the draw level
setkey(aroc, sim, location_id)
aroc <- as.data.frame(
  aroc[,
    list(
      aroc_8000 = log(sbr[year_id == 2000]/sbr[year_id == 1980])/(2000 - 1980),
      aroc_8020 = log(sbr[year_id == 2020]/sbr[year_id == 1980])/(2020 - 1980),
      aroc_8021 = log(sbr[year_id == 2021]/sbr[year_id == 1980])/(2021 - 1980),
      aroc_9000 = log(sbr[year_id == 2000]/sbr[year_id == 1990])/(2000 - 1990),
      aroc_9020 = log(sbr[year_id == 2020]/sbr[year_id == 1990])/(2020 - 1990),
      aroc_9021 = log(sbr[year_id == 2021]/sbr[year_id == 1990])/(2021 - 1990),
      aroc_0010 = log(sbr[year_id == 2010]/sbr[year_id == 2000])/(2010 - 2000),
      aroc_0020 = log(sbr[year_id == 2020]/sbr[year_id == 2000])/(2020 - 2000),
      aroc_0021 = log(sbr[year_id == 2021]/sbr[year_id == 2000])/(2021 - 2000),
      aroc_1020 = log(sbr[year_id == 2020]/sbr[year_id == 2010])/(2020 - 2010),
      aroc_1021 = log(sbr[year_id == 2021]/sbr[year_id == 2010])/(2021 - 2010),
      aroc_1521 = log(sbr[year_id == 2021]/sbr[year_id == 2015])/(2021 - 2015)),
    by = key(aroc)]
)

## Summarize AROC
arocsum <- copy(aroc)
arocsum <- data.table(arocsum)
setkey(arocsum, location_id)
arocsum <- as.data.frame(
  arocsum[,
    list(
      aroc_8000_lower = quantile(aroc_8000, prob = c(.025), na.rm = TRUE),
      aroc_8000_upper = quantile(aroc_8000, prob = c(.975), na.rm = TRUE),
      aroc_8020_lower = quantile(aroc_8020, prob = c(.025), na.rm = TRUE),
      aroc_8020_upper = quantile(aroc_8020, prob = c(.975), na.rm = TRUE),
      aroc_8021_lower = quantile(aroc_8021, prob = c(.025), na.rm = TRUE),
      aroc_8021_upper = quantile(aroc_8021, prob = c(.975), na.rm = TRUE),
      aroc_9000_lower = quantile(aroc_9000, prob = c(.025), na.rm = TRUE),
      aroc_9000_upper = quantile(aroc_9000, prob = c(.975), na.rm = TRUE),
      aroc_9020_lower = quantile(aroc_9020, prob = c(.025), na.rm = TRUE),
      aroc_9020_upper = quantile(aroc_9020, prob = c(.975), na.rm = TRUE),
      aroc_9021_lower = quantile(aroc_9021, prob = c(.025), na.rm = TRUE),
      aroc_9021_upper = quantile(aroc_9021, prob = c(.975), na.rm = TRUE),
      aroc_0010_lower = quantile(aroc_0010, prob = c(.025), na.rm = TRUE),
      aroc_0010_upper = quantile(aroc_0010, prob = c(.975), na.rm = TRUE),
      aroc_0020_lower = quantile(aroc_0020, prob = c(.025), na.rm = TRUE),
      aroc_0020_upper = quantile(aroc_0020, prob = c(.975), na.rm = TRUE),
      aroc_0021_lower = quantile(aroc_0021, prob = c(.025), na.rm = TRUE),
      aroc_0021_upper = quantile(aroc_0021, prob = c(.975), na.rm = TRUE),
      aroc_1020_lower = quantile(aroc_1020, prob = c(.025), na.rm = TRUE),
      aroc_1020_upper = quantile(aroc_1020, prob = c(.975), na.rm = TRUE),
      aroc_1021_lower = quantile(aroc_1021, prob = c(.025), na.rm = TRUE),
      aroc_1021_upper = quantile(aroc_1021, prob = c(.975), na.rm = TRUE),
      aroc_1521_lower = quantile(aroc_1521, prob = c(.025), na.rm = TRUE),
      aroc_1521_upper = quantile(aroc_1521, prob = c(.975), na.rm = TRUE)),
   by = key(arocsum)]
)

locroc <- mortdb::get_locations(level = "all", gbd_year = gbd_year)

arocsum <- merge(
  arocsum,
  locroc[, c("location_id", "ihme_loc_id", "location_name")],
  all.x = TRUE
)

## Get mean from AROC of means
aroc_mean <- copy(summary)
aroc_mean <- data.table(aroc_mean)
aroc_mean <- aroc_mean[, .(location_id, year_id, sbr_mean)]
setkey(aroc_mean, location_id)
aroc_mean <- aroc_mean[,
  list(
    aroc_8000_mean = log(sbr_mean[year_id == 2000]/sbr_mean[year_id == 1980])/(2000 - 1980),
    aroc_8020_mean = log(sbr_mean[year_id == 2020]/sbr_mean[year_id == 1980])/(2020 - 1980),
    aroc_8021_mean = log(sbr_mean[year_id == 2021]/sbr_mean[year_id == 1980])/(2021 - 1980),
    aroc_9000_mean = log(sbr_mean[year_id == 2000]/sbr_mean[year_id == 1990])/(2000 - 1990),
    aroc_9020_mean = log(sbr_mean[year_id == 2020]/sbr_mean[year_id == 1990])/(2020 - 1990),
    aroc_9021_mean = log(sbr_mean[year_id == 2021]/sbr_mean[year_id == 1990])/(2021 - 1990),
    aroc_0010_mean = log(sbr_mean[year_id == 2010]/sbr_mean[year_id == 2000])/(2010 - 2000),
    aroc_0020_mean = log(sbr_mean[year_id == 2020]/sbr_mean[year_id == 2000])/(2020 - 2000),
    aroc_0021_mean = log(sbr_mean[year_id == 2021]/sbr_mean[year_id == 2000])/(2021 - 2000),
    aroc_1020_mean = log(sbr_mean[year_id == 2020]/sbr_mean[year_id == 2010])/(2020 - 2010),
    aroc_1021_mean = log(sbr_mean[year_id == 2021]/sbr_mean[year_id == 2010])/(2021 - 2010),
    aroc_1521_mean = log(sbr_mean[year_id == 2021]/sbr_mean[year_id == 2015])/(2021 - 2015)),
  by = key(aroc_mean)
]

## Merge on AROC means
arocsum <- merge(
  aroc_mean,
  arocsum,
  by = "location_id",
  all = TRUE
)

readr::write_csv(
  arocsum,
  "FILEPATH"
)

#############################
## Repeat for Unscaled GPR ##
#############################

## Read in files
unscaled_files <- list.files(
  path = "FILEPATH",
  full.names = TRUE
)
unscaled_files <- unscaled_files[unscaled_files %like% paste0("sim_", main_std_def)]

gpr <- rbindlist(lapply(unscaled_files, FUN = fread), fill = TRUE)

## Clean data table
gpr[, V1 := NULL]

gpr[, year := floor(year)]
gpr[, ihme_loc_id := str_remove(ihme_loc_id, "[b]")]
gpr[, ihme_loc_id := str_remove_all(ihme_loc_id, "[']")]

## Check for missing files
missing_locs <- c()
missing_locs <- locs[!(ihme_loc_id %in% unique(gpr$ihme_loc_id))]
if (nrow(missing_locs) > 0) stop ("Files are missing.")

## Merge on location_id, dropping ihme_loc_id
locations <- mortdb::get_locations(level = "estimate", gbd_year = gbd_year)
locations <- locations[, list(ihme_loc_id, location_id),]

gpr <- merge(
  gpr,
  locations,
  by = "ihme_loc_id",
  all.x = TRUE
)

setnames(gpr, "year", "year_id")

## Read in births and neonatal mortality rate
bir_nmr <- fread(
  "FILEPATH"
)
bir_nmr <- bir_nmr[, list(location_id, year_id, births, q_nn_med)]

gpr <- merge(
  gpr,
  bir_nmr,
  by = c("location_id", "year_id"),
  all.x = TRUE
)

if (nrow(gpr[is.na(gpr$births), ]) != 0) stop("Missing births")
if (nrow(gpr[is.na(gpr$q_nn_med), ]) != 0) stop("Missing NMR")

gpr <- gpr[!is.na(sim)]

## Calculate stillbirth rate and number of stillbirths
if (model == "SBR/NMR") {

  gpr[, mort := lograt_to_sbr(mort, q_nn_med)] ## calculate SBR
  assertable::assert_values(gpr, colnames = "mort", test = "not_na")

  gpr[, sb := sbr_to_sb(mort, births)] ## calculate number of stillbirths
  assertable::assert_values(gpr, colnames = "sb", test = "not_na")

} else if (model == "SBR + NMR") {

  gpr[, mort := exp(mort) - q_nn_med] ## calculate SBR
  assertable::assert_values(gpr, colnames = "mort", test = "not_na")

  gpr[, sb := sbr_to_sb(mort, births)] ## calculate number of stillbirths
  assertable::assert_values(gpr, colnames = "sb", test = "not_na")

} else if (model == "SBR") {

  gpr[, mort := exp(mort)] ## calculate SBR
  assertable::assert_values(gpr, colnames = "mort", test = "not_na")

  gpr[, sb := sbr_to_sb(mort, births)] ## calculate number of stillbirths
  assertable::assert_values(gpr, colnames = "sb", test = "not_na")

}

gpr <- gpr[, c("ihme_loc_id", "mort", "q_nn_med") := NULL]

gpr[, sbr := sb/(sb + births)]

## Summarize
setkey(gpr, location_id, year_id, births)
summary <- gpr[,
  list(
    sbr_mean = mean(sbr, na.rm = TRUE),
    sbr_lower = quantile(sbr, probs = c(.025), na.rm = TRUE),
    sbr_upper = quantile(sbr, probs = c(.975), na.rm = TRUE),
    sb_mean = mean(sb, na.rm = TRUE),
    sb_lower = quantile(sb, probs = c(.025), na.rm = TRUE),
    sb_pper = quantile(sb, probs = c(.975), na.rm = TRUE)),
    by = key(gpr)
]

## Save
readr::write_csv(
  summary,
  "FILEPATH"
)
