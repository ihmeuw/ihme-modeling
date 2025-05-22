################################################################################
## Description: Download all inputs needed for the model fitting so that the
##              databases aren't repeatedly queried and we avoid pulling from
##              the j drive in other scripts.
################################################################################

library(data.table)
library(readr)
library(parallel)
library(assertable)
library(mortdb)
library(mortcore)

rm(list = ls())
MKL_NUM_THREADS <- Sys.getenv("MKL_NUM_THREADS")
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEPATH")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--pop_vid", type = "character",
                    help = "The version number for this run of population, used to read in settings file")
parser$add_argument("--test", type = "character",
                    help = "Whether this is a test run of the process")
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$pop_vid <- "99999"
  args$test <- "T"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", ifelse(test, "test/", ""), pop_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

location_hierarchy <- fread(paste0(output_dir, "/database/all_location_hierarchies.csv"))
location_hierarchy <- location_hierarchy[location_set_name == "population"]

loc_ids_estimate <- location_hierarchy[is_estimate == 1, location_id]


# Age groupings -----------------------------------------------------------

# get all possible age groups
age_groups_all <- mortdb::get_age_map(type = "all", drop_deleted_age_groups = T)
age_groups_all <- age_groups_all[!grepl("standardized", age_group_name)]
age_groups_all[age_group_id == 164, age_group_years_start := -1] # adjust age_group start and end for births

age_groups_gbd <- mortdb::get_age_map(type = "gbd")
age_groups_reporting <- mortdb::get_age_map(type = "population")
age_groups_single <- mortdb::get_age_map(type = "single_year", single_year_terminal_age_start = terminal_age)
age_groups_fertility_single <- mortdb::get_age_map(type = "fertility_single_year")


# need to subset from gbd standard age groups to population five year age groups
age_groups_five_model <- rbind(age_groups_all[age_group_id == 1], age_groups_gbd[age_group_years_start >= 5])

# mark the type of age group for all that we need
age_groups_all[, single_year_model := age_group_id %in% age_groups_single$age_group_id]
age_groups_all[, five_year_model := age_group_id %in% age_groups_five_model$age_group_id]
age_groups_all[, most_detailed := age_group_id %in% c(2:3, 388, 389) | age_group_id %in% age_groups_single[age_group_years_start >= 1, age_group_id]]
age_groups_all[, reporting := age_group_id %in% age_groups_reporting$age_group_id]
age_groups_all[, reporting_migration := age_group_id %in% reporting_age_groups_migration | age_group_id %in% age_groups_gbd[age_group_years_start >= 1, age_group_id]]
age_groups_all[, plot_migration := age_group_id %in% c(28, 5) | (five_year_model & age_group_years_start >= 5)]
age_groups_all[, fertility_single_year := age_group_id %in% age_groups_fertility_single$age_group_id]

readr::write_csv(age_groups_all, paste0(output_dir, "/database/age_groups.csv"))


# Census data -------------------------------------------------------------

census_data <- mortdb::get_mort_outputs(model_name = "census processed", model_type = "data", run_id = census_processed_data_vid,
                                        location_ids = location_hierarchy[is_estimate == 1, location_id])

# merge on ids
record_type_ids <- mortdb::get_mort_ids(type = "record_type")
method_ids <- mortdb::get_mort_ids(type = "method")
pes_adjustment_type_ids <- mortdb::get_mort_ids(type = "pes_adjustment_type")
data_stage_ids <- mortdb::get_mort_ids(type = "data_stage")
outlier_type_ids <- mortdb::get_mort_ids(type = "outlier_type")
census_data <- merge(census_data, record_type_ids, by = "record_type_id", all.x = T)
census_data <- merge(census_data, method_ids, by = "method_id", all.x = T)
census_data <- merge(census_data, pes_adjustment_type_ids, by = "pes_adjustment_type_id", all.x = T)
census_data <- merge(census_data, data_stage_ids, by = "data_stage_id", all.x = T)
census_data <- merge(census_data, outlier_type_ids, by = "outlier_type_id", all.x = T)

# subset to the last processing step with the baseline included
census_data <- census_data[data_stage == "baseline included" & sex_id %in% c(1, 2),
                           list(location_id, year_id, nid, underlying_nid, source_name, outlier_type,
                                record_type, method_short, pes_adjustment_type, data_stage,
                                sex_id, age_group_id, mean)]
census_data_id_vars <- c("location_id", "year_id", "nid", "underlying_nid", "source_name",
                    "outlier_type", "record_type", "method_short", "pes_adjustment_type", "data_stage")
setkeyv(census_data, c(census_data_id_vars, "sex_id", "age_group_id"))

readr::write_csv(census_data, paste0(output_dir, "/database/census_data.csv"))
rm(census_data); gc()


# Migration data ----------------------------------------------------------

# pull data from database
migration_data <- mortdb::get_mort_outputs(model_name = "migration flow", model_type = "data", run_id = migration_data_vid,
                                           location_ids = location_hierarchy[is_estimate == 1, location_id])
readr::write_csv(migration_data, paste0(output_dir, "/database/migration_data.csv"))
rm(migration_data); gc()


# Migration age patterns --------------------------------------------------

# QAT migration age pattern

QAT_best_drop_age <- fread(paste0("FILEPATH", pop_current_round_run_id, "/versions_best.csv"))[ihme_loc_id == "QAT", drop_age]
QAT_copy_success <- file.copy(
  from = paste0("FILEPATH"),
  to = paste0(output_dir, "/database/QAT_migration.csv"),
  overwrite = T
)

if (!QAT_copy_success) {
  stop(paste0(
    "Could not copy QAT migration age pattern file `migration_proportion_drop", QAT_best_drop_age, ".csv`",
    " from `FILEPATH`.",
    " Maybe you are trying to copy from a baseline run that hasn't had these files copied into it yet?"
  ))
}

# EUROSTAT migration age pattern

EUROSTAT_copy_success <- file.copy(
  from = paste0("FILEPATH"),
  to = paste0(output_dir, "/database/EUROSTAT_migration.csv"),
  overwrite = T
)

if (!EUROSTAT_copy_success) {
  stop(paste0(
    "Could not copy EUROSTAT migration age pattern file `3_selected_migration.csv`",
    " from `FILEPATH`."
  ))
}


# Sex ratio at birth ------------------------------------------------------

srb <- mortdb::get_mort_outputs(model_name = "birth sex ratio", model_type = "estimate", run_id = srb_vid,
                                location_ids = location_hierarchy[is_estimate == 1, location_id])
assertable::assert_ids(srb, id_vars = list(location_id = location_hierarchy[is_estimate == 1, location_id],
                                           year_id = years))
readr::write_csv(srb, paste0(output_dir, "/database/srb.csv"))
rm(srb); gc()


# ASFR --------------------------------------------------------------------

asfr_dir <- paste0("FILEPATH", asfr_vid, "/loop2/gpr")

age_int_name <- "one"
fert_age_int <- 1
asfr <- parallel::mclapply(location_hierarchy[is_estimate == 1, ihme_loc_id], function(ihme_loc) {
  print(ihme_loc)
  loc_id <- location_hierarchy[ihme_loc_id == ihme_loc, location_id]
  data <- fread(paste(asfr_dir, paste0(age_int_name, "_by_", age_int_name),
                      paste0(ihme_loc, "_fert_", fert_age_int, "by", fert_age_int, ".csv"), sep = "/"))
  data <- data[, list(location_id = loc_id, year_id, age_group_years_start = age, mean = value_mean)]
  return(data)
}, mc.cores = MKL_NUM_THREADS)
asfr <- rbindlist(asfr)
asfr <- mortcore::age_start_to_age_group_id(asfr, id_vars = c("location_id", "year_id"), keep_age_group_id_only = F)
assertable::assert_ids(asfr, id_vars = list(location_id = location_hierarchy[is_estimate == 1, location_id],
                                            year_id = years,
                                            age_group_years_start = 0:terminal_age))
readr::write_csv(asfr, paste0(output_dir, "/database/asfr.csv"))
rm(asfr); gc()


# Full lifetables ---------------------------------------------------------

dir_canonizer <- fs::path("FILEPATH", full_life_table_run_id)

age_groups_canon_lt <- rbindlist(
  list(
    age_groups_gbd[age_group_years_start < 1],
    mortdb::get_age_map(type = "single_year", single_year_terminal_age_start = terminal_age)[age_group_years_start >= 1]
  ),
  use.names = TRUE,
  fill = TRUE
)[order(age_group_years_start), .(age_group_id, age = age_group_years_start)]

canonical_lifetables <-
  fs::path(dir_canonizer, "output/final_full_lt_summary", loc_ids_estimate, ext = "arrow") |>
  arrow::open_dataset(format = "arrow") |>
  dplyr::left_join(age_groups_canon_lt, by = "age_group_id") |>
  dplyr::filter(type == "with_shock", sex_id != 3) |>
  dplyr::select(location_id, year_id, sex_id, age, lx, nLx, Tx) |>
  dplyr::arrange(location_id, year_id, sex_id, age) |>
  dplyr::collect()

if (use_onemod_mortality) {

  locs_pak <- location_hierarchy[ihme_loc_id %like% "PAK", location_id]
  locs_ukr_sub <- location_hierarchy[ihme_loc_id %like% "UKR_", location_id]
  locs_eth <- location_hierarchy[ihme_loc_id %like% "ETH", location_id]

  dt_onemod <-
    fs::path("FILEPATH") |>
    fs::path("FILEPATH") |>
    arrow::open_dataset(format = "arrow") |>
    dplyr::filter(!location_id %in% c(152, 522, 122, locs_ukr_sub, locs_pak)) |> # Don't use SAU, SDN, ECU, UKR_sub, PAK onemod
    dplyr::filter(!location_id %in% locs_eth) |>
    dplyr::select(location_id, year_id, sex_id, age = age_start, lx, nLx, Tx) |>
    dplyr::arrange(location_id, year_id, sex_id, age) |>
    dplyr::collect() |>
    setDT()

  dt_onemod_eth <-
    fs::path("FILEPATH") |>
    fs::path("FILEPATH") |>
    arrow::open_dataset(format = "arrow") |>
    dplyr::filter(location_id %in% locs_eth) |>
    dplyr::select(location_id, year_id, sex_id, age = age_start, lx, nLx, Tx) |>
    dplyr::arrange(location_id, year_id, sex_id, age) |>
    dplyr::collect() |>
    setDT()

  dt_onemod <- rbindlist(
    list(dt_onemod, dt_onemod_eth),
    use.names = TRUE,
    fill = FALSE
  )

  canonical_lifetables[
    dt_onemod,
    c("lx", "nLx", "Tx") := .(i.lx, i.nLx, i.Tx),
    on = .(location_id, year_id, sex_id, age)
  ]
  rm(dt_onemod)

}

assertable::assert_ids(
  canonical_lifetables,
  id_vars = list(location_id = loc_ids_estimate, year_id = years, sex_id = 1:2, age = age_groups_canon_lt$age)
)

# Canonical life tables include only the detailed under 1 age groups, so we must
# collapse them to the overall under 1 age group.
u1_lifetables <- canonical_lifetables[
  age < 1,
  .(age = 0, lx = 1, nLx = sum(nLx)),
  by = .(location_id, year_id, sex_id)
]
u1_lifetables[
  canonical_lifetables,
  Tx := Tx,
  on = .(location_id, year_id, sex_id, age)
]

full_lifetables <- rbindlist(
  list(u1_lifetables, canonical_lifetables[age >= 1]),
  use.names = TRUE
)
full_lifetables[, age := as.integer(age)]
setorder(full_lifetables, location_id, year_id, sex_id, age)
readr::write_csv(full_lifetables, paste0(output_dir, "/database/full_lt.csv"))
rm(canonical_lifetables, u1_lifetables, full_lifetables); gc()


# Previous best population estimates --------------------------------------

# current GBD round's best population estimates
current_round_best_run <- mortdb::get_mort_outputs(model_name = "population single year", model_type = "estimate",
                                                   sex_id = 1:2, location_ids = if (length(test_locations) > 0) location_hierarchy[is_estimate == 1, location_id] else location_hierarchy[, location_id],
                                                   run_id = pop_single_current_round_run_id)
readr::write_csv(current_round_best_run, paste0(output_dir, "/database/gbd_population_current_round_best.csv"))
