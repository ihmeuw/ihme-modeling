################################################################################
## Purpose:   Save RhoD sales data from old custom bundle ID: 4715, bundle version ID: 43365
##            to Dismod Bundle ID: 10513 and save crosswalk version to be used
##            for saving RhoD model results into MEID: 28989
## Input:     Bundle ID: 4715, Bundle Version ID: 43365
## Output:    FILEPATH
################################################################################

# set up
library(dplyr)

# pull bundle data from old custom bundle --------------------------------------
bundle <- ihme::get_bundle_version(bundle_version_id = 43365)

# format bundle data for saving to dismod bundle 10513 -------------------------
bundle |>
  transmute(
    seq = NA,
    input_type = NA,
    underlying_nid = NA,
    nid = nid,
    source_type = "Surveillance - other/unknown",
    location_id = location_id,
    sex = sex,
    year_start = year_start,
    year_end = year_end,
    age_start = 0,
    age_end = 99,
    measure = measure,
    mean = NA,
    upper = NA,
    lower = NA,
    cases = cases,
    sample_size = 10000000, # arbitrarily high sample size
    effective_sample_size = NA,
    design_effect = NA,
    unit_type = "Person",
    unit_value_as_published = 1,
    uncertainty_type = "Confidence interval",
    uncertainty_type_value = NA,
    representative_name = "Nationally and subnationally representative",
    urbanicity_type = "Unknown",
    recall_type = "Not Set",
    sampling_type = NA,
    is_outlier = 0,
    # Columns that per documentation shouldn't be required, but validations fail
    # without them. See DisMod shape validations:
    # FILEPATH
    recall_type_value = NA,
    standard_error = NA,
  ) |>
  openxlsx::write.xlsx(
    file = "FILEPATH",
    sheetName = "extraction"
  )

# save bundle version and automatic crosswalk version --------------------------
bundle_id <- 10513
upload_filepath <-
  'FILEPATH'
result1 <- ihme::upload_bundle_data(bundle_id, filepath=upload_filepath)
result2 <- ihme::save_bundle_version(bundle_id, automatic_crosswalk = TRUE)

# pull RhoD to live births modeled results -------------------------------------
rhod <- data.table::fread("FILEPATH")

# format bundle data for saving to dismod bundle 10513 -------------------------
rhod <- rhod %>% mutate(
  age_group_id = nch::id_for("age_group", "Birth"),
  measure_id = nch::id_for("measure", "proportion"),
  metric_id = nch::id_for("metric", "Rate"),
  sex_id = nch::id_for("sex", "Male")
)

rhod <- bind_rows(rhod, rhod %>%
                    mutate(sex_id = nch::id_for("sex", "Female")))

data.table::fwrite(
  rhod,
  "FILEPATH"
)

# save model results -----------------------------------------------------------
index_df <- ihme::save_results_epi(
  input_dir = "FILEPATH",
  input_file_pattern = 'all_draws.csv',
  modelable_entity_id = 28989,
  description = "GBD23 - rhod sales to live births proportion",
  measure_id = nch::id_for("measure", "proportion"),
  metric_id = nch::id_for("metric", "Rate"),
  release_id = nch::id_for("release", "GBD 2023"),
  mark_best = TRUE,
  birth_prevalence = TRUE,
  bundle_id = 10513,
  crosswalk_version_id = 46052
)
