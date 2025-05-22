################################################################################
## Purpose:   Saving kernicterus data from custom bundle 7256 to dismod bundle 10337
##            and saving the kernicterus proportion model results to MEID 27679
## Input:     Final draws for kernicterus proportions
##            FILEPATH
## Output:    MEID: 27679
################################################################################

# set up -----------------------------------------------------------------------
library(openxlsx)
library(data.table)
library(dplyr)

# format bundle ----------------------------------------------------------------
bundle <- ihme::get_bundle_version(bundle_version_id = 29633)
bundle <- bundle %>%
  mutate(source_type = ifelse(nid == 145780, "Facility - inpatient", source_type))

bundle |>
  transmute(
    seq = NA,
    input_type = NA,
    underlying_nid = NA,
    nid = nid,
    source_type = source_type,
    location_id = location_id,
    sex = sex,
    year_start = year_start,
    year_end = year_end,
    age_start = age_start,
    age_end = age_end,
    measure = measure,
    mean = mean,
    upper = upper,
    lower = lower,
    cases = cases,
    sample_size = sample_size,
    effective_sample_size = NA,
    design_effect = NA,
    unit_type = "Person",
    unit_value_as_published = 1,
    uncertainty_type = "Confidence interval",
    uncertainty_type_value = 95,
    representative_name = "Nationally and subnationally representative",
    urbanicity_type = "Unknown",
    recall_type = "Point",
    sampling_type = NA,
    is_outlier = 0,
    # Columns that per documentation shouldn't be required, but validations fail
    # without them. See DisMod shape validations:
    # FILEPATH
    recall_type_value = NA,
    standard_error = standard_error,
  ) |>
  openxlsx::write.xlsx(
    file = "FILEPATH",
    sheetName = "extraction"
  )

# save bundle version ----------------------------------------------------------
bundle_id <- 10337
upload_filepath <-
  'FILEPATH'
result1 <- ihme::upload_bundle_data(bundle_id, filepath=upload_filepath)

# save crosswalk version -------------------------------------------------------
# for crosswalk version we are applying sex as "Both" for all values since we don't
# distinguish the sex for modeling, changing proportion to prevalence for dismod validation,
# and linking those that changed to "Both" to the original seq value
bundle_version_id <- 47919
bundle_version_df <- ihme::get_bundle_version(bundle_version_id = bundle_version_id)
bundle_version_df <- bundle_version_df %>%
  mutate(measure = "prevalence") %>%
  mutate(crosswalk_parent_seq = NA) %>%
  mutate(crosswalk_parent_seq = ifelse(sex == "Female" | sex == "Male", seq, crosswalk_parent_seq)) %>%
  mutate(sex = ifelse(sex == "Female" | sex == "Male", "Both", sex))

data_filepath <- "FILEPATH"
openxlsx::write.xlsx(
  bundle_version_df,
  file = data_filepath,
  sheetName = "extraction"
)
description <- "for kernicterus proportion model results"
result2 <- ihme::save_crosswalk_version(
  bundle_version_id = bundle_version_id,
  data_filepath = data_filepath,
  description = description
)

# pull model results -----------------------------------------------------------
kern_draws <- data.table::fread("FILEPATH")

# format draws for saving ME ---------------------------------------------------
# converting draws to linear
kern_draws <- kern_draws %>%
  mutate(across(.cols = starts_with("draw_"), .fns = ~ nch::inv_logit(.)))

kern_draws <- kern_draws %>% mutate(
  age_group_id = nch::id_for("age_group", "Birth"),
  measure_id = nch::id_for("measure", "proportion"),
  metric_id = nch::id_for("metric", "Rate"),
  sex_id = nch::id_for("sex", "Male")
)

kern_draws <- bind_rows(kern_draws, kern_draws %>% #both sexes required for viewing map
                          mutate(sex_id = nch::id_for("sex", "Female")))

data.table::fwrite(
  kern_draws,
  "FILEPATH"
)

# Saving results ------------------------------------------------------------------------------------------------------------------------
crosswalk_version_id <- 46222
meid <- 27679
index_df <- ihme::save_results_epi(
  input_dir = "FILEPATH",
  input_file_pattern = 'all_draws.csv',
  modelable_entity_id = meid,
  description = "GBD23 - mrbrt model results for kernicterus proportions",
  measure_id = nch::id_for("measure", "proportion"),
  metric_id = nch::id_for("metric", "Rate"),
  release_id = nch::id_for("release", "GBD 2023"),
  mark_best = TRUE,
  birth_prevalence = TRUE,
  bundle_id = bundle_id,
  crosswalk_version_id = crosswalk_version_id
)
