################################################################################
## Purpose: Save results for EHB total draws
## Input:   EHB draws saved from sum_ehb.R
## Output:  Model version for MEID 19812
################################################################################

index_df <- ihme::save_results_epi(
  input_dir = "FILEPATH",
  input_file_pattern = '{location_id}.csv',
  modelable_entity_id = 19812,
  description = "GBD23 - all new inputs and revised pipeline - with rhneg using lme4, maori adjusted, updated emr",
  measure_id = nch::id_for("measure", "prevalence"),
  metric_id = nch::id_for("metric", "Rate"),
  release_id = nch::id_for("release", "GBD 2023"),
  mark_best = TRUE,
  birth_prevalence = TRUE,
  bundle_id = 458,
  crosswalk_version_id = 46994
)
