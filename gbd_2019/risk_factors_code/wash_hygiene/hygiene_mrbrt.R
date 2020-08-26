# handwashing RR calculation

library(data.table)
library(openxlsx)
library(magrittr)
source("FILEPATH")

## diarrhea
# run mr-brt
hyg_dia <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = "hyg_diarrhea",
  data = "FILEPATH",
  mean_var = "log_mean",
  se_var = "log_se",
  covs = list(
    cov_info("cv_subpopulation", "Z"),
    cov_info("cv_exposure_selfreport", "Z"),
    cov_info("cv_exposure_study", "Z"),
    cov_info("cv_outcome_unblinded", "Z"),
    cov_info("cv_confounding_nonrandom", "Z"),
    cov_info("cv_confounding_uncontrolled", "Z"),
    cov_info("cv_selection_bias", "Z")
  ),
  study_id = "nid",
  overwrite_previous = TRUE,
  trim_pct = 0.1,
  method = "trim_maxL"
)

hyg_dia_results <- load_mr_brt_outputs(hyg_dia)
hyg_dia_coefs <- hyg_dia$model_coefs
hyg_dia_metadata <- hyg_dia$input_metadata

plot_mr_brt(hyg_dia)

## lri
# run mr-brt
hyg_lri <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = "hyg_lri",
  data = "FILEPATH",
  mean_var = "log_mean",
  se_var = "log_se",
  covs = list(
    cov_info("cv_subpopulation", "Z"),
    cov_info("cv_exposure_selfreport", "Z"),
    cov_info("cv_exposure_study", "Z"),
    cov_info("cv_outcome_selfreport", "Z"),
    cov_info("cv_outcome_unblinded", "Z"),
    cov_info("cv_confounding_nonrandom", "Z"),
    cov_info("cv_confounding_uncontrolled", "Z"),
    cov_info("cv_selection_bias", "Z")
  ),
  study_id = "nid",
  overwrite_previous = TRUE,
  trim_pct = 0.1,
  method = "trim_maxL"
)

hyg_lri_results <- load_mr_brt_outputs(hyg_lri)
hyg_lri_coefs <- hyg_lri$model_coefs
hyg_lri_metadata <- hyg_lri$input_metadata

plot_mr_brt(hyg_lri)