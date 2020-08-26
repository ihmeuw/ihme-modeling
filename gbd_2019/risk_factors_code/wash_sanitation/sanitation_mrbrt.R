# sanitation RR calculation
library(data.table)
library(openxlsx)
library(magrittr)
source("FILEPATH")

#### network meta-analysis ---------------

san_network <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = "sanitation_network",
  data = "FILEPATH",
  mean_var = "log_mean",
  se_var = "log_se",
  covs = list(
    cov_info("improved", "X"),
    cov_info("sewer", "X"),
    cov_info("cv_subpopulation", "Z"),
    cov_info("cv_exposure_selfreport", "Z"),
    cov_info("cv_confounding_nonrandom", "Z"),
    cov_info("cv_confounding_uncontrolled", "Z")
  ),
  study_id = "nid",
  remove_x_intercept = TRUE,
  overwrite_previous = TRUE,
  trim_pct = 0.1,
  method = "trim_maxL"
)

san_network_results <- load_mr_brt_outputs(san_network)
san_network_coefs <- san_network_results$model_coefs
san_network_metadata <- san_network_results$input_metadata

plot_mr_brt(san_network)