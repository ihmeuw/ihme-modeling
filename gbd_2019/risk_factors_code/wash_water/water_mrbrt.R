# sanitation RR calculation
library(data.table)
library(openxlsx)
library(magrittr)
source("FILEPATH")

#### network meta-analysis ---------------
fit2 <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = "water_network",
  data = "FILEPATH",
  mean_var = "value",
  se_var = "se",
  covs = list(
    cov_info("improved", "X"),
    cov_info("piped", "X"),
    cov_info("filter", "X"),
    cov_info("solar", "X"),
    cov_info("hq_piped", "X"),
    cov_info("cv_exposure_population", "Z"),
    cov_info("cv_exposure_selfreport", "Z"),
    cov_info("cv_exposure_study", "Z"),
    cov_info("cv_outcome_unblinded", "Z"),
    cov_info("cv_confounding_nonrandom", "Z"),
    cov_info("cv_confounding_uncontrolled", "Z"),
    cov_info("cv_selection_bias", "Z")
  ),
  remove_x_intercept = TRUE,
  study_id = "nid",
  overwrite_previous = TRUE,
  trim_pct = 0.1,
  method = "trim_maxL"
)

results2 <- load_mr_brt_outputs(fit2)
coefs2 <- results2$model_coefs

plot_mr_brt(pred1)
