# #
# # test_spline_cascade.R
# #
# # September 2020
# #
#
#
# library(dplyr)
# library(mrbrt001, lib.loc = "FILEPATH")
#
# rm(list = ls())
#
# dev_version <- "35"
# base_output_dir <- "FILEPATH"
# output_dir <- file.path(base_output_dir, dev_version)
#
# df <- read.csv(file.path(output_dir, "df_deaths_byageloc_preppedcumulative.csv"), stringsAsFactors = FALSE) %>%
#   mutate(
#     is_indonesia = ifelse(substr(data_filename,1,3) == "IDN", TRUE, FALSE) ) %>%
#   filter(!is_indonesia & !location_id == 161) %>% # remove indonesia and bangladesh as outliers
#   filter(location_id %in% c(unique(location_id)[1:10], 6, 61))
#
#
# dat_loc <- MRData()
# dat_loc$load_df(
#   data = df,
#   col_obs = "logit_mort", col_obs_se = "logit_mort_se",
#   col_covs = list("age_start", "age_end"), col_study_id = "location_id"
# )
#
# # -- using the knot locations from a previous ensemble spline model
# best_knots1 <- readRDS(file.path(output_dir, "mort_optimal_knot_locs.RDS"))
#
# #####
#
# tmp_beta_prior <- rbind(c(rep(-Inf, 4), -2), c(rep(Inf, 4), -2))
#
# mod3 <- MRBRT(
#   data = dat_loc,
#   cov_models = list(
#     LinearCovModel("intercept", use_re = TRUE), #, prior_gamma_uniform = array(c(0, 0)) ),
#     LinearCovModel(
#       alt_cov = list("age_start", "age_end"),
#       use_re = TRUE,
#       use_spline = TRUE,
#       # spline_knots = array(seq(0, 1, by = 0.2)),
#       spline_knots = array(best_knots1),
#       spline_degree = 3L,
#       # spline_knots_type = 'frequency',
#       spline_knots_type = 'domain',
#       spline_l_linear = FALSE,
#       spline_r_linear = TRUE,
#       prior_spline_monotonicity = 'increasing',
#       prior_spline_monotonicity_domain = tuple(0.1, 1.0)
#     )
#   )
#   # inlier_pct = 0.95
# )
#
# mod3$fit_model(inner_print_level = 5L, inner_max_iter = 2000L)
#
# fit1 <- run_spline_cascade(
#   stage1_model_object = mod3,
#   df = df,
#   col_obs = "logit_mort",
#   col_obs_se = "logit_mort_se",
#   col_study_id = "location_id",
#   stage_id_vars = c("super_region_name", "location_id"),
#   # stage_id_vars = c("super_region_name"),
#   inner_print_level = 2L,
#   thetas = c(1,1),
#   output_dir = "/home/j/temp/reed/misc/",
#   model_label = "mrbrt_cascade_test4",
#   overwrite_previous = TRUE
# )
#
#
# df_loc <- df %>%
#   select(super_region_name, location_id) %>%
#   filter(!duplicated(.)) %>%
#   as.data.frame(.) %>%
#   mutate(
#     super_region_name = as.character(super_region_name),
#     location_id = as.character(location_id)
#   )
#
# df_pred <- expand.grid(
#   stringsAsFactors = FALSE,
#   age_start = seq(0, 100, by = 10),
#   location_id = c(NA, "6", "61")) %>%
#   left_join(df_loc) %>%
#   mutate(
#     age_end = age_start
#     # super_region_name = NA
#     # super_region_name = "High-income"
#   )
#
# preds1 <- predict_spline_cascade(
#   fit = fit1,
#   newdata = df_pred
# )
#
# with(preds1, plot(age_start, pred, type = "n", xlab = "Age"))
# for (id in unique(preds1$cascade_prediction_id)) {
#   with(
#     filter(preds1, cascade_prediction_id == id),
#     lines(
#       x = age_start,
#       y = pred,
#       lwd = ifelse(cascade_prediction_id == "stage1__stage1", 3, 1),
#       col = ifelse(cascade_prediction_id == "stage1__stage1", "darkorange", "black")
#     )
#   )
# }
#
#
#
#
#
