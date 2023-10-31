library(data.table)
library(msm, lib.loc = "FILEPATH")
library(ggplot2)

source("FILEPATH/mr_brt_functions.R")


dataset <- fread("FILEPATH/csa_alc.csv")
output_dir <- "FILEPATH"
model_label <- "csa_alc_ss"

dataset <- dataset[!(dataset$study_id == 4), ]

## Run stage 1 model ##

fit1 <- run_mr_brt(
  output_dir = output_dir,
  model_label = model_label,
  data = dataset,
  mean_var = "log_effect_size",
  se_var = "log_se",
  covs = list(cov_info("cv_symptom_scale", "X")),
  remove_x_intercept = FALSE,
  method = "remL",
  study_id = "study_id",
  overwrite_previous = TRUE,
  lasso = FALSE)

saveRDS(fit1, paste0(output_dir, "/", model_label, "/model_output.RDS"))
check_for_outputs(fit1)


##end here##
results1 <- load_mr_brt_outputs(fit1)
plot_mr_brt(fit1)
df_pred1 <- expand.grid(cv_symptom_scale = c(0,1)
                        #cv_confounding_uncontrolled = c(0, 1, 2),
                        #cv_subpopulation = c(0,1), cv_selection_bias = c(0,1,2), cv_reverse_causation = c(0,1)
                        )
pred1 <- as.data.table(predict_mr_brt(fit1, newdata = df_pred1)["model_summaries"])
names(pred1) <- gsub("model_summaries.", "", names(pred1))
names(pred1) <- gsub("X_", "", names(pred1))

## Prepare stage 2 model ##

pred1[, `:=` (exp_beta = exp(Y_mean), se = (Y_mean_hi - Y_mean_lo) / (qnorm(0.975, 0, 1)))]
pred1$exp_upper <- exp(pred1$Y_mean_hi)
pred1$exp_lower <- exp(pred1$Y_mean_lo)
pred1[, `:=` (keep = cv_symptom_scale)]
pred1[, `:=` (x_cov = ifelse(cv_symptom_scale == 1, "symptom_scale",  "reference"))]
final_table <- pred1[keep == 1, .(x_cov, beta = Y_mean, exp_beta = exp(Y_mean), lower = Y_mean_lo, upper = Y_mean_hi)]

write.csv(final_table, paste0(output_dir, "/", model_label, "/final_betas.csv"), row.names=F)









