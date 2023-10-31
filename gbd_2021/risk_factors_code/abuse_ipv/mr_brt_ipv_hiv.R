library(data.table)
library(msm, lib.loc = "FILEPATH")
library(ggplot2)

source("FILEPATH/mr_brt_functions.R")

dataset <- fread("FILEPATH")
output_dir <- "FILEPATH"
model_label <- "ipv_hiv"

## Run stage 1 model ##

fit1 <- run_mr_brt(
  output_dir = output_dir,
  model_label = model_label,
  data = dataset,
  mean_var = "log_effect_size",
  se_var = "log_se",
  remove_x_intercept = FALSE,
  method = "remL",
  study_id = "study_id",
  overwrite_previous = TRUE,
  lasso = FALSE)

saveRDS(fit1, paste0(output_dir, "/", model_label, "/model_output.RDS"))







