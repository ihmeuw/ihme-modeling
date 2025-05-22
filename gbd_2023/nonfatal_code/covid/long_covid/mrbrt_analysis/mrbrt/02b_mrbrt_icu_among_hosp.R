#--------------------------------------------------------------
# Name: NAME USERNAME
# Date: 27 Feb 2021
# Project: GBD nonfatal COVID
# Purpose: estimate % asymptomatic among mild/moderate cases and among hospital 
#   cases
#--------------------------------------------------------------

# setup -------------------------------------------------------

# clear workspace
rm(list=ls())
setwd("FILEPATH")

# map drives
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH'
  h_root <- '~/'
} else {
  j_root <- 'FILEPATH/'
  h_root <- 'FILEPATH/'
}


# load packages
pacman::p_load(data.table, DBI, openxlsx, gtools)
library(ggplot2)
library(reticulate) 
use_python("FILEPATH")
cw <- import("crosswalk")
mr <- import("mrtool")
library(plyr)
library(msm)

folder <- "FILEPATH"
data <- paste0(folder, "/extraction_icu_hospital.xlsx")
outputfolder <- "FILEPATH"

version <- 1
save_final <- 1

# 1: inlier 0.9

dataset_raw <- read.xlsx(data, colNames=TRUE)
dataset <- data.table(dataset_raw)


################################################################
# DATA PREP
################################################################

# exclude data points marked to exclude
dataset <- dataset[!grepl('exclude', exclude), ]

dataset <- dataset[, standard_error := sqrt(
  (ratio * (1 - ratio)) / hospital_admissions)]

logvals <- cw$utils$linear_to_log(
  mean = array(dataset$ratio), 
  sd = array(dataset$standard_error))

logmean <- logvals[1]
logse <- logvals[2]
dataset$logmean <- logmean
dataset$logse <- logse

dataset$one <- 1

dataset <- dataset[, c('location_name', 'hospital_admissions', 'icu_admissions', 
                       'ratio', 'standard_error', 'logmean', 'logse', 'one')]

################################################################################
#   run model with all data with multiple follow up times, to get duration
#      and beta on follow_up_days to use as prior for cluster-specific models
################################################################################

# set up folder
model_dir <- paste0("v", version, "/")
dir.create(paste0(outputfolder))
dir.create(paste0(outputfolder, model_dir))
dir.create(paste0(outputfolder, "plots"))

# set up data
mr_df <- mr$MRData()

cvs <- list("one")

mr_df$load_df(
  data = dataset, col_obs = "logmean", col_obs_se = "logse",
  col_covs = cvs, col_study_id = "location_name")

model <- mr$MRBRT(
  data = mr_df,
  cov_models =list(
    mr$LinearCovModel("intercept", use_re = TRUE),
    mr$LinearCovModel("one", use_re = FALSE)
  ),
  inlier_pct = 0.9)


# fit model
model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

(coeffs <- rbind(model$cov_names, model$beta_soln))
write.csv(coeffs, file.path(outputfolder, model_dir, "coeffs.csv"))

# save model object
py_save_object(object = model, filename = 
                 file.path(outputfolder, model_dir, "mod1.pkl"), pickle = "dill")

# make predictions for full year
predict_matrix <- data.table(intercept = model$beta_soln[1], one=1)

predict_data <- mr$MRData()
predict_data$load_df(
  data = predict_matrix,
  col_covs=cvs)

n_samples <- 1000L
samples <- model$sample_soln(sample_size = n_samples)

draws_raw <- model$create_draws(
  data = predict_data,
  beta_samples = samples[[1]],
  gamma_samples = samples[[2]],
  random_study = TRUE,
  sort_by_data_id = TRUE)

# write draws for pipeline
draws_raw <- data.table(draws_raw)
draws_raw <- cbind(draws_raw, predict_matrix)
setnames(draws_raw, paste0("V", c(1:1000)), paste0("draw_", c(0:999)))

draws[, one := NULL]

draws <- melt(data = draws_raw, id.vars = c("intercept"))
setnames(draws, "variable", "draw")
setnames(draws, "value", "proportion")
draws[, proportion := exp(proportion)]

write.csv(draws, file =file.path(outputfolder, model_dir, "predictions_draws.csv"))

predict_matrix[, pred := model$predict(predict_data, sort_by_data_id = TRUE)]
predict_matrix[, pred := exp(pred)]
predict_matrix[, pred_lo := apply(draws_raw, 1, function(x) quantile(x, 0.025))]
predict_matrix[, pred_lo := exp(pred_lo)]
predict_matrix[, pred_hi := apply(draws_raw, 1, function(x) quantile(x, 0.975))]
predict_matrix[, pred_hi := exp(pred_hi)]

used_data <- cbind(
  model$data$to_df(), 
  data.frame(w = model$w_soln))

used_data <- as.data.table(used_data)
rr_summaries <- copy(predict_matrix)

rr_summaries
rr_summaries$gamma <- mean(samples[[2]])
write.csv(rr_summaries, file = file.path(outputfolder, model_dir, 
                                      "predictions_summary.csv"))


used_data[, obs_raw := exp(obs)]
used_data <- used_data[order(obs)]

dataset <- dataset[order(logmean)]

used_data[, se := dataset$standard_error]
used_data[, weight := 1 / (15 * se)]

rr_summaries$study_id <- " ESTIMATE"

used_data[, obs_lo := obs - 2 * obs_se]
used_data[, obs_lo := exp(obs_lo)]
used_data[, obs_hi := obs + 2 * obs_se]
used_data[, obs_hi := exp(obs_hi)]


plot <- ggplot(
  data = rr_summaries,
  aes(x = study_id, y = pred, ymin = pred_lo, ymax = pred_hi)
) +
  geom_pointrange(color = "blue") +
  geom_pointrange(
    data = used_data[w == 1],
    aes(x = study_id, y = obs_raw, ymin = obs_lo, ymax = obs_hi)
  ) +
  geom_pointrange(
    data = used_data[w == 0],
    aes(x = study_id, y = obs_raw, ymin = obs_lo, ymax = obs_hi), shape = 1
  ) +
  coord_flip() +
  ylab("Proportion") +
  xlab("Study") +
  ggtitle("Proportion ICU among hospital admissions") +
  theme_minimal() +
  theme(axis.line = element_line(colour = "black")) +
  scale_y_continuous(expand = c(0, 0.02), breaks = seq(0, 1, 0.1), 
                     limits = c(0, 1)) +
  guides(fill = FALSE)

plot

ggsave(plot, 
       filename = file.path(outputfolder,"plots", 
                            paste0("v", version, "_prop_icu_among_hosp.pdf")), 
       width = 6, height = 4)


final_draws <- draws[, c('draw', 'proportion')]
head(final_draws)

if(save_final==1) {
  write.csv(final_draws,
    file = file.path(outputfolder, "final_prop_icu_among_hosp.csv")
  )
}



