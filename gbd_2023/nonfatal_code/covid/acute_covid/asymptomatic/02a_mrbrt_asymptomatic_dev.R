#--------------------------------------------------------------
# Name: NAME (USERNAME)
# Date: 27 Feb 2021
# Project: GBD nonfatal COVID
# Purpose: estimate % asymptomatic among mild/moderate cases and among hospital cases
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
library(data.table) 
library(ggplot2) 
library(openxlsx) 
library(reticulate) 
use_python("FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")
library(plyr)
library(msm)


folder <- "FILEPATH"
data <- "FILEPATH/extraction_asymptomatic_11Sept2021.xlsx"
outputfolder <- "FILEPATH"


version <- 17
save_final <- 1
age_pattern <- 0

# 17: random sample studies only
# 16: DATA CHANGE.  from PCR-based, to seroprevalence-based studies.  
    # 11 Sept 2021.  random and nonrandom studies included, with bias cv on latter
# 15: incorporate age pattern from Bi et al
# 14: no minCV
# 13: minCV=0.2, added Wuhan seroprev study
# 12: include China data, no minCV
# 11: sample size>20, minCV=0.5
# 10: quick adjust all data using 33.6% missed asymptomatic cases by PCR, minCV=0.5
# 9: exclude Luo with very low % asymptomatic, Wu with household contacts of 
    # hospitalized COVID cases
# 8: add articles from ITA village, articles that followed patients until 
    # negative PCR test
# 7: add article from Japan
# 6: exclude articles with 7-day follow-up time or until negative PCR test.  
    # only included 14-day follow-up time studies
# 5: exclude #1 and hospital data, exclude if sample size<10.  younger adults
# 4: no covariates, assume % asymptomatic constant across age
# 3: exclude #1 and hospital data, exclude if sample_size<10.  
    # children, younger adults, older adults
# 2: exclude #1 and hospital data.  children, younger adults, older adults
# 1: exclude ITA village, healthcare workers, blood donors.  hospital, 
    # children, younger adults, older adults


dataset_raw <- read.xlsx(data, colNames=TRUE)
dataset <- data.table(dataset_raw)

if (age_pattern == 1) {
  or_data <- read.xlsx(
    file.path(folder, "asymptomatic age pattern calculation.xlsx"), 
    colNames=TRUE, sheet='extraction')
}

################################################################
# DATA PREP
################################################################

table(dataset$sample_characteristics)
table(dataset$sample_age_group)
table(dataset$symptom_cluster)

dataset[, exclude := 0]


if (version == 17) {
  dataset[cv_nonrandom == 1, exclude := 1]
}

dataset <- dataset[exclude == 0]
df <- dataset[, c("author", "cases", "sample_size", "mean", "standard_error", 
                  "cv_nonrandom")]

logitvals <- cw$utils$linear_to_logit(mean = array(df$mean), sd = array(df$standard_error))
logitmean <- logitvals[1]
logitse <- logitvals[2]
df$logitmean <- logitmean
df$logitse <- logitse


df[, logitmean := logit_mean]
df[, logitse := logit_se]

################################################################################
#   run model with all data with multiple follow up times, to get duration
#      and beta on follow_up_days to use as prior for cluster-specific models
################################################################################

# set up folder
model_dir <- paste0("v", version)
dir.create(file.path(outputfolder, model_dir))
dir.create(file.path(outputfolder, "plots"))

# set up data
mr_df <- mr$MRData()

cvs <- list("cv_nonrandom")

mr_df$load_df(
  data = df, col_obs = "logitmean", col_obs_se = "logitse",
  col_covs = cvs, col_study_id = "author")

model <- mr$MRBRT(
  data = mr_df,
  cov_models =list(
    mr$LinearCovModel("intercept", use_re = TRUE),
    mr$LinearCovModel("cv_nonrandom", use_re = FALSE)
  ),
  inlier_pct = 1)


# fit model
model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

(coeffs <- rbind(model$cov_names, model$beta_soln))
write.csv(coeffs, file.path(outputfolder, model_dir, "coeffs.csv"))

# save model object
py_save_object(object = model, 
               filename = file.path(outputfolder, model_dir, "mod1.pkl"), 
               pickle = "dill")

# make predictions for full year
predict_matrix <- data.table(intercept = model$beta_soln[1], cv_nonrandom = 0)

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

draws_raw[, cv_nonrandom := NULL]

draws <- melt(data = draws_raw, id.vars = c("intercept"))

setnames(draws, "variable", "draw")
setnames(draws, "value", "proportion")
draws[, proportion := exp(proportion) / (1 + exp(proportion))]

write.csv(draws, file = file.path(outputfolder, model_dir, "predictions_draws.csv"))

predict_matrix$pred <- model$predict(predict_data, sort_by_data_id = TRUE)

predict_matrix[, pred := exp(pred) / (1 + exp(pred))]
predict_matrix[, pred_lo := apply(draws_raw, 1, function(x) quantile(x, 0.025))]
predict_matrix[, pred_lo := exp(pred_lo) / (1 + exp(pred_lo))]
predict_matrix[, pred_hi := apply(draws_raw, 1, function(x) quantile(x, 0.975))]
predict_matrix[, pred_hi := exp(pred_hi) / (1 + exp(pred_hi))]

used_data <- cbind(
  model$data$to_df(), 
  data.frame(w = model$w_soln))

used_data <- as.data.table(used_data)
rr_summaries <- copy(predict_matrix)

rr_summaries
rr_summaries$gamma <- mean(samples[[2]])
write.csv(rr_summaries, 
          file = file.path(outputfolder, model_dir, "predictions_summary.csv"))

used_data[, obs_raw := exp(obs) / (1 + exp(obs))]

used_data <- used_data[order(obs)]
df <- df[order(logitmean)]

used_data$se <- df$standard_error
used_data[, weight := 1 / (15 * se)]

rr_summaries$study_id <- " ESTIMATE"

used_data[, obs_lo := obs - 2 * obs_se]
used_data[, obs_lo := exp(obs_lo) / (1 + exp(obs_lo))]
used_data[, obs_hi := obs + 2 * obs_se]
used_data[, obs_hi := exp(obs_hi) / (1 + exp(obs_hi))]


plot <- ggplot(data = rr_summaries, 
               aes(x = study_id, y = pred, ymin = pred_lo, ymax = pred_hi)) +
  geom_pointrange(color = "blue") +
  geom_pointrange(data = used_data, 
                  aes(x = study_id, y = obs_raw, ymin = obs_lo, ymax = obs_hi)) +
  coord_flip() +
  ylab("Proportion") +
  xlab("Study") +
  ggtitle("Proportion asymptomatic among SARS-CoV-2 infections") +
  theme_minimal() +
  theme(axis.line = element_line(colour = "black")) +
  scale_y_continuous(expand = c(0, 0.02), breaks = seq(0, 1, 0.1), 
                     limits = c(0, 1)) +
  guides(fill = FALSE)

plot

ggsave(plot, 
       filename = file.path(
         outputfolder, "plots", 
         paste0("v", version, "_prop_asymptomatic.pdf")), 
       width = 6, height = 4)


final_draws <- reshape(draws, idvar = c("intercept"), timevar = "draw", 
                       direction = "wide")

final_draws$intercept <- NULL

setnames(final_draws, paste0("proportion.draw_", c(0:999)), 
         paste0("draw_", c(0:999)))


head(final_draws)
final_draws$age <- NULL
final_draws[1:5, 1:5]
final_draws[1:5, 998:ncol(final_draws)]

if(save_final==1) {
  write.csv(final_draws, 
            file = file.path(outputfolder, "final_asymptomatic_proportion_draws.csv"))
}
