## ******************************************************************************
##
## Purpose: Determine relationship of HAQI on proportion of kernicterus
## Input:
## Output:
## Last Update: 7/15/2020
##
## ******************************************************************************

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "PATHNAME"
  h <- "PATHNAME"
} else {
  j <- "PATHNAME"
  h <- "PATHNAME"
}

library('openxlsx')
library('data.table')
library(ggplot2)
source("PATHNAME/upload_bundle_data.R")
source("PATHNAME/get_covariate_estimates.R")
source("PATHNAME/get_bundle_data.R")
source("PATHNAME/save_bundle_version.R")
source("PATHNAME/get_bundle_version.R")
source("PATHNAME/save_crosswalk_version.R")
source("PATHNAME/get_crosswalk_version.R")


# set parameters
bun_id <- 7256

# add any new data to the bundle itself. For custom bundles, you have to calculate mean
# and standard error yourself.
filepath <- paste0('PATHNAME/kernicterus_risk_gbd2020_iterative_upload.xlsx')
new_data <- data.table(read.xlsx(filepath))

new_data[, mean := cases /sample_size]
z <- qnorm(0.975)
new_data[(is.na(standard_error) | standard_error == 0), 
    standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
new_data[, `:=` (lower = mean - (1.96 * standard_error), 
                 upper = mean + (1.96 * standard_error))]
new_data[lower < 0, lower := 0]
new_data[upper > 1, upper := 1]

filepath <- paste0('PATHNAME/kernicterus_risk_gbd2020_iterative_upload_prepped.xlsx')
write.xlsx(new_data, file = filepath, sheetName = 'extraction')

upload_bundle_data(bundle_id = bun_id, decomp_step = 'iterative', gbd_round_id = 7,
                   filepath = filepath)

# save off a bundle version (no crosswalk versions for custom bundles)
bv_metadata <- save_bundle_version(bundle_id = bun_id, decomp_step = 'iterative',
                                   gbd_round_id = 7, include_clinical = NULL)


# pull the bundle version and prep for modeling
bv_id <- 29633
bv_data <- get_bundle_version(bundle_version_id = bv_id, fetch = 'all')

# drop the group reviewed data
dt <- bv_data[group_review == 1 | is.na(group_review)]

# also only keep the data that is the proportion of acute impairment, not long-term 
# impairment, which is represented in this bundle as case_name == 'kernicterus'
dt <- dt[case_name == "acute bilirubin encephalopathy"]

nrow(dt[is.na(mean)])

dt$mean <- as.numeric(dt$mean)
dt$sample_size <- as.numeric(dt$sample_size)
dt$location_id <- as.numeric(dt$location_id)
dt$year_start <- as.numeric(dt$year_start)
dt$year_end <- as.numeric(dt$year_end)
dt[, year_id := ceiling((year_start + year_end)/2)]

dt[sample_name == "extreme hyperbilirubinemia", from_jaundice := 0]
dt[sample_name == "jaundice", from_jaundice := 1]

dt[sex == 'Male', sex_id := 1]
dt[sex == 'Female', sex_id := 2]
dt[sex == 'Both', sex_id := 3]

setnames(dt, 'sample.TSB.threshold.(mg/dL)', 'TSB')


#load haqi
haqi <- get_covariate_estimates(covariate_id=1099, gbd_round_id=7, decomp_step='iterative')
setnames(haqi, 'mean_value', 'haqi')
dt3 <- merge(dt, haqi, by = c('location_id', 'year_id'), all.x = FALSE)
dt3 <- dt3[!is.na(TSB)]


# dt3[haqi < 40, haqi_level := '<40']
# dt3[haqi >= 40 & haqi < 55, haqi_level := '40-54']
# dt3[haqi >= 55 & haqi < 70, haqi_level := '55-69']
# dt3[haqi >= 70, haqi_level := '70+']
# 
# 
# pdf(file = paste0("PATHNAME"),
#          width = 11, height = 8.5)
#     
# gg <- ggplot(data = dt3[case_name == "acute bilirubin encephalopathy"],
#              aes(x = haqi, y = mean, color = TSB, size = sample_size)) +
#   geom_point() +
#   stat_smooth(data = dt3[case_name == "acute bilirubin encephalopathy"], 
#               method = lm, 
#               method.args = list(weight = sample_size)) +
#   labs(title = paste0('Proportion of Kernicterus Among Neonates with Total Serum Bilirubin Threshold (mg/dL)'),
#        x = 'TSB >',
#        y = 'Kernicterus Proportion')
# print(gg)
# 
# gg <- ggplot(data = dt3,
#        aes(x = TSB, y = mean, color = haqi, size = sample_size)) +
#   geom_point() +
#   stat_smooth(method = lm) +
#   facet_wrap(~ haqi_level)  +
#   labs(title = paste0('Proportion of Kernicterus Among Neonates with Total Serum Bilirubin Threshold (mg/dL)'),
#        x = 'TSB >',
#        y = 'Kernicterus Proportion')
# print(gg)
# dev.off()
# 
# #fit a basic linear regression
# dt_jaundice <- dt3[sample_name == 'jaundice']
# cor(dt_jaundice$log_haqi, dt_jaundice$mean)
# 
# dt_ehb <- dt3[sample_name == 'extreme hyperbilirubinemia']
# cor(dt_ehb$log_haqi, dt_ehb$mean)
# 
# lin_mod <- lm(mean ~ TSB, data = dt3[case_name == "acute bilirubin encephalopathy"], weight = sample_size)
# quad_mod <- lm(mean ~ poly(TSB,2), data = dt3, weight = sample_size)
# 
# 
# predicted_vector <- predict(quad_mod, data = c(5:40))
# predicted_df <- data.table(predicted_vector, TSB=c(1:39))

#fit a meta-regression using MR-BRT
repo_dir <- "PATHNAME"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
library(metafor, lib.loc = "PATHNAME")
library(msm, lib.loc = "PATHNAME")
library(dplyr)

# drop to just the necessary columns
data <- dt3[, .(mean, standard_error, haqi, TSB, from_jaundice)]

# logit transform the original data
# -- SEs transformed using the delta method
# -- need to apply an offset to handle data value of 0 and 1, however these
# -- points ultimately are outliered anyway, so DON'T offset the other points.
data$standard_error <- as.numeric(data$standard_error)

data[mean == 1, mean := mean - 0.000001]
data[mean == 0, mean := mean + 0.000001]
data$mean_logit <- log(data$mean / (1-data$mean))
data$se_logit <- sapply(1:nrow(data), function(i) {
  mean_i <- as.numeric(data[i, "mean"])
  se_i <- as.numeric(data[i, "standard_error"])
  deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
})

setnames(data, c('mean', 'standard_error'), c('mean_linear', 'standard_error_linear'))
setnames(data, c('mean_logit', 'se_logit'), c('mean', 'standard_error'))

fit1 <- run_mr_brt(
  output_dir = paste0("PATHNAME"), # user home directory
  model_label = "gbd20_ehb_haqi_TSB_maxL_trim10_monotonic_3knot",
  data = data,
  mean_var = "mean",
  se_var = "standard_error",
  method = 'trim_maxL',
  overwrite_previous = TRUE,
  trim_pct = 0.10,
  covs = list(cov_info("haqi", "X", degree = 3, n_i_knots = 3, r_linear = TRUE, l_linear = TRUE, bspline_mono = "decreasing"),
              cov_info("TSB", "X"))
)

check_for_outputs(fit1)
saveRDS(fit1, file="PATHNAME/fit1.RData")

#load model object
results <- load_mr_brt_outputs(fit1)
train_data <- data.table(results$train_data)

#make predictions for hemolytic
haqi <- haqi[, .(location_id, location_name, year_id, haqi)]
df_pred <- haqi[, TSB := 25]

#make predictions based on the model
#df_pred <- data.frame(intercept = 1)
df_pred <- expand.grid(haqi = seq(26,88,0.2), TSB = seq(0,40,1))
pred1 <- predict_mr_brt(fit1, newdata = df_pred)
check_for_preds(pred1)

pred_object <- load_mr_brt_preds(pred1)
preds <- data.table(pred_object$model_summaries)

#plot predictions
pdf(file = paste0("PATHNAME"),
    width = 12, height = 8.5)

ggplot() +
  geom_ribbon(data = preds[X_TSB == 35], 
              aes(x = (X_haqi), ymax = Y_mean_hi, ymin = Y_mean_lo), alpha = 0.5, fill = "grey") +
  geom_line(data = preds[X_TSB == 35], 
            aes(x = (X_haqi), y = Y_mean), color = 'dodgerblue1') + 
  geom_ribbon(data = preds[X_TSB == 25], 
              aes(x = (X_haqi), ymax = Y_mean_hi, ymin = Y_mean_lo), alpha = 0.5, fill = "grey") +
  geom_line(data = preds[X_TSB == 25], 
            aes(x = (X_haqi), y = Y_mean), color = 'dodgerblue4') + 
  geom_ribbon(data = preds[X_TSB == 15], 
              aes(x = (X_haqi), ymax = Y_mean_hi, ymin = Y_mean_lo), alpha = 0.5, fill = "grey") +
  geom_line(data = preds[X_TSB == 15], 
            aes(x = (X_haqi), y = Y_mean)) +
  geom_point(data=train_data, 
             aes(x = haqi, y = mean, shape = as.factor(w), size = 1 - standard_error, color = TSB), 
             alpha = 0.8) + 
  scale_shape_manual(values=c(13, 19), name = "Trimming", labels = c("Trimmed", "Included")) +
  guides(size = FALSE) +
  xlab("HAQI") + ylab("Kernicterus Proportion") + ggtitle("Predicted Kernicterus Proportion by HAQI for TSB Levels of 35, 25, and 15")

#transform back into linear space
#exp(x1)/(1+exp(x1)

preds[, Y_mean_linear := (exp(Y_mean)/(1 + exp(Y_mean)))]
preds[, Y_mean_hi_linear := (exp(Y_mean_hi)/(1 + exp(Y_mean_hi)))]
preds[, Y_mean_lo_linear := (exp(Y_mean_lo)/(1 + exp(Y_mean_lo)))]

ggplot() +
  geom_ribbon(data = preds[X_TSB == 35], 
              aes(x = (X_haqi), ymax = Y_mean_hi_linear, ymin = Y_mean_lo_linear), alpha = 0.5, fill = "grey") +
  geom_line(data = preds[X_TSB == 35], 
            aes(x = (X_haqi), y = Y_mean_linear), color = 'dodgerblue1') + 
  geom_ribbon(data = preds[X_TSB == 25], 
              aes(x = (X_haqi), ymax = Y_mean_hi_linear, ymin = Y_mean_lo_linear), alpha = 0.5, fill = "grey") +
  geom_line(data = preds[X_TSB == 25], 
            aes(x = (X_haqi), y = Y_mean_linear), color = 'dodgerblue4') + 
  geom_ribbon(data = preds[X_TSB == 15], 
              aes(x = (X_haqi), ymax = Y_mean_hi_linear, ymin = Y_mean_lo_linear), alpha = 0.5, fill = "grey") +
  geom_line(data = preds[X_TSB == 15], 
            aes(x = (X_haqi), y = Y_mean_linear)) +
  geom_point(data=train_data, 
             aes(x = haqi, y = mean_linear, shape = as.factor(w), size = 1 - (standard_error_linear), color = TSB), 
             alpha = 0.8) + 
  scale_shape_manual(values=c(13, 19), name = "Trimming", labels = c("Trimmed", "Included")) +
  guides(size = FALSE) +
  xlab("HAQI") + ylab("Kernicterus Proportion") + ggtitle("Predicted Kernicterus Proportion by HAQI for TSB Levels of 35, 25, and 15")

dev.off()


#examples of histograms
ggplot() + geom_histogram(data = train_data, stat = "bin", aes(x = mean), binwidth = 0.05)

ggplot() + geom_histogram(data = train_data, stat = "bin", aes(x = log(mean)), binwidth = 0.1)

ggplot() + geom_histogram(data = train_data, stat = "bin", aes(x = logit(mean)), binwidth = 0.1)
