# Run crosswalk for self-harm

rm(list=ls())

library(ggplot2)
library(reticulate)
# Filepath to use shared code for IHME crosswalk package
reticulate::use_python("FILEPATH")
cw <- import("crosswalk")

# Source IHME shared functions
invisible(sapply(list.files("FILEPATH", full.names = T), source))

matches <- fread("FILEPATH")

# Quick plot to visualize matches
ggplot(matches) +
  geom_point(aes(x = age_midpoint, y = log_diff, shape = factor(location_id), color = factor(location_id)), size = 2) + 
  theme_bw()

# Format data for meta-regression; pass in data.frame and variable names
dat1 <- cw$CWData(
  df = matches,
  obs = "log_diff",            # matched differences in log space
  obs_se = "log_diff_se",      # SE of matched differences in log space
  alt_dorms = "dorm_alt",      # variable for the alternative def
  ref_dorms = "dorm_ref",      # variable for the reference def
  covs = list("age_midpoint"),       # list of covariate columns
  study_id = "location_id"     # study_id for variable (group) for random effects
)

# Find knots based on data density
# Want 4 interior knots (6 total)
quantile(matches$age_midpoint, probs = seq(0, 1, length.out = 6))

# Setup covariates
# Spline on age using knots from above
cov_models <- list(cw$CovModel(cov_name = "age_midpoint", 
                               spline = cw$XSpline(r_linear = TRUE,
                                                   #l_linear = TRUE,
                                                   knots = c(0, 22, 32, 47, 57, 100), # outer knots must encompass the data
                                                   degree = 2L # polynomial order (1=linear, 2=quadratic, 3=cubic)
                               )), 
                   cw$CovModel("intercept"))

# Create the CWModel object
fit1 <- cw$CWModel(cwdata = dat1,           # result of CWData() function call
                   obs_type = "diff_log",   # must be "diff_logit" or "diff_log"
                   cov_models = cov_models, # fom above
                   gold_dorm = "inpatient"  # level of "ref_dorms" that's the gold standard
)

# Add trimming (inlier_pct = 0.9 corresponds to 10% trim)
fit1$fit(inlier_pct = 0.9)

# Save model object
py_save_object(object = fit1,
               filename = paste0("FILEPATH/bundle_X_xwalk_model_",
                                 Sys.Date(),
                                 ".pkl"),
               pickle = "dill")



# Create dummy dataset to visualize model results
df_pred <- matches

# Adjust original values
df_pred[, c("inc2", "inc2_se", "diff", "diff_se", "data_id")] <- fit1$adjust_orig_vals(
  df = df_pred,                # original data with obs to be adjusted
  orig_dorms = "dorm_alt",     # name of column with (all) def/method levels
  orig_vals_mean = "inc_alt",  # original mean
  orig_vals_se = "inc_se_alt"  # standard error of original mean
)

# Use weights to determine what got trimmed
df_pred[!is.na(inc_ref), trim := ifelse(fit1$w==1, "no", "yes")]
df_pred[is.na(inc_ref), trim := "no"]

# Prep predictions
df_dedupe <- df_pred[, c("age_midpoint", "diff")]
df_dedupe <- unique(df_dedupe)

# Make plot, showing input data with model prediction line overlaid
ggplot() + 
  geom_point(data = df_pred, aes(x = age_midpoint, y = log_diff, color = trim, size = log_diff_se, shape = factor(location_id)), alpha = .5) +
  geom_line(data = df_dedupe, aes(x = age_midpoint, y = diff)) +
  labs(x = "Age", y = "Log Ratio (outpatient/inpatient)", color = "Trimmed?", size = "Log SE", shape = "location") +
  theme_bw() +
  scale_shape_manual(values = c(17,16)) +
  scale_color_brewer(palette = "Dark2") +
  ggtitle(paste0("Self-harm (parent) crosswalk model results overlaid on input data, ", Sys.Date()))
ggsave(paste0("FILEPATH/model_fit_bundle_X_", Sys.Date(), ".pdf"), width = 8, height = 6)

### Calculate outpatient coefficients (transform to normal space)
# Get unique age midpoints
ages <- get_age_metadata(release_id = 16)
ages <- ages[,.(age_group_id,age_group_years_start,age_group_years_end)]
colnames(ages) <- c("age_group_id","age_start","age_end")

# Apply age restriction (ages 10+)
ages <- ages[age_start > 9]

# Prep to calculate age_midpoints
ages <- ages[, age_end := age_end - 1]
ages <- ages[age_end == 124, age_end := 99]
ages <- ages[, age_midpoint := (age_start + age_end)/2]

# Make dummy columns in order for adjust_orig_vals to work
ages <- ages[, mean := 1]
ages <- ages[, se := 0.1]
ages <- ages[, dorm := "outpatient"]

# Predictions dataset
pred <- ages

# Calculate
pred[, c("mean2", "mean2_se", "diff", "diff_se", "data_id")] <- fit1$adjust_orig_vals(
  df = pred,            # original data with obs to be adjusted
  orig_dorms = "dorm", # name of column with (all) def/method levels
  orig_vals_mean = "mean",  # original mean
  orig_vals_se = "se"  # standard error of original mean
)

# Transform to normal space (exponentiate)
pred <- pred[, out_coeff := exp(diff)]

# Visualize
ggplot(pred) +
  geom_point(aes(x = age_midpoint, y = out_coeff), color = "dodgerblue3") +
  geom_line(aes(x = age_midpoint, y = out_coeff), color = "dodgerblue3") +
  ylim(0,3) +
  geom_abline(slope = 0, intercept = 1, linetype = "dashed", color = "red") +
  theme_bw() +
  ylab("outpatient/inpatient") +
  ggtitle(paste0("Self-harm (parent) outpatient coefficients, ", Sys.Date()))
ggsave(paste0("FILEPATH/inj_suicide_", Sys.Date(), ".pdf"), width = 8, height = 6)

# Coefficients
coeffs <- pred[,.(age_group_id, age_midpoint, age_start, out_coeff)]
fwrite(coeffs, "FILEPATH/inj_suicide.csv")
