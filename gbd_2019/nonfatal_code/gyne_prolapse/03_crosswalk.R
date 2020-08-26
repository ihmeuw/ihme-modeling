

#######################################################################################################
## Prepare data for network
## Step 1. Find matches and do single crosswalks per comparison
## Step 2. Prepared dataset for network
## Step 3. Run network meta-analysis with MR-BRT with logit transformation
## Step 4. Apply ratios
## Step 5. Save datasets with mean original and mean adjusted
## Step 6. Save datasets with adjusted mean only ready to upload
########################################################################################################
##
## Perform a crosswalk with logit transformation
##
## Step 1. Find matches
##
## Step 2. Logit transform the mean prevalence of the reference and alternative definition
## logit(p) = log(p/1-p) = log(p) - log(1-p)
## invlogit function = exp(x)/(1 + exp(x)
##
## Step 3. Estimate the "ratio"  =  difference  of the logit transformed  mean prevalence reference and alternative definition
## diff_logit = logit_ref - logit_alt
##
## Step 4. Logit transform the standard error of the alternative and reference means
##
## Step 5. Calculate the standard error of the logit difference
####################################################################

fib_network <- fread(FILEPATH) # bundle data with matches for comparison

## Create covariates for the network

# dummy k = 1 if k is the numerator = mean_1, 0 otherwise
fib_network <- fib_network %>%
mutate(comparison = paste0(dx_1, "/", dx_2))

fib_network <- fib_network %>%
mutate(cv_clinical = if_else(dx_1 == "clinical", 1, 0)) %>%
mutate(cv_self_report = if_else(dx_1 == "self_report", 1, 0)) %>%
mutate(cv_symptomatic =  if_else(dx_1 == "symp", 1,0))



#dummy k =  -1  if k is the denominator
fib_network$cv_clinical[(fib_network$dx_2 == "clinical")] <- -1
fib_network$cv_self_report[(fib_network$dx_2 == "self_report")] <- -1
fib_network$cv_symptomatic[(fib_network$dx_2 == "symp")] <- -1

View(fib_network)

write.xlsx(fib_network,
file = paste0(save_dir, "FILEPATH", bun_id, "_net_prep.xlsx"),
row.names = FALSE)

#######################################################################################################################
###############################################################################################################

#matched_merge <- fib_network

fib_network <- fread(paste0("/FILEPATH, bun_id, "_net_prep_exc_means.csv"))


# Exclude observations with mean < 0.005
matched_merge <- fib_network %>%
filter(mean_1 >0.005) %>%
filter(mean_2 >0.005)

write.xlsx(matched_merge,
file = paste0(save_dir, "network_prep/02_input_mrbrt_network/", bun_id, "_net_prep_exc_means_s4.xlsx"),
row.names = FALSE)


dat_metareg <- as.data.frame(matched_merge)
extra_vars <- c("cv_clinical", "cv_self_report", "cv_symptomatic")


# Select variables of interest
logit_metareg <- as.data.frame(matched_merge) %>%
select(age, location_name.x, mean_1, se_1,  mean_2, se_2, mean, standard_error, extra_vars)


# Logit transform the means and compute the "ratio" == difference in logit space
logit_metareg <- (logit_metareg) %>%
mutate (logit_mean_alt = logit(mean_1)) %>%
mutate (logit_mean_ref = logit(mean_2)) %>%
mutate(logit_diff = logit_mean_alt - logit_mean_ref)


# Logit transform the SE of the altermative mean
logit_metareg$logit_se_alt<- sapply(1:nrow(logit_metareg), function(a) {
    ratio_a <- logit_metareg[a, "mean_2"]
    ratio_se_a <- logit_metareg[a, "se_2"]
    deltamethod(~log(x1), ratio_a, ratio_se_a^2)
})


# Logit transform the SE of the reference mean
logit_metareg$logit_se_ref<- sapply(1:nrow(logit_metareg), function(r) {
    ratio_r <- logit_metareg[r, "mean_1"]
    ratio_se_r <- logit_metareg[r, "se_1"]
    deltamethod(~log(x1), ratio_r, ratio_se_r^2)
})


# Calculate the standard error of the ratio = logit  difference
logit_metareg$logit_se_diff <- sqrt(logit_metareg$logit_se_alt^2 + logit_metareg$logit_se_ref^2)

# Standardize variable names
ratio_var <- "logit_diff"
ratio_se_var <- "logit_se_diff"
extra_vars <- extra_vars
metareg_vars <- c(ratio_var, ratio_se_var, extra_vars)

tmp_metareg <- as.data.frame(logit_metareg) %>%
.[, metareg_vars] %>%
setnames(metareg_vars, c("ratio_logit", "ratio_se_logit", extra_vars))


tmp_metareg$intercept <- 1

source("/FILEPATH/mr_brt_functions.R")

outdir <- "FILEPATH"
bundle <- 203
mod_lab <- paste0(bundle,  "_", 'network_logit')

fit_1 <- run_mr_brt(
output_dir = outdir, # user home directory
model_label = mod_lab,
data = tmp_metareg,
mean_var = "ratio_logit",
se_var = "ratio_se_logit",
covs =  list(
cov_info("cv_clinical", "X"),
cov_info("cv_self_report", "X"),
cov_info("cv_symptomatic", "X")),
trim_pct = 0.1,
method = "trim_maxL",
remove_x_intercept = TRUE,
overwrite_previous = TRUE
)

plot <- plot_mr_brt(fit_1)

########################################################

## Apply ratios

check_for_outputs(fit_1)
results <- load_mr_brt_outputs(fit_1)
names(results)
coefs <- results$model_coefs
metadata <- results$input_metadata

#######
# Create data sets with standarized variables names

###Apply ratios
# load all original bundle data
# -- read in data frame
#dat_original <- fread(paste0("/ihme/mnch/crosswalks/gyne/network_prep/00_clinical_crosswalked/step4/", bun_id, "_for_modeling_s4.csv"))
dat_original <- read_xlsx("/FILEPATH/203_prevalence_for_modeling.xlsx")

dat_original <- as.data.table(dat_original)
glimpse(dat_original)



# --  identify reference studies
dat_original <- dat_original %>%
mutate(cv_case_def_acog = 0) %>%
mutate(cv_self_report = 0) %>%
mutate(cv_dx_symptomatic_only  = 0) %>%
mutate(cv_case_def_acog = 0) %>%
mutate(cv_clinical =  1) %>%
mutate(dx =  "NA")


dat_original[, reference := 0]
dat_original[cv_case_def_acog  == 1, reference := 1]
dat_original[, age := round((age_start + age_end)/2)]
dat_original[, dx := "NA"]
dat_original[cv_self_report == 1, dx := "self_report"]
dat_original[cv_dx_symptomatic_only == 1,  dx := "symptomatic"]
dat_original[cv_clinical == 1,  dx := "clinical"]

# -- identify mean and standard error variables and covariates
reference_var <- "reference"
reference_value <- 1
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- c("cv_clinical", "cv_self_report", "cv_dx_symptomatic_only")

# create datasets with standardized variable names
orig_vars <- c(mean_var, se_var, reference_var, cov_names)
# metareg_vars2 <- c(ratio_var, ratio_se_var, extra_vars) # defined before running the model

tmp_orig <- as.data.frame(dat_original) %>%
.[, orig_vars] %>%
setnames(orig_vars, c("mean", "se", "ref", cov_names)) %>%
mutate(ref = if_else(ref == reference_value, 1, 0, 0))

#transform original data <0
tmp_orig$mean[(tmp_orig$mean == 0 & tmp_orig$ref == 0)] <- 0.025

tmp_orig$mean_logit <- logit(tmp_orig$mean)
tmp_orig$se_logit <- sapply(1:nrow(tmp_orig), function(i) {
    mean_i <- tmp_orig[i, "mean"]
    se_i <- tmp_orig[i, "se"]
    deltamethod(~log(x1), mean_i, se_i^2)
})

df_pred <- as.data.frame(tmp_orig[, cov_names]) # cov_names from line 55
names(df_pred) <- extra_vars
pred1 <- predict_mr_brt(fit_1, newdata = df_pred)
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries

tmp_preds <- preds %>%
mutate(
beta0 = Y_mean,
# don't need to incorporate tau as with metafor; MR-BRT already included it in the uncertainty
beta0_se_tau = (Y_mean_hi - Y_mean_lo) / 3.92) %>%
select(beta0, beta0_se_tau)


tmp_orig <- cbind(tmp_orig, tmp_preds)
tmp_orig <- select(tmp_orig, -cv_clinical, -cv_self_report, -cv_dx_symptomatic_only)

tmp_orig2 <- tmp_orig %>%
mutate(
mean_logit_tmp = mean_logit - beta0, # adjust the mean estimate: log(mean_original) - (log(alt) - log(ref))
var_logit_tmp = se_logit^2 + beta0_se_tau^2, # adjust the variance
se_logit_tmp = sqrt(var_logit_tmp)
)


# if original data point was a reference data point, leave as-is
# if original data point was a reference data point, leave as-is
tmp_orig3 <- tmp_orig2 %>%
mutate(
mean_logit_adjusted = if_else(ref == 1, mean_logit, mean_logit_tmp),
se_logit_adjusted = if_else(ref == 1, se_logit, se_logit_tmp),
lo_logit_adjusted = mean_logit_adjusted - 1.96 * se_logit_adjusted,
hi_logit_adjusted = mean_logit_adjusted + 1.96 * se_logit_adjusted,
mean_adjusted = invlogit(mean_logit_adjusted),
lo_adjusted = invlogit(lo_logit_adjusted),
hi_adjusted = invlogit(hi_logit_adjusted) )


#estimate the adjusted SE using the delta method
tmp_orig3$se_adjusted <- sapply(1:nrow(tmp_orig3), function(i) {
    ratio_i <- tmp_orig3[i, "mean_logit_adjusted"]
    ratio_se_i <- tmp_orig3[i, "se_logit_adjusted"]
    deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})



# Check for NaN SE
check <- select(tmp_orig3, ref,  mean, mean_adjusted, se, se_adjusted, lo_adjusted, hi_adjusted, mean_logit, se_logit, mean_logit_adjusted, se_logit_adjusted)
head(check)
tail(check)


filter(check,is.na(se_adjusted))

# 'final_data' is the original extracted data plus the new variables
final_data <- cbind(
dat_original,
tmp_orig3[, c("ref", "mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
)



#Reed's code leaves 47 rows of reference data in my bundle with NaNs in the se_adjusted
#column, instead of just copying over the standard_error. Something going awry with logging
#these small numbers back and forth. Adding thsi band-aid fix for now.

final_data[is.na(se_adjusted) & ref == 1, se_adjusted := standard_error]
final_data[ref == 1, mean_adjusted:= mean]
final_data[ref == 1, se_adjusted:= standard_error]
final_data[ref == 1, hi_adjusted := upper]
final_data[ref == 1, lo_adjusted := lower]
final_data[ref == 1, hi_adjusted := upper]


check <- select(final_data, ref,  mean, mean_adjusted, standard_error, se_adjusted, lo_adjusted, hi_adjusted, cv_self_report, cv_case_def_acog, cv_clinical)

write.csv(final_data,
file = paste0(save_dir, "network_prep/04_adjusted_data/step4/", bun_id, "_", measure, "_s4_rerun.csv"),
row.names = FALSE)

## Delete duplicative mean and se rows for modeling
final_data$is_outlier[(final_data$mean == 0 & final_data$cv_clinical == 1)] <- 1
final_data$is_outlier[(final_data$mean <= 0.005 & final_data$cv_clinical == 1)] <- 1


for_modeling <- final_data %>%
mutate(mean = mean_adjusted) %>%
mutate(lower = lo_adjusted) %>%
mutate(upper = hi_adjusted) %>%
mutate(standard_error = se_adjusted) %>%
mutate(step2_location_year = if_else(ref == 0, "crosswalked clinical/sympt/selfreport data, logit MR-BRT", "original data point"))

# Add/Change/Delete columns needed to upload
#for_modeling[ref == 1, crosswalk_parent_seq := NaN]
#for_modeling$crosswalk_parent_seq[(for_modeling$ref == 1)] <- NaN

#Mark outliers
for_modeling$is_outlier[(for_modeling$year_start == 2000 & for_modeling$clinical_data_type == "claims")] <- 1
for_modeling$is_outlier[(for_modeling$mean == 0 & for_modeling$cv_inpatient == 1)] <- 1
for_modeling$is_outlier[(for_modeling$mean <= 0.005 & for_modeling$cv_inpatient == 1)] <- 1


#for_modeling[is_outlier == 1, lower := NA]
#for_modeling[is_outlier == 1, upper := NA]
#for_modeling[is_outlier == 1, uncertainty_type := NA]
#for_modeling[is_outlier == 1, uncertainty_type_value := NA]



write.csv(for_modeling,
file = paste0(save_dir, "network_prep/04_adjusted_data/step4/", bun_id, "_for_modeling_rerun.csv"),
row.names = FALSE)

################

for_modeling <- for_modeling %>%
select(-(reference:hi_adjusted))

#incidence <- read.xlsx(paste0(save_dir, "agesex_split_data/all_data/199_incidence.xlsx"))
#to_upload <- rbind(for_modeling, incidence)


to_upload <- for_modeling

to_upload <- to_upload %>%
select(-group_review)

to_upload <- to_upload %>%
select(-cv_questionnaire)

write.xlsx(to_upload,
file = paste0(save_dir, "network_prep/05_to_upload/step4/", bun_id, "_step4_rerun.xlsx"),
row.names = FALSE, sheetName = "extraction")






