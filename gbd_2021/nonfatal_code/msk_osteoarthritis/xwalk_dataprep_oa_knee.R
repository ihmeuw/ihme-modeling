####################################################
## Author: USER
## Date: 12/11/2018
## Description: Crosswalk Data Processing - Osteo KNEE
####################################################

rm(list=ls())

library(readxl)
library(data.table)
library(ggplot2)
library(dplyr)
library(tidyr)
library(Hmisc, lib.loc = "FILEPATH")
library(mvtnorm)
library(survival, lib.loc = "FILEPATH")
library(expm, lib.loc = "FILEPATH")
library(msm, lib.loc = "FILEPATH")
library(openxlsx)
library(metafor, lib.loc = "FILEPATH")
library(mortdb, lib = "FILEPATH")

source(paste0("FILEPATH", "cov_info_function.R"))
source(paste0("FILEPATH", "run_mr_brt_function.R"))
source(paste0("FILEPATH", "check_for_outputs_function.R"))
source(paste0("FILEPATH", "load_mr_brt_outputs_function.R"))
source(paste0("FILEPATH", "predict_mr_brt_function.R"))
source(paste0("FILEPATH", "check_for_preds_function.R"))
source(paste0("FILEPATH", "load_mr_brt_preds_function.R"))
source(paste0("FILEPATH", "plot_mr_brt_function.R"))

source(paste0("FILEPATH", "crosswalk_sex_split_fn.R"))
source(paste0("FILEPATH", "get_location_metadata.R"))
source(paste0("FILEPATH", "get_bundle_data.R"))
source(paste0("FILEPATH", "get_population.R"))
source(paste0("FILEPATH", "get_bundle_version.R"))

loc_data <- as.data.table(get_location_metadata(location_set_id = 35, gbd_round_id = 6))
loc_data <- loc_data[, c("location_id", "location_name", "ihme_loc_id", "region_name", "super_region_name")]

date <- gsub("-", "_", Sys.Date())
draws <- paste0("draw_", 0:999)

knee <- as.data.table(get_bundle_version(9167 , export = F, transform = T))
knee <- knee[clinical_data_type != "inpatient"]
knee <- knee[!(field_citation_value %like% "Truven Health" & clinical_data_type == ""),]
knee <- knee[!(field_citation_value %like% "Taiwan National Health Insurance" & clinical_data_type == ""),]
knee[field_citation_value %like% "Truven Health" & year_start == 2000, cv_marketscan_all_2000 := 1]
knee[field_citation_value %like% "Truven Health" & year_start != 2000, cv_marketscan := 1]
knee[field_citation_value %like% "Taiwan National Health Insurance", cv_taiwan_claims_data := 1]
knee[, "crosswalk_parent_seq" := as.numeric(NA)]

knee <- knee[is_outlier != 1 | is.na(is_outlier)]

knee[, case_def := ifelse(!is.na(cv_marketscan) & cv_marketscan == 1, "marketscan", NA)]
knee[, case_def := ifelse(!is.na(cv_marketscan_all_2000) & cv_marketscan_all_2000 == 1, "marketscan00", case_def)]
knee[, case_def := ifelse(!is.na(cv_taiwan_claims_data) & cv_taiwan_claims_data == 1, "taiwan_claims", case_def)]
knee[, case_def := ifelse(!is.na(cv_dx_selfreported_pain) & cv_dx_selfreported_pain == 1, "sympt_no_xray", case_def)]
knee[, case_def := ifelse((!is.na(cv_dx_radiographic_only) & cv_dx_radiographic_only == 1) | (!is.na(cv_dx_symptomatic_only) & cv_dx_symptomatic_only == 1), "xray_maybe_sympt", case_def)]
knee[, case_def := ifelse(!is.na(cv_dx_oa_selfreport) & cv_dx_oa_selfreport == 1, "selfrep_physdx", case_def)]
knee[, case_def := ifelse(is.na(case_def), "GBD", case_def)]

# APPEND REGION, SUPER REGION, ETC
knee <- merge(knee, loc_data, by = "location_id")
knee$country <- substr(knee$ihme_loc_id, 0, 3)
knee[, location_name.x := NULL]
setnames(knee, "location_name.y", "location_name")

# FILL OUT MEAN/CASES/SAMPLE SIZE
# CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
knee <- get_cases_sample_size(knee)
knee <- get_se(knee)

# SEX SPLIT
test <- find_sex_match(knee)
test2 <- calc_sex_ratios(test)

model_name <- paste0("oa_knee_sexsplit_", date)

sex_model <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = model_name,
  data = test2,
  mean_var = "log_ratio",
  se_var = "log_se",
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.1,
  overwrite_previous = T
)

plot_mr_brt(sex_model)

# AGE-SEX SPLIT WITHIN LIT
test <- age_sex_split(knee)

# APPLY SEX SPLIT
predict_sex <- split_data(test, sex_model)

pdf("FILEPATH")
graph_predictions(predict_sex$graph)
dev.off()

write.xlsx(predict_sex$final, "FILEPATH")
test <- copy(predict_sex$final)
test <- read.xlsx("FILEPATH")

# DROP EXTRANEOUS COLUMNS
knee <- as.data.table(copy(test))
original_w_cd <- as.data.table(copy(knee))

claims_data <- knee[clinical_data_type == "claims" & location_name != "Taiwan"]
knee <- knee[cv_marketscan != 1 | is.na(cv_marketscan)]
knee <- knee[cv_marketscan_all_2000 != 1 | is.na(cv_marketscan_all_2000)]
claims_data <- aggregate_marketscan(claims_data)
claims_data <- claims_data[location_name == "United States" | location_name == "Taiwan"]
knee <- rbind(knee, claims_data)
knee <- get_cases_sample_size(knee)
knee <- get_se(knee)

knee <- knee[, c("nid", "sex", "year_start", "year_end", "age_start", "age_end", "location_id", "location_name", "ihme_loc_id", "country", "region_name", "super_region_name", "measure", "mean", "lower", "upper", "standard_error", "case_def")]

# CREATE CD MATCHES BY DEMOGRAPHICS
knee[, demographics := paste2(sex, country, measure, sep = "_")]
knee[, demographics := paste2(sex, location_name, measure, sep = "_")]

knee_cds <- list(unique(knee$case_def))

knee[, year_mean := (year_start + year_end)/2]
knee[, age_mean := (age_start + age_end)/2]

knee$case_def <- as.factor(knee$case_def)
knee <- knee[order(case_def)]

list <- lapply(unique(knee$case_def), function(i) {
  subset(knee, case_def == i)
})

system.time({
  for (i in 1:(length(list)-1)) {
    for (j in (i+1):length(list)) {
      name <- paste0("paired_", gsub(" ", "_", unique(list[[i]]$case_def)), "_", gsub(" ", "_", unique(list[[j]]$case_def)))
      assign(name, as.data.table(merge(list[[i]], list[[j]], by = c("demographics"), all.x = F, suffixes = c(".denom", ".num"), allow.cartesian = T)))
    }
  }
})

pairss <- grep("paired", names(.GlobalEnv), value = T)

knee_matched <- copy(paired_GBD_marketscan)
knee_matched <- knee_matched[0,]

for (i in 1:length(pairss)) {
  knee_matched <- rbind(knee_matched, get(pairss[i]))
}

knee_matched <- knee_matched[abs(year_mean.denom - year_mean.num) < 11]
knee_matched <- knee_matched[abs(age_start.denom - age_start.num) < 6 & abs(age_end.denom - age_end.num) <6]

# CALCULATE RATIO AND STANDARD ERROR

knee_matched <- knee_matched %>%
  mutate(
    ratio = mean.num / mean.denom,
    ref_se = standard_error.denom, #(upper.denom - lower.denom) / 3.92,
    alt_se = standard_error.num, #(upper.num - lower.num) / 3.92,
    ratio_se =
      sqrt((mean.num^2 / mean.denom^2) * (alt_se^2/mean.num^2 + ref_se^2/mean.denom^2))
  )

knee_matched <- as.data.table(knee_matched)
knee_matched[, id := paste0(nid.num, " (", country.num, ": ", sex.num, " ", age_start.num, "-", age_end.num, ") - ", nid.denom, " (", country.denom, ": ", sex.denom, " ", age_start.denom, "-", age_end.denom, ")")]
knee_matched[, id_var := paste0(nid.num, ":", nid.denom)]

# WRITING FILES
write.xlsx(knee_matched, "FILEPATH")

# SET DUMMY VARIABLES
df <- as.data.table(read_xlsx("FILEPATH"))
df <- copy(knee_matched)

df2 <- select(df, id, id_var, ratio, ratio_se, ref = case_def.denom, alt = case_def.num) %>%
  filter(!is.na(ratio) & !is.na(ratio_se))

df2$ref <- as.character(df2$ref)
df2$alt <- as.character(df2$alt)
refs <- unique(unlist(df2$ref))
alts <- unique(unlist(df2$alt))

unique(c(df2$ref, df2$alt))
case_defs <- unique(c(refs, alts))
nonGBD_case_defs <- case_defs[!case_defs == "GBD"]

df2 <- as.data.table(df2)

for (i in nonGBD_case_defs) df2[, i] <- 0

for (i in nonGBD_case_defs) {
  df2[alt == i, paste0(i) := 1]
}

for (i in nonGBD_case_defs) {
  df2[ref == i, paste0(i) := -1]
}

df2 <- as.data.frame(df2)

check_newvars <- lapply(nonGBD_case_defs, function(x) table(df2[, x]) )
names(check_newvars) <- nonGBD_case_defs
check_newvars

write.xlsx(df2, "FILEPATH")

# MR BRT PREP

# original extracted data
# -- read in data frame
# -- specify how to identify reference studies
# -- identify mean, standard error and covariate variables

dat_original <- copy(original_w_cd)
dat_original[cv_taiwan_claims_data == 1, `:=` (group = 1, specificity = "no crosswalk possible", group_review = 0)]

dat_original[, one := 1]
dat_original <- dcast(dat_original, ... ~ case_def, value.var = "one")
for (i in unlist(knee_cds)) {
  colname <- paste0(i)
  dat_original[[colname]][is.na(dat_original[[colname]])] <- 0
}

dat_original[, paste0(c(case_defs)) := 0]
dat_original[, GBD := ifelse(case_def == "GBD", 1, 0)]
dat_original[, marketscan := ifelse(case_def == "marketscan", 1, 0)]
dat_original[, marketscan00 := ifelse(case_def == "marketscan00", 1, 0)]
dat_original[, sympt_no_xray := ifelse(case_def == "sympt_no_xray", 1, 0)]
dat_original[, selfrep_physdx := ifelse(case_def == "selfrep_physdx", 1, 0)]
dat_original[, xray_maybe_sympt := ifelse(case_def == "xray_maybe_sympt", 1, 0)]


reference_var <- "GBD"
reference_value <- 1
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- nonGBD_case_defs # can be a vector of names


# data for meta-regression model
# -- read in data frame
# -- identify variables for ratio and SE
#    NOTE: ratio must be specified as reference/alternative

dat_metareg <- read_xlsx("FILEPATH")
ratio_var <- "ratio"
ratio_se_var <- "ratio_se"

# create datasets with standardized variable names
orig_vars <- c(mean_var, se_var, reference_var, cov_names)

metareg_vars <- c(ratio_var, ratio_se_var)
extra_vars <- nonGBD_case_defs
id_vars <- c("id", "id_var", "ref", "alt")
metareg_vars2 <- c(id_vars, metareg_vars, extra_vars)

tmp_orig <- as.data.frame(dat_original) %>%
  .[, orig_vars] %>%
  setnames(orig_vars, c("mean", "se", "ref", cov_names)) %>%
  mutate(ref = if_else(ref == reference_value, 1, 0, 0))

tmp_metareg <- as.data.frame(dat_metareg) %>%
  .[, metareg_vars2] %>%
  setnames(metareg_vars, c("ratio", "ratio_se"))

# log transform the original data
# -- SEs transformed using the delta method
tmp_orig$mean_log <- log(tmp_orig$mean)
tmp_orig$se_log <- sapply(1:nrow(tmp_orig), function(i) {
  mean_i <- tmp_orig[i, "mean"]
  se_i <- tmp_orig[i, "se"]
  deltamethod(~log(x1), mean_i, se_i^2)
})

# log transform the meta-regression data
tmp_metareg$ratio_log <- log(tmp_metareg$ratio)
tmp_metareg$ratio_se_log <- sapply(1:nrow(tmp_metareg), function(i) {
  ratio_i <- tmp_metareg[i, "ratio"]
  ratio_se_i <- tmp_metareg[i, "ratio_se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})

tmp_metareg$intercept <- 1

# RUN MR BRT
cov_list <- lapply(nonGBD_case_defs, function(x) cov_info(x, "X"))

model_name <- "crosswalk_network_oa_knee"

fit1 <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = model_name,
  data = tmp_metareg,
  mean_var = "ratio_log",
  se_var = "ratio_se_log",
  covs = cov_list,
  remove_x_intercept = TRUE,
  method = "trim_maxL",
  trim_pct = 0.1,
  overwrite_previous = TRUE,
  study_id = "id_var"
)

############
############

coefs <- as.data.table(fit1$model_coefs)
coefs[, beta_exp := exp(beta_soln)]
coefs[, se_soln := sqrt(beta_var)]
coefs[, beta_soln_lower := beta_soln - 1.96 * se_soln]
coefs[, beta_soln_upper := beta_soln + 1.96 * se_soln]
coefs[, beta_exp_lower := exp(beta_soln_lower)]
coefs[, beta_exp_upper := exp(beta_soln_upper)]

############
############
# this creates a ratio prediction for each observation in the original data
df_pred <- as.data.frame(tmp_orig[, cov_names]) 
names(df_pred) <- cov_names
pred1 <- predict_mr_brt(fit1, newdata = df_pred, write_draws = T)
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries

tmp_preds <- preds %>%
  dplyr::mutate(
    pred = Y_mean,
    pred_se = (Y_mean_hi - Y_mean_lo) / 3.92  ) %>%
  select(pred, pred_se)

# new variance of the adjusted data point is just the sum of variances
# because the adjustment is a sum rather than a product in log space
tmp_orig2 <- cbind(tmp_orig, tmp_preds) %>%
  dplyr::mutate(
    mean_log_tmp = mean_log - pred, # adjust the mean estimate: log(mean_original) - (log(alt) - log(ref))
    var_log_tmp = se_log^2 + pred_se^2, # adjust the variance
    se_log_tmp = sqrt(var_log_tmp)
  )

# if original data point was a reference data point, leave as-is
tmp_orig3 <- tmp_orig2 %>%
  dplyr::mutate(
    mean_log_adjusted = if_else(ref == 1, mean_log, mean_log_tmp),
    se_log_adjusted = if_else(ref == 1, se_log, se_log_tmp),
    lo_log_adjusted = mean_log_adjusted - 1.96 * se_log_adjusted,
    hi_log_adjusted = mean_log_adjusted + 1.96 * se_log_adjusted,
    mean_adjusted = exp(mean_log_adjusted),
    lo_adjusted = exp(lo_log_adjusted),
    hi_adjusted = exp(hi_log_adjusted) )

tmp_orig3$se_adjusted <- sapply(1:nrow(tmp_orig3), function(i) {
  ratio_i <- tmp_orig3[i, "mean_log_adjusted"]
  ratio_se_i <- tmp_orig3[i, "se_log_adjusted"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})
tmp_orig3 <- as.data.table(tmp_orig3)
tmp_orig3[mean == 0, se_adjusted := sqrt(se^2 + pred_se^2)]

final_data <- cbind(
  dat_original,
  tmp_orig3[, c("ref", "mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
)

final_data <- as.data.table(final_data)
setnames(final_data, c("mean", "upper", "lower", "standard_error"), c("mean_old", "upper_old", "lower_old", "se_old"))
setnames(final_data, c("mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted"), c("mean", "standard_error", "lower", "upper"))
write.xlsx(final_data, "FILEPATH")

########## AGE SPLITTING
source(paste0("FILEPATH", "age_split_fns.R"))

final_test <- read.xlsx("FILEPATH")
final_test <- copy(final_data)
#########
dt <- copy(final_test)
ages <- get_age_metadata(12)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_groups <- ages[, age_group_id]
id <- 2145

df <- copy(dt)
# df[, 87] <- NULL
age <- age_groups
gbd_id <- id

final_split <- age_split(gbd_id = id, df = df, age = age_groups, region_pattern = F, location_pattern_id = 1)
write.xlsx(final_split, "FILEPATH")

x <- copy(final_split)
x[, age := (age_start + age_end) / 2]
gg <- ggplot(x, aes(x = age, y = mean)) +
  geom_smooth(se = F) +
  labs(x = "Age", y = "Prevalence", title = "Age Pattern") +
  theme_classic()

pdf("FILEPATH")
gg
dev.off()

# SAVE CROSSWALK VERSION
source(paste0("FILEPATH", "save_crosswalk_version.R"))
acause <- "msk_osteoarthritis"
bundle_id <- 216
step2_version <- 9167

to_save_xwalk <- read.xlsx("FILEPATH")
to_save_xwalk <- as.data.table(to_save_xwalk)
to_save_xwalk <- to_save_xwalk[sex != "Both"]
to_save_xwalk[measure == "prevalence" & upper > 1, `:=` (lower = NaN, upper = NaN, uncertainty_type_value = NA)]
to_save_xwalk <- to_save_xwalk[group_review == 1 | is.na(group_review)]
to_save_xwalk[, `:=` (group = NA, specificity = NA, group_review = NA)]
to_save_xwalk[is.na(upper), `:=` (lower = NA, uncertainty_type_value = NA)]
to_save_xwalk[!is.na(upper), uncertainty_type_value := 95]

write.xlsx(to_save_xwalk, "FILEPATH", sheetName = "extraction")
save_crosswalk_version(step2_version, "FILEPATH", description = "xwalked, age split, no taiwan claims data")












