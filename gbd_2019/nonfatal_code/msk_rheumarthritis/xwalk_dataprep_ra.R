####################################################
## Description: Crosswalk Data Processing - RA
####################################################

rm(list=ls())

# Setting directories

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
source(paste0("FILEPATH", "/get_location_metadata.R"))
source(paste0("FILEPATH", "get_bundle_data.R"))
source(paste0("FILEPATH", "get_population.R"))
source(paste0("FILEPATH", "get_bundle_version.R"))

date <- gsub("-", "_", Sys.Date())
draws <- paste0("draw_", 0:999)

# SETTING OBJECTS
loc_data <- as.data.table(get_location_metadata(location_set_id = 35, gbd_round_id = 6))
loc_data <- loc_data[, c("location_id", "location_name", "ihme_loc_id", "region_name", "super_region_name")]

######################
ra <- as.data.table(get_bundle_version(8222, export = F, transform = T))
ra <- ra[clinical_data_type != "inpatient"]
ra[field_citation_value %like% "Truven Health" & year_start == 2000, cv_marketscan_all_2000 := 1]
ra[field_citation_value %like% "Truven Health" & year_start != 2000, cv_marketscan := 1]
ra[field_citation_value %like% "Taiwan National Health Insurance", cv_taiwan_claims_data := 1]
ra[, "crosswalk_parent_seq" := as.numeric(NA)]

ra <- ra[is_outlier != 1 | is.na(is_outlier)]
ra <- ra[is.na(drop) | drop != 1]

ra[, case_def := ifelse(!is.na(cv_cd_acr) & cv_cd_acr == 1, "reference", NA)]
ra[, case_def := ifelse(!is.na(cv_cd_non_acr)  & cv_cd_non_acr == 1, "Not ACR 1987", case_def)]
ra[, case_def := ifelse(!is.na(cv_cd_admin) & cv_cd_admin == 1, "reference", case_def)]
ra[, case_def := ifelse(!is.na(cv_marketscan_all_2000) & cv_marketscan_all_2000 == 1, "marketscan2000", case_def)]
ra[, case_def := ifelse(!is.na(cv_marketscan) & cv_marketscan == 1, "reference", case_def)]
ra[, case_def := ifelse(!is.na(cv_taiwan_claims_data) & cv_taiwan_claims_data == 1, "taiwan", case_def)]

# APPEND REGION, SUPER REGION

ra <- merge(ra, loc_data, by = "location_id")
ra$country <- substr(ra$ihme_loc_id, 0, 3)
ra[, location_name.x := NULL]
setnames(ra, "location_name.y", "location_name")

## FILL OUT MEAN/CASES/SAMPLE SIZE
## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS

ra$cases <- as.numeric(ra$cases)
ra$sample_size <- as.numeric(ra$sample_size)
ra <- get_cases_sample_size(ra)
ra <- get_se(ra)

orig <- as.data.table(copy(ra))

# SEX SPLIT

test <- find_sex_match(ra)
test2 <- calc_sex_ratios(test)

model_name <- paste0("ra_sexsplit_", date)

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

test <- age_sex_split(ra)

# APPLY SEX SPLIT

predict_sex <- split_data(ra, sex_model)

pdf("FILEPATH")
graph_predictions(predict_sex$graph)
dev.off()

write.xlsx(predict_sex$final, "FILEPATH")
test <- copy(predict_sex$final)

# DROP EXTRANEOUS COLUMNS
ra <- as.data.table(copy(test))
orig <- as.data.table(copy(ra))

claims_data <- ra[clinical_data_type == "claims"]
ra <- ra[clinical_data_type != "claims"]
claims_data <- aggregate_marketscan(claims_data)
claims_data <- claims_data[location_name == "United States" | location_name == "Taiwan"]
ra <- rbind(ra, claims_data)
ra <- get_cases_sample_size(ra)
ra <- get_se(ra)

ra <- ra[, c("nid", "sex", "year_start", "year_end", "age_start", "age_end", "location_id", "location_name", "ihme_loc_id", "country", "region_name", "super_region_name", "measure", "mean", "lower", "upper", "standard_error", "case_def")]

# CREATE CD MATCHES BY DEMOGRAPHICS
ra[, demographics := paste2(sex, country, measure, sep = "_")]

ra2 <- copy(ra)

ra2 <- ra2[!is.na(case_def)]
ra2 <- ra2[!is.na(standard_error)]

ra_cds <- list(unique(ra2$case_def))

ra2[, year_mean := (year_start + year_end)/2]
ra2[, age_mean := (age_start + age_end)/2]

ra2$case_def <- as.factor(ra2$case_def)
levels(ra2$case_def)
ra2$case_def <- factor(ra2$case_def, levels(ra2$case_def)[c(3, 1:2)])
ra2 <- ra2[order(case_def)]

list2 <- lapply(unique(ra2$case_def), function(i) {
  subset(ra2, case_def == i)
})

system.time({
  for (i in 1:(length(list2)-1)) {
    for (j in (i+1):length(list2)) {
      name <- paste0("paired_", gsub(" ", "_", unique(list2[[i]]$case_def)), "_", gsub(" ", "_", unique(list2[[j]]$case_def)))
      assign(name, as.data.table(merge(list2[[i]], list2[[j]], by = c("demographics"), all.x = F, suffixes = c(".denom", ".num"), allow.cartesian = T)))
    }
  }
})

pairss <- grep("paired", names(.GlobalEnv), value = T)
rm_pairss <- c() # clear out matched cds with 0 observations
for (i in pairss) {
  if (nrow(get(i)) < 1) {
    rm_pairss <- c(rm_pairss, paste0(i))
  }
}
rm(list = rm_pairss, envir = .GlobalEnv)
pairss <- grep("paired", names(.GlobalEnv), value = T)

ra_matched <- copy(paired_reference_marketscan2000)
ra_matched <- ra_matched[0,]

for (i in 1:length(pairss)) {
  ra_matched <- rbind(ra_matched, get(pairss[i]))
}

nrow(ra_matched[nid.denom == nid.num])
nrow(ra_matched[nid.denom != nid.num])

ra_matched <- ra_matched[abs(year_mean.denom - year_mean.num) < 6]
ra_matched <- ra_matched[abs(age_start.denom - age_start.num) < 6 & abs(age_end.denom - age_end.num) <6]

nrow(ra_matched[nid.denom == nid.num])
nrow(ra_matched[nid.denom != nid.num])

unique(ra_matched$case_def.num)
unique(ra_matched$case_def.denom)
remaining_cd <- unique(c(unique(as.character(ra_matched$case_def.num)), unique(as.character(ra_matched$case_def.denom))))

for (i in pairss) {
  dt <- as.data.table(get(paste0(i)))
  dt <- dt[abs(year_mean.denom - year_mean.num) < 6]
  dt <- dt[abs(age_start.denom - age_start.num) < 6 & abs(age_end.denom - age_end.num) <6]
  assign(paste0("subset_", paste0(i)), dt)
}
subset_matches <- grep("subset", names(.GlobalEnv), value = T)

rm_subset <- c() # clear out matched cds with 0 observations
for (i in subset_matches) {
  if (nrow(get(i)) < 1) {
    rm_subset <- c(rm_subset, paste0(i))
  }
}
rm(list = rm_subset, envir = .GlobalEnv)
subset_matches <- grep("subset", names(.GlobalEnv), value = T)

# CALCULATE RATIO AND STANDARD ERROR
ra_matched <- ra_matched %>%
  mutate(
    ratio = mean.num / mean.denom,
    ref_se = standard_error.denom, #(upper.denom - lower.denom) / 3.92,
    alt_se = standard_error.num, #(upper.num - lower.num) / 3.92,
    ratio_se =
      sqrt((mean.num^2 / mean.denom^2) * (alt_se^2/mean.num^2 + ref_se^2/mean.denom^2))
  )

ra_matched <- as.data.table(ra_matched)

ra_matched2 <- copy(ra_matched)
ra_matched2[, id := paste0(nid.num, " (", country.num, ": ", sex.num, " ", age_start.num, "-", age_end.num, ") - ", nid.denom, " (", country.denom, ": ", sex.denom, " ", age_start.denom, "-", age_end.denom, ")")]
ra_matched2[, id_var := paste0(nid.num, ":", nid.denom)]

# WRITING FILES

write.xlsx(ra_matched2, "FILEPATH")

# SET UP DUMMY VARIABLES

df <- as.data.table(read_xlsx("FILEPATH"))

df$case_def.denom <- gsub(pattern = " ", replacement = "_", x = df$case_def.denom)
df$case_def.num <- gsub(pattern = " ", replacement = "_", x = df$case_def.num)

df2 <- select(df, id, id_var, ratio, ratio_se, ref = case_def.denom, alt = case_def.num) %>%
  filter(!is.na(ratio) & !is.na(ratio_se))
df2$ref <- as.character(df2$ref)
df2$alt <- as.character(df2$alt)

alts <- unique(df2$alt)
refs <- unique(df2$ref)
reference_def <- "reference"
refs <- refs[refs != reference_def]
case_defs <- unique(c(refs, alts))

df2 <- as.data.table(df2)
df2[, paste0(case_defs) := 0]
for (each in alts) {
  df2[alt==each, paste0(each) := 1]
}
for (each in refs) {
  df2[ref == each, paste0(each) := -1]
}

df2 <- as.data.frame(df2)
check_newvars <- lapply(case_defs, function(x) table(df2[, x]) )
names(check_newvars) <- case_defs
check_newvars

### MORE MR-BeRT PREP

# original extracted data
# -- read in data frame
# -- specify how to identify reference studies
# -- identify mean, standard error and covariate variables

dat_original <- copy(orig)

dat_original[, paste0(c(case_defs, reference_def)) := 0]
dat_original[, reference := ifelse(case_def == "reference", 1, 0)]
dat_original[, Rome_1961 := ifelse(case_def == "Rome 1961", 1, 0)]
dat_original[, marketscan2000 := ifelse(case_def == "marketscan2000", 1, 0)]
dat_original[, taiwan := ifelse(case_def == "taiwan", 1, 0)]

reference_var <- "reference"
reference_value <- 1
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- case_defs # can be a vector of names

# data for meta-regression model
# -- read in data frame
# -- identify variables for ratio and SE
#    NOTE: ratio must be specified as reference/alternative
dat_metareg <- df2
ratio_var <- "ratio"
ratio_se_var <- "ratio_se"

# create datasets with standardized variable names
orig_vars <- c(mean_var, se_var, reference_var, cov_names)

metareg_vars <- c(ratio_var, ratio_se_var)
extra_vars <- case_defs
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

#####
cov_list <- lapply(case_defs, function(x) cov_info(x, "X"))
model_name <- "crosswalk_network_ra"

tmp_metareg <- tmp_metareg[is.finite(tmp_metareg$ratio) & is.finite(tmp_metareg$ratio_log) & is.finite(tmp_metareg$ratio_se) & is.finite(tmp_metareg$ratio_se_log), ]

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

check_for_outputs(fit1)

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
    # don't need to incorporate tau as with metafor; MR-BRT already included it in the uncertainty
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

# 'final_data' is the original extracted data plus the new variables
final_data <- cbind(
  dat_original,
  tmp_orig3[, c("ref", "mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
)

final_data <- as.data.table(final_data)
setnames(final_data, c("mean", "upper", "lower", "standard_error"), c("mean_old", "upper_old", "lower_old", "se_old"))
setnames(final_data, c("mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted"), c("mean", "standard_error", "lower", "upper"))
write.xlsx(final_data, "FILEPATH")

########## AGE SPLITTING
source("FILEPATH")
model_name <- "crosswalk_network_ra"

final_test <- as.data.table(read.xlsx("FILEPATH"))
#########
dt <- copy(final_test)
ages <- get_age_metadata(12)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_groups <- ages[, age_group_id]
id <- 2137

df <- copy(dt)
age <- age_groups
gbd_id <- id

final_split <- age_split(gbd_id = id, df = dt, age = age_groups, region_pattern = F, location_pattern_id = 1)
write.xlsx(final_split, "FILEPATH")

# SAVE CROSSWALK VERSION
source(paste0("FILEPATH", "save_crosswalk_version.R"))
acause <- "msk_rheumarthritis"
bundle_id <- 214
step2_version <- 8222

to_save_xwalk <- read.xlsx("FILEPATH")
to_save_xwalk <- as.data.table(to_save_xwalk)
to_save_xwalk <- to_save_xwalk[sex != "Both"]
to_save_xwalk[measure == "prevalence" & upper > 1, `:=` (lower = NaN, upper = NaN, uncertainty_type_value = NA)]
to_save_xwalk <- to_save_xwalk[group_review == 1 | is.na(group_review)]
to_save_xwalk[, `:=` (group = NA, specificity = NA, group_review = NA)]
to_save_xwalk[is.na(upper), `:=` (lower = NA, uncertainty_type_value = NA)]
to_save_xwalk[!is.na(upper), uncertainty_type_value := 95]
to_save_xwalk[upper > 1, `:=` (upper = NaN, lower = NaN, uncertainty_type_value = NA)]

write.xlsx(to_save_xwalk, "FILEPATH", sheetName = "extraction")
save_crosswalk_version(step2_version, "FILEPATH", description = "xwalked, age split")

