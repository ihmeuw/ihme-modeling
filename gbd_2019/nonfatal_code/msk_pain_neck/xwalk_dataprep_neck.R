####################################################
## Description: Crosswalk Data Processing - NECK
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

np <- as.data.table(get_bundle_version(9014, export = F, transform = T))
np <- np[clinical_data_type != "inpatient"]
np[field_citation_value %like% "Truven Health" & year_start == 2000, cv_marketscan_all_2000 := 1]
np[field_citation_value %like% "Truven Health" & year_start != 2000, cv_marketscan := 1]
np[field_citation_value %like% "Taiwan National Health Insurance", cv_taiwan_claims_data := 1]
np[, "crosswalk_parent_seq" := as.numeric(NA)]

np <- np[is_outlier != 1 | is.na(is_outlier)]
np <- np[is.na(drop) | drop != 1]

np[, GBD := ifelse(((!is.na(case_definition) & (grepl("current", case_definition, ignore.case = T) | grepl("present", case_definition, ignore.case = T))) | (!is.na(specificity) & grepl("point", specificity, ignore.case = T))) & is.na(cv_broad) & is.na(cv_pop_school) & is.na(cv_recall_ever) & is.na(cv_recall_week) & is.na(cv_recall_month), "GBD_", NA)]
np[, chronic := ifelse(!is.na(case_definition) & grepl("chronic", case_definition, ignore.case = T), "chronic_", NA)]
np[, students := ifelse(!is.na(cv_pop_school) & cv_pop_school == 1, "students_", NA)]
np[, recall1mless := ifelse(!is.na(cv_recall_week) & cv_recall_week == 1, "recall1mless_", NA)]
np[, recall1yrless := ifelse(!is.na(cv_recall_month) & cv_recall_month == 1, "recall1yrless_", NA)]
np[, recallLifetime := ifelse(!is.na(cv_recall_ever) & cv_recall_ever == 1, "recallLife_", NA)]
np[, activityLimit := ifelse((!is.na(cv_beh_activity) & cv_beh_activity ==1) | (!is.na(case_definition) & grepl("activity*limit*", case_definition, ignore.case = T)), "activityLimit_", NA)]
np[, anatBroad := ifelse(!is.na(cv_broad) & cv_broad == 1, "anatBroad_", NA)]
np[, taiwanClaims := ifelse(!is.na(cv_taiwan_claims_data) & cv_taiwan_claims_data == 1, "taiwanClaims", NA)]
np[, marketscan2000 := ifelse(!is.na(cv_marketscan_all_2000) & cv_marketscan_all_2000 == 1, "marketscan2000", NA)]
np[, marketscan2010 := ifelse(!is.na(cv_marketscan) & cv_marketscan == 1, "marketscan2010", NA)]

np <- np[is.na(recallLifetime)]

np[, case_def := paste2(GBD, chronic, students, recall1mless, recall1yrless, activityLimit, anatBroad, taiwanClaims, marketscan2000, marketscan2010, sep = "")]

# APPEND REGION, SUPER REGION, ETC

np <- merge(np, loc_data, by = "location_id")
np$country <- substr(np$ihme_loc_id, 0, 3)
np[, location_name.x := NULL]
setnames(np, "location_name.y", "location_name")

## FILL OUT MEAN/CASES/SAMPLE SIZE
## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS

np$cases <- as.numeric(np$cases)
np$sample_size <- as.numeric(np$sample_size)
np <- get_cases_sample_size(np)
np <- get_se(np)

orig <- as.data.table(copy(np))

# SEX SPLIT
test <- find_sex_match(np)
test2 <- calc_sex_ratios(test)

model_name <- paste0("np_sexsplit_", date)

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
test <- age_sex_split(np)

# APPLY SEX SPLIT
predict_sex <- split_data(test, sex_model)

pdf("FILEPATH")
graph_predictions(predict_sex$graph)
dev.off()

write.xlsx(predict_sex$final, "FILEPATH")
test <- copy(predict_sex$final)
test <- read.xlsx("FILEPATH")

# DROP EXTRANEOUS COLUMNS
np <- as.data.table(copy(test))
orig <- as.data.table(copy(np))

claims_data <- np[clinical_data_type == "claims"]
np <- np[clinical_data_type != "claims"]
claims_data <- aggregate_marketscan(claims_data)
claims_data <- claims_data[location_name == "United States" | location_name == "Taiwan"]
np <- rbind(np, claims_data)
np <- get_cases_sample_size(np)
np <- get_se(np)

np <- np[, c("nid", "sex", "year_start", "year_end", "age_start", "age_end", "location_id", "location_name", "ihme_loc_id", "country", "region_name", "super_region_name", "measure", "mean", "lower", "upper", "standard_error", "case_def")]

# CREATE CD MATCHES BY DEMOGRAPHICS
np[, demographics := paste2(sex, country, measure, sep = "_")]

np <- np[case_def != ""]

np_cds <- list(unique(np$case_def))

np[, year_mean := (year_start + year_end)/2]
np[, age_mean := (age_start + age_end)/2]

np$case_def <- as.factor(np$case_def)
np$case_def <- factor(np$case_def, levels(np$case_def)[c(6, 1:5, 7:14)])
np <- np[order(case_def)]

list2 <- lapply(unique(np$case_def), function(i) {
  subset(np, case_def == i)
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

np_matched <- copy(paired_anatBroad__chronic_recall1yrless_)
np_matched <- np_matched[0,]

for (i in 1:length(pairss)) {
  np_matched <- rbind(np_matched, get(pairss[i]))
}

nrow(np_matched[nid.denom == nid.num])
nrow(np_matched[nid.denom != nid.num])

np_matched <- np_matched[abs(year_mean.denom - year_mean.num) < 6]
np_matched <- np_matched[abs(age_mean.denom - age_mean.num) < 6]

nrow(np_matched[nid.denom == nid.num])
nrow(np_matched[nid.denom != nid.num])

unique(np_matched$case_def.num)
unique(np_matched$case_def.denom)
remaining_cd <- unique(c(unique(as.character(np_matched$case_def.num)), unique(as.character(np_matched$case_def.denom))))

for (i in pairss) {
  dt <- as.data.table(get(paste0(i)))
  dt <- dt[abs(year_mean.denom - year_mean.num) < 6]
  dt <- dt[abs(age_mean.denom - age_mean.num) < 6]
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

np_matched <- np_matched %>%
  mutate(
    ratio = mean.num / mean.denom,
    ref_se = standard_error.denom, #(upper.denom - lower.denom) / 3.92,
    alt_se = standard_error.num, #(upper.num - lower.num) / 3.92,
    ratio_se =
      sqrt((mean.num^2 / mean.denom^2) * (alt_se^2/mean.num^2 + ref_se^2/mean.denom^2))
  )

np_matched <- as.data.table(np_matched)
np_matched[, id := paste0(nid.num, " (", country.num, ": ", sex.num, " ", age_start.num, "-", age_end.num, ") - ", nid.denom, " (", country.denom, ": ", sex.denom, " ", age_start.denom, "-", age_end.denom, ")")]
np_matched[, id_var := paste0(nid.num, ":", nid.denom)]

# WRITING FILES
write.xlsx(np_matched, "FILEPATH")

# CREATE DUMMY VARIABLES

df <- copy(np_matched)
df <- read.xlsx("FILEPATH")

df2 <- select(df, id, id_var, ratio, ratio_se, ref = case_def.denom, alt = case_def.num) %>%
  filter(!is.na(ratio) & !is.na(ratio_se))

df2$ref <- as.character(df2$ref)
df2$alt <- as.character(df2$alt)
refs <- unique(unlist(lapply(df2$ref, function(x) strsplit(x, split = "_")[[1]])))
alts <- unique(unlist(lapply(df2$alt, function(x) strsplit(x, split = "_")[[1]])))

unique(c(df2$ref, df2$alt))
case_defs <- unique(c(refs, alts))
nonGBD_case_defs <- case_defs[!case_defs == "GBD"]

for (i in nonGBD_case_defs) df2[, i] <- 0
for (i in nonGBD_case_defs) df2[, i] <- df2[, i] - sapply(i, grepl, df2$ref)
for (i in nonGBD_case_defs) df2[, i] <- df2[, i] + sapply(i, grepl, df2$alt)

check_newvars <- lapply(nonGBD_case_defs, function(x) table(df2[, x]) )
names(check_newvars) <- nonGBD_case_defs
check_newvars

write.xlsx(df2, "FILEPATH")

# MR BRT
dat_original <- copy(orig)
reference_var <- "GBD"

dat_original <- as.data.table(dat_original)
dat_original[, GBD := ifelse(!is.na(GBD), 1, 0)]
dat_original[, chronic := ifelse(!is.na(chronic), 1, 0)]
dat_original[, students := ifelse(!is.na(students), 1, 0)]
dat_original[, recall1mless := ifelse(!is.na(recall1mless), 1, 0)]
dat_original[, recall1yrless := ifelse(!is.na(recall1yrless), 1, 0)]
dat_original[, activityLimit := ifelse(!is.na(activityLimit), 1, 0)]
dat_original[, anatBroad := ifelse(!is.na(anatBroad), 1, 0)]
dat_original[, taiwanClaims := ifelse(!is.na(taiwanClaims), 1, 0)]
dat_original[, marketscan2000 := ifelse(!is.na(marketscan2000), 1, 0)]
dat_original[, marketscan2010 := ifelse(!is.na(marketscan2010), 1, 0)]

reference_value <- 1
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- nonGBD_case_defs # can be a vector of names

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


cov_list <- lapply(nonGBD_case_defs, function(x) cov_info(x, "X"))
model_name <- "crosswalk_network_np"

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
source(paste0("FILEPATH", "age_split_fns.R"))

final_test <- read.xlsx("FILEPATH")

#########
dt <- copy(final_test)
ages <- get_age_metadata(12)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_groups <- ages[, age_group_id]
id <- 2154

df <- copy(dt)
age <- age_groups
gbd_id <- id

final_split <- age_split(gbd_id = id, df = dt, age = age_groups, region_pattern = F, location_pattern_id = 1)
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
acause <- "msk_pain_neck"
bundle_id <- 218
step2_version <- 9014

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

