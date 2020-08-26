####################################################
## Description: Crosswalk Data Processing - Osteo hand
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

# LOADING DATA
hand <- as.data.table(get_bundle_version(10307, export = F, transform = T))
hand[field_citation_value %like% "Truven Health" & year_start == 2000, cv_marketscan_all_2000 := 1]
hand[field_citation_value %like% "Truven Health" & year_start != 2000, cv_marketscan := 1]
hand[, "crosswalk_parent_seq" := as.numeric(NA)]
hand <- hand[!(field_citation_value %like% "Truven Health" & age_start >= 65)]

hand <- hand[is_outlier != 1 | is.na(is_outlier)]

hand[, case_def := ifelse(!is.na(cv_marketscan) & cv_marketscan == 1, "marketscan", NA)]
hand[, case_def := ifelse(!is.na(cv_marketscan_all_2000) & cv_marketscan_all_2000 == 1, "marketscan00", case_def)]

hand[, site := ifelse(grepl("IP", case_name, ignore.case = T), "IP", NA)]
hand[, site := ifelse(grepl("DIP", case_name, ignore.case = T), "DIP", site)]
hand[, site := ifelse(grepl("PIP", case_name, ignore.case = T), "PIP", site)]
hand[, site := ifelse(grepl("thumb base", case_name, ignore.case = T), "thumbBase", site)]
hand[, site := ifelse(grepl("CMC", case_name, ignore.case = T), "CMC", site)]
hand[, site := ifelse(grepl("MCM", case_name, ignore.case = T), "MCM", site)]
hand[, site := ifelse(grepl("MCP", case_name, ignore.case = T), "MCP", site)]
hand[, site := ifelse(grepl("MTP", case_name, ignore.case = T), "MTP", site)]
hand[, site := ifelse(grepl("RC", case_name, ignore.case = T), "RC", site)]
hand[, site := ifelse(grepl("OMCL", case_name, ignore.case = T), "OMCL", site)]
hand[, site := ifelse(grepl("wrist", case_name, ignore.case = T), "wrist", site)]
hand[, site := ifelse(grepl("carpus", case_name, ignore.case = T), "carpus", site)]
hand[, site := ifelse(grepl("[+]", case_name) | grepl("at least", case_name, ignore.case = T) | grepl("out of", case_name, ignore.case = T), "multipleJoints", site)]
hand[, site := ifelse(grepl("any joint", case_name, ignore.case = T) | grepl("whole hand", case_name, ignore.case = T) | grepl("hand pain", case_name, ignore.case = T) | grepl("any", case_name, ignore.case = T) | grepl("^hand$", case_name, ignore.case = T), "anyJoint", site)]
hand[, site := ifelse(grepl("generalized", case_name, ignore.case = T), "generalized", site)]
hand[, site := ifelse(site == "IP" | site == "DIP" | site == "PIP" | site == "MCP" | site == "MTP" | site == "RC" | site == "OMCL", "singleJoint", site)]
hand[, site := ifelse(site == "thumbBase" | site == "CMC", "thumbCMC", site)]
hand[, site := ifelse(site == "singleJoint" | site == "anyJoint" | site == "wrist", "anyJoint", site)]

hand[, test := ifelse(cv_self_report == 1, "selfReport", NA)]
hand[, test := ifelse(cv_symptomatic == 1 & cv_self_report == 0 & cv_phys_dx == 0 & cv_test_other == 0 & cv_kellgren == 0, "symptOnly", test)]
hand[, test := ifelse(cv_symptomatic == 1 & (cv_test_other == 1 | cv_kellgren == 1), "symptAndRadiography", test)]
hand[, test := ifelse(cv_symptomatic == 0 & (cv_test_other == 1 | cv_kellgren == 1), "RadiographyOnly", test)]
hand[, test := ifelse(cv_symptomatic == 1 & cv_phys_dx == 1, "symptAndPhysDx", test)]
hand[, test := ifelse(cv_symptomatic == 0 & cv_phys_dx == 1, "physDxOnly", test)]

hand[, case_def := ifelse(is.na(case_def), paste2(site, test, sep = "_"), case_def)]
hand[case_def == "anyJoint_symptAndRadiography", case_def := "reference"]

# APPEND REGION, SUPER REGION, ETC
hand <- merge(hand, loc_data, by = "location_id")
hand$country <- substr(hand$ihme_loc_id.y, 0, 3)
hand[, location_name.x := NULL]
hand[, ihme_loc_id.x := NULL]
setnames(hand, "location_name.y", "location_name")
setnames(hand, "ihme_loc_id.y", "ihme_loc_id")

# FILL OUT MEAN/CASES/SAMPLE SIZE
# CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
hand <- get_cases_sample_size(hand)
hand <- get_se(hand)

# SEX SPLIT
test <- find_sex_match(hand)
test2 <- calc_sex_ratios(test)

model_name <- paste0("oa_hand_sexsplit_", date)

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
test <- age_sex_split(hand)

# APPLY SEX SPLIT
predict_sex <- split_data(test, sex_model)

pdf("FILEPATH")
graph_predictions(predict_sex$graph)
dev.off()

write.xlsx(predict_sex$final, "FILEPATH")
test <- copy(predict_sex$final)
test <- read.xlsx("FILEPATH")

# DROP EXTRANEOUS COLUMNS
hand <- as.data.table(copy(test))
hand <- get_cases_sample_size(hand)
hand <- get_se(hand)

original_w_cd <- as.data.table(copy(hand))

hand <- hand[, c("nid", "sex", "year_start", "year_end", "age_start", "age_end", "location_id", "location_name", "ihme_loc_id", "country", "region_name", "super_region_name", "measure", "mean", "lower", "upper", "standard_error", "case_def")]

# CREATE CD MATCHES BY DEMOGRAPHICS
hand[, demographics := paste2(sex, country, measure, sep = "_")]
hand[, demographics := paste2(sex, location_name, measure, sep = "_")]

hand_cds <- list(unique(hand$case_def))

hand[, year_mean := (year_start + year_end)/2]
hand[, age_mean := (age_start + age_end)/2]

hand$case_def <- as.factor(hand$case_def)
hand$case_def <- factor(hand$case_def, levels(hand$case_def)[c(16, 1:15, 17:21)])
hand <- hand[order(case_def)]

list <- lapply(unique(hand$case_def), function(i) {
  subset(hand, case_def == i)
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

hand_matched <- copy(paired_anyJoint_physDxOnly_marketscan)
hand_matched <- hand_matched[0,]

for (i in 1:length(pairss)) {
  hand_matched <- rbind(hand_matched, get(pairss[i]))
}

hand_matched <- hand_matched[abs(year_mean.denom - year_mean.num) < 11]
hand_matched <- hand_matched[abs(age_start.denom - age_start.num) < 6 & abs(age_end.denom - age_end.num) <6]

remaining_cd <- unique(c(unique(as.character(hand_matched$case_def.num)), unique(as.character(hand_matched$case_def.denom))))

# CALCULATE RATIO AND STANDARD ERROR
hand_matched <- hand_matched %>%
  mutate(
    ratio = mean.num / mean.denom,
    ref_se = standard_error.denom, #(upper.denom - lower.denom) / 3.92,
    alt_se = standard_error.num, #(upper.num - lower.num) / 3.92,
    ratio_se =
      sqrt((mean.num^2 / mean.denom^2) * (alt_se^2/mean.num^2 + ref_se^2/mean.denom^2))
  )

hand_matched <- as.data.table(hand_matched)
hand_matched[, id := paste0(nid.num, " (", country.num, ": ", sex.num, " ", age_start.num, "-", age_end.num, ") - ", nid.denom, " (", country.denom, ": ", sex.denom, " ", age_start.denom, "-", age_end.denom, ")")]
hand_matched[, id_var := paste0(nid.num, ":", nid.denom)]

# WRITING FILES
write.xlsx(hand_matched, "FILEPATH")

# CREATE DUMMY VARIABLES

df <- copy(hand_matched)
df <- read.xlsx("FILEPATH")

df2 <- select(df, id, id_var, ratio, ratio_se, ref = case_def.denom, alt = case_def.num) %>%
  filter(!is.na(ratio) & !is.na(ratio_se))

df2$ref <- as.character(df2$ref)
df2$alt <- as.character(df2$alt)
refs <- unique(unlist(lapply(df2$ref, function(x) strsplit(x, split = "_")[[1]])))
alts <- unique(unlist(lapply(df2$alt, function(x) strsplit(x, split = "_")[[1]])))

unique(c(df2$ref, df2$alt))
case_defs <- unique(c(refs, alts))
nonGBD_case_defs <- case_defs[!case_defs == "reference"] # NO SELF REPORT

for (i in nonGBD_case_defs) df2[, i] <- 0
for (i in nonGBD_case_defs) df2[, i] <- df2[, i] - sapply(i, grepl, df2$ref)
for (i in nonGBD_case_defs) df2[, i] <- df2[, i] + sapply(i, grepl, df2$alt)

check_newvars <- lapply(nonGBD_case_defs, function(x) table(df2[, x]) )
names(check_newvars) <- nonGBD_case_defs
check_newvars

write.xlsx(df2, "FILEPATH")

# MR BRT
dat_original <- copy(original_w_cd)

dat_original[grepl("selfReport", case_def, ignore.case = T), `:=` (group = 1, specificity = "no crosswalk possible", group_review = 0)]

dat_original[, paste(case_defs) := NA]
for (i in case_defs) {
  dat_original[, paste(i) := ifelse(grepl(paste(i), case_def), 1, 0)]
}

reference_var <- "reference"
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

# RUN MR BRT
for (i in nonGBD_case_defs) if (length(unique(tmp_metareg[, i])) < 2) tmp_metareg[, i] <- NULL
varyingcomps <- NULL
for (i in nonGBD_case_defs) if (i %in% names(tmp_metareg)) varyingcomps <- append(varyingcomps, i)

varyingcomps <- varyingcomps[!(varyingcomps == "thumbCMC")]

cov_list <- lapply(varyingcomps, function(x) cov_info(x, "X"))

tmp_metareg <- tmp_metareg[is.finite(tmp_metareg$ratio) & is.finite(tmp_metareg$ratio_log) & is.finite(tmp_metareg$ratio_se) & is.finite(tmp_metareg$ratio_se_log), ]

fit1 <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = "crosswalk_network_oa_hand",
  data = tmp_metareg,
  mean_var = "ratio_log",
  se_var = "ratio_se_log",
  covs = cov_list,
  remove_x_intercept = TRUE,
  method = "trim_maxL",
  trim_pct = 0.1,
  study_id = "id_var",
  overwrite_previous = TRUE
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
df_pred <- as.data.frame(tmp_orig[, cov_names]) # cov_names from line 55 data.fr
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
final_data[grepl("thumbCMC", case_def, ignore.case = T), `:=` (group = 1, specificity = "no crosswalk possible", group_review = 0)]

write.xlsx(final_data, "FILEPATH")

########## AGE SPLITTING
source(paste0("FILEPATH", "age_split_hand.R"))

final_test <- read.xlsx("FILEPATH")
final_test <- copy(final_data)
#########
dt <- copy(final_test)
ages <- get_age_metadata(12)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_groups <- ages[, age_group_id]

df <- copy(dt)
age <- age_groups

final_split <- age_split(df = df, age = age_groups, region_pattern = F, location_pattern_id = 1)
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
bundle_id <- 6086
step2_version <- 10307

to_save_xwalk <- read.xlsx("FILEPATH")
to_save_xwalk <- as.data.table(to_save_xwalk)
to_save_xwalk <- to_save_xwalk[sex != "Both"]
to_save_xwalk[measure == "prevalence" & upper > 1, `:=` (lower = NaN, upper = NaN, uncertainty_type_value = NA)]
to_save_xwalk <- to_save_xwalk[group_review == 1 | is.na(group_review)]
to_save_xwalk[, `:=` (group = NA, specificity = NA, group_review = NA)]
to_save_xwalk[is.na(upper), `:=` (lower = NA, uncertainty_type_value = NA)]
to_save_xwalk[!is.na(upper), uncertainty_type_value := 95]
to_save_xwalk <- to_save_xwalk[!(mean > 1)]

write.xlsx(to_save_xwalk, "FILEPATH", sheetName = "extraction")
save_crosswalk_version(step2_version, "FILEPATH", description = "xwalked, age split, no thumbCMC or self-report")

