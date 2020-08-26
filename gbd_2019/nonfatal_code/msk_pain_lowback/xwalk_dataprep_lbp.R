####################################################
## Description: Crosswalk Data Processing - LBP
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

# SETTING OBJECTS
date <- gsub("-", "_", Sys.Date())
draws <- paste0("draw_", 0:999)

loc_data <- as.data.table(get_location_metadata(location_set_id = 35, gbd_round_id = 6))
loc_data <- loc_data[, c("location_id", "location_name", "ihme_loc_id", "region_name", "super_region_name")]

lbp <- as.data.table(get_bundle_version(8999, export = F, transform = T))
lbp <- lbp[clinical_data_type != "inpatient"]
lbp[field_citation_value %like% "Truven Health" & year_start == 2000, cv_marketscan_all_2000 := 1]
lbp[field_citation_value %like% "Truven Health" & year_start != 2000, cv_marketscan := 1]
lbp[field_citation_value %like% "Taiwan National Health Insurance", cv_taiwan_claims_data := 1]
lbp[, "crosswalk_parent_seq" := as.numeric(NA)]

lbp <- lbp[is_outlier != 1 | is.na(is_outlier)]
lbp <- lbp[is.na(drop) | drop != 1]

lbp[, GBD := ifelse(((!is.na(case_definition) & (grepl("current", case_definition, ignore.case = T) | grepl("present", case_definition, ignore.case = T))) | (!is.na(specificity) & grepl("point", specificity, ignore.case = T))) & is.na(cv_broad) & is.na(cv_pop_school) & is.na(cv_recall_ever) & is.na(cv_recall_week) & is.na(cv_recall_month), "GBD_", NA)]
lbp[, chronic := ifelse(!is.na(case_definition) & grepl("chronic", case_definition, ignore.case = T), "chronic_", NA)]
lbp[, students := ifelse(!is.na(cv_pop_school) & cv_pop_school == 1, "students_", NA)]
lbp[, recall1mless := ifelse(!is.na(cv_recall_week) & cv_recall_week == 1, "recall1mless_", NA)]
lbp[, recall1yrless := ifelse(!is.na(cv_recall_month) & cv_recall_month == 1, "recall1yrless_", NA)]
lbp[, recallLife := ifelse(!is.na(cv_recall_ever) & cv_recall_ever == 1, "recallLife_", NA)]
lbp[, activityLimit := ifelse((!is.na(cv_beh_activity) & cv_beh_activity ==1) | (!is.na(case_definition) & grepl("activity*limit*", case_definition, ignore.case = T)), "activityLimit_", NA)]
lbp[, anatBroad := ifelse(!is.na(cv_broad) & cv_broad == 1, "anatBroad_", NA)]
lbp[, marketscan2000 := ifelse(!is.na(cv_marketscan_all_2000) & cv_marketscan_all_2000 == 1, "marketscan2000", NA)]
lbp[, marketscan2010 := ifelse(!is.na(cv_marketscan) & cv_marketscan == 1, "marketscan2010", NA)]

lbp <- lbp[is.na(recallLife)]

lbp[, case_def := paste2(GBD, chronic, students, recall1mless, recall1yrless, recallLife, activityLimit, anatBroad, marketscan2000, marketscan2010, sep = "")]
lbp <- lbp[case_def != ""]

# APPEND REGION, SUPER REGION, ETC
lbp <- merge(lbp, loc_data, by = "location_id")
lbp$country <- substr(lbp$ihme_loc_id, 0, 3)
lbp[, location_name.x := NULL]
setnames(lbp, "location_name.y", "location_name")

## FILL OUT MEAN/CASES/SAMPLE SIZE
## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
lbp$cases <- as.numeric(lbp$cases)
lbp$sample_size <- as.numeric(lbp$sample_size)
lbp <- get_cases_sample_size(lbp)
lbp <- get_se(lbp)

orig <- as.data.table(copy(lbp))

# SEX SPLIT
test <- find_sex_match(lbp)
test2 <- calc_sex_ratios(test)

model_name <- paste0("lbp_sexsplit_", date)

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
test <- age_sex_split(lbp)

# APPLY SEX SPLIT
predict_sex <- split_data(test, sex_model)

pdf("FILEPATH")
graph_predictions(predict_sex$graph)
dev.off()

write.xlsx(predict_sex$final, "FILEPATH")
test <- read.xlsx("FILEPATH")

# DROP EXTRANEOUS COLUMNS
lbp <- as.data.table(copy(test))
orig <- as.data.table(copy(lbp))

claims_data <- lbp[clinical_data_type == "claims"]
lbp <- lbp[clinical_data_type != "claims"]
claims_data <- aggregate_marketscan(claims_data)
claims_data <- claims_data[location_name == "United States" | location_name == "Taiwan"]
lbp <- rbind(lbp, claims_data)
lbp <- get_cases_sample_size(lbp)
lbp <- get_se(lbp)

lbp <- lbp[, c("nid", "sex", "year_start", "year_end", "age_start", "age_end", "location_id", "location_name", "ihme_loc_id", "country", "region_name", "super_region_name", "measure", "mean", "lower", "upper", "standard_error", "case_def")]

# CREATE CD MATCHES BY DEMOGRAPHICS

lbp[, demographics := paste2(sex, country, measure, sep = "_")]
lbp2 <- copy(lbp)
lbp2 <- lbp2[!is.na(standard_error)]

lbp_cds <- list(unique(lbp2$case_def))

lbp2[, year_mean := (year_start + year_end)/2]
lbp2[, age_mean := (age_start + age_end)/2]

lbp2$case_def <- as.factor(lbp2$case_def)
lbp2$case_def <- factor(lbp2$case_def, levels(lbp2$case_def)[c(9, 1:8, 10:23)])
lbp2 <- lbp2[order(case_def)]

list2 <- lapply(unique(lbp2$case_def), function(i) {
  subset(lbp2, case_def == i)
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

lbp_matched <- copy(paired_activityLimit_anatBroad__anatBroad_)
lbp_matched <- lbp_matched[0,]

for (i in 1:length(pairss)) {
  lbp_matched <- rbind(lbp_matched, get(pairss[i]))
}

nrow(lbp_matched[nid.denom == nid.num])
nrow(lbp_matched[nid.denom != nid.num])

lbp_matched <- lbp_matched[abs(year_mean.denom - year_mean.num) < 6]
lbp_matched <- lbp_matched[abs(age_start.denom - age_start.num) < 6 & abs(age_end.denom - age_end.num) <6]

nrow(lbp_matched[nid.denom == nid.num])
nrow(lbp_matched[nid.denom != nid.num])
# CALCULATE RATIO AND STANDARD ERROR

lbp_matched <- lbp_matched %>%
  mutate(
    ratio = mean.num / mean.denom,
    ref_se =  standard_error.denom, #(upper.denom - lower.denom) / 3.92,
    alt_se = standard_error.num, #(upper.num - lower.num) / 3.92,
    ratio_se =
      sqrt((mean.num^2 / mean.denom^2) * (alt_se^2/mean.num^2 + ref_se^2/mean.denom^2))
  )

lbp_matched <- as.data.table(lbp_matched)

lbp_matched2 <- copy(lbp_matched)
lbp_matched2[, id := paste0(nid.num, " (", country.num, ": ", sex.num, " ", age_start.num, "-", age_end.num, ") - ", nid.denom, " (", country.denom, ": ", sex.denom, " ", age_start.denom, "-", age_end.denom, ")")]
lbp_matched2[, id_var := paste0(nid.num, ":", nid.denom)]

# WRITING FILES
write.xlsx(lbp_matched2, "FILEPATH")

# SET UP DUMMY VARIABLES
df <- as.data.table("FILEPATH")
df2 <- select(df, id, id_var, mean.denom, mean.num, ref_se, alt_se, ref = case_def.denom, alt = case_def.num)

df2$ref <- as.character(df2$ref)
df2$alt <- as.character(df2$alt)
refs <- unique(unlist(lapply(df2$ref, function(x) strsplit(x, split = "_")[[1]])))
alts <- unique(unlist(lapply(df2$alt, function(x) strsplit(x, split = "_")[[1]])))

case_defs <- unique(c(refs, alts))
nonGBD_case_defs <- case_defs[!case_defs == "GBD"]

df2 <- as.data.frame(df2)
for (i in nonGBD_case_defs) df2[, i] <- 0
for (i in nonGBD_case_defs) df2[, i] <- df2[, i] - sapply(i, grepl, df2$ref)
for (i in nonGBD_case_defs) df2[, i] <- df2[, i] + sapply(i, grepl, df2$alt)

check_newvars <- lapply(nonGBD_case_defs, function(x) table(df2[, x]) )
names(check_newvars) <- nonGBD_case_defs
check_newvars

# MR BRT

# original extracted data
# -- read in data frame
# -- specify how to identify reference studies
# -- identify mean, standard error and covariate variables

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
dat_original[, marketscan2000 := ifelse(!is.na(marketscan2000), 1, 0)]
dat_original[, marketscan2010 := ifelse(!is.na(marketscan2010), 1, 0)]

reference_value <- 1
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- nonGBD_case_defs # can be a vector of names

##
dat_metareg <- copy(df2)
prev_ref_var <- "mean.denom"
se_prev_ref_var <- "ref_se"
prev_alt_var <- "mean.num"
se_prev_alt_var <- "alt_se"

# create datasets with standardized variable names
orig_vars <- c(mean_var, se_var, reference_var, cov_names)

metareg_vars <- c(prev_ref_var, se_prev_ref_var, prev_alt_var, se_prev_alt_var, cov_names)
id_vars <- c("id", "id_var", "ref", "alt")
metareg_vars2 <- c(id_vars, metareg_vars)

tmp_orig <- as.data.frame(dat_original) %>%
  .[, orig_vars] %>%
  setnames(orig_vars, c("mean", "se", "ref", cov_names)) %>%
  mutate(ref = if_else(ref == reference_value, 1, 0, 0))

tmp_metareg <- as.data.frame(dat_metareg) %>%
  .[, metareg_vars2] %>%
  setnames(metareg_vars, c("prev_ref", "se_prev_ref", "prev_alt", "se_prev_alt", cov_names))

# logit transform the original data
# -- SEs transformed using the delta method
tmp_orig$mean_logit <- log(tmp_orig$mean / (1-tmp_orig$mean))
tmp_orig$se_logit <- sapply(1:nrow(tmp_orig), function(i) {
  mean_i <- tmp_orig[i, "mean"]
  se_i <- tmp_orig[i, "se"]
  deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
})

# logit transform the meta-regression data
# -- alternative
tmp_metareg$prev_logit_alt <- log(tmp_metareg$prev_alt / (1-tmp_metareg$prev_alt))
tmp_metareg$se_prev_logit_alt <- sapply(1:nrow(tmp_metareg), function(i) {
  prev_i <- tmp_metareg[i, "prev_alt"]
  prev_se_i <- tmp_metareg[i, "se_prev_alt"]
  deltamethod(~log(x1/(1-x1)), prev_i, prev_se_i^2)
})

# -- reference
tmp_metareg$prev_logit_ref <- log(tmp_metareg$prev_ref / (1-tmp_metareg$prev_ref))
tmp_metareg$se_prev_logit_ref <- sapply(1:nrow(tmp_metareg), function(i) {
  prev_i <- tmp_metareg[i, "prev_ref"]
  prev_se_i <- tmp_metareg[i, "se_prev_ref"]
  deltamethod(~log(x1/(1-x1)), prev_i, prev_se_i^2)
})

tmp_metareg$diff_logit <- tmp_metareg$prev_logit_alt - tmp_metareg$prev_logit_ref
tmp_metareg$se_diff_logit <- sqrt(tmp_metareg$se_prev_logit_alt^2 + tmp_metareg$se_prev_logit_ref^2)

model_name <- "crosswalk_network_lbp_logit"

covs1 <- list()
for (nm in cov_names) covs1 <- append(covs1, list(cov_info(nm, "X")))
covs1[[7]]$uprior_lb <- 0
covs1[[7]]$uprior_ub <- Inf

tmp_metareg <- tmp_metareg[is.finite(tmp_metareg$prev_logit_alt) & is.finite(tmp_metareg$prev_logit_ref) & is.finite(tmp_metareg$se_prev_logit_alt) & is.finite(tmp_metareg$se_prev_logit_ref) & is.finite(tmp_metareg$diff_logit) & is.finite(tmp_metareg$se_diff_logit), ]

  fit1 <- run_mr_brt(
    output_dir = "FILEPATH",
    model_label = model_name,
    data = tmp_metareg,
    mean_var = "diff_logit",
    se_var = "se_diff_logit",
    covs = covs1,
    overwrite_previous = TRUE,
    method = "trim_maxL",
    trim_pct = 0.1,
    study_id = "id_var"
  )


preds <- predict_mr_brt(fit1, newdata = tmp_orig)$model_summaries
tmp_orig$adj_logit <- preds$Y_mean
tmp_orig$se_adj_logit <- (preds$Y_mean_hi - preds$Y_mean_lo) / 3.92

tmp_orig2 <- tmp_orig %>%
  mutate(
    prev_logit_adjusted_tmp = mean_logit - adj_logit,
    se_prev_logit_adjusted_tmp = sqrt(se_logit^2 + se_adj_logit^2),
    prev_logit_adjusted = ifelse(ref == 1, mean_logit, prev_logit_adjusted_tmp),
    se_prev_logit_adjusted = ifelse(ref == 1, se_logit, se_prev_logit_adjusted_tmp),
    prev_adjusted = exp(prev_logit_adjusted)/(1+exp(prev_logit_adjusted)),
    lo_logit_adjusted = prev_logit_adjusted - 1.96 * se_prev_logit_adjusted,
    hi_logit_adjusted = prev_logit_adjusted + 1.96 * se_prev_logit_adjusted,
    lo_adjusted = exp(lo_logit_adjusted)/(1+exp(lo_logit_adjusted)),
    hi_adjusted = exp(hi_logit_adjusted)/(1+exp(hi_logit_adjusted))
  )


tmp_orig2$se_adjusted <- sapply(1:nrow(tmp_orig2), function(i) {
  mean_i <- tmp_orig2[i, "prev_logit_adjusted"]
  mean_se_i <- tmp_orig2[i, "se_prev_logit_adjusted"]
  deltamethod(~exp(x1)/(1+exp(x1)), mean_i, mean_se_i^2)
})

# 'final_data' is the original extracted data plus the new variables
final_data <- cbind(
  dat_original,
  as.data.frame(tmp_orig2)[, c("ref", "prev_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
)

final_data <- as.data.table(final_data)
setnames(final_data, c("mean", "upper", "lower", "standard_error"), c("mean_old", "upper_old", "lower_old", "se_old"))
setnames(final_data, c("prev_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted"), c("mean", "standard_error", "lower", "upper"))
write.xlsx(final_data, "FILEPATH")


########## AGE SPLITTING
source(paste0("FILEPATH", "age_split_fns.R"))

final_test <- read.xlsx("FILEPATH")

#########
dt <- copy(final_test)
ages <- get_age_metadata(12)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_groups <- ages[, age_group_id]
id <- 2149

df <- copy(dt)
age <- age_groups
gbd_id <- id

final_split <- age_split(gbd_id = id, df = dt, age = age_groups, region_pattern = F, location_pattern_id = 1)
write.xlsx(final_split, "FILEPATH")

# SAVE CROSSWALK VERSION
source(paste0("FILEPATH", "save_crosswalk_version.R"))
acause <- "msk_pain_lowback"
bundle_id <- 217
step2_version <- 8996

to_save_xwalk <- read.xlsx("FILEPATH")
to_save_xwalk <- as.data.table(to_save_xwalk)
to_save_xwalk <- to_save_xwalk[sex != "Both"]
to_save_xwalk[measure == "prevalence" & upper > 1, `:=` (lower = NaN, upper = NaN, uncertainty_type_value = NA)]
to_save_xwalk <- to_save_xwalk[group_review == 1 | is.na(group_review)]
to_save_xwalk[, `:=` (group = NA, specificity = NA, group_review = NA)]
to_save_xwalk[is.na(upper), `:=` (lower = NA, uncertainty_type_value = NA)]
to_save_xwalk[!is.na(upper), uncertainty_type_value := 95]
to_save_xwalk <- to_save_xwalk[mean <= 1]

write.xlsx(to_save_xwalk, "FILEPATH", sheetName = "extraction")
save_crosswalk_version(step2_version, "FILEPATH", description = "xwalked, age split, student beta 0")
