####################################################
## Date: 6/28/19
## Description: Crosswalk Data Processing - MIGRAINE PROBABLE AND DEFINITE
####################################################
rm(list=ls())


shared_functions_dir <- 'FILEPATH'

library(readxl)
library(data.table)
library(ggplot2)
library(dplyr)
library(tidyr)
library(Hmisc, lib.loc = paste0(h_root, "package_lib/"))
library(mvtnorm)
library(survival, lib.loc = paste0(h_root, "package_lib/"))
library(expm, lib.loc = paste0(h_root, "package_lib/"))
# library(msm, lib.loc = "/home/j/temp/reed/prog/R_libraries/")
library(msm, lib.loc = paste0(h_root, "package_lib/"))
library(openxlsx)
library(metafor, lib.loc = 'FILEPATH')
library(mortdb, lib ='FILEPATH')

mr_brt_dir <- 'FILEPATH'
source(paste0(mr_brt_dir, "cov_info_function.R"))
source(paste0(mr_brt_dir, "run_mr_brt_function.R"))
source(paste0(mr_brt_dir, "check_for_outputs_function.R"))
source(paste0(mr_brt_dir, "load_mr_brt_outputs_function.R"))
source(paste0(mr_brt_dir, "predict_mr_brt_function.R"))
source(paste0(mr_brt_dir, "check_for_preds_function.R"))
source(paste0(mr_brt_dir, "load_mr_brt_preds_function.R"))
source(paste0(mr_brt_dir, "plot_mr_brt_function.R"))

source(paste0(j_root, 'FILEPATH'))
source(paste0(j_root, 'FILEPATH'))
source(paste0(j_root, 'FILEPATH'))
source(paste0(j_root, 'FILEPATH')
source(paste0(shared_functions_dir, "get_bundle_version.R"))

# SETTING OBJECTS
main_dir <- paste0(j_root, "temp/prhine22/")
bundle_dir <- paste0(j_root, "WORK/12_bundle/")

date <- gsub("-", "_", Sys.Date())
draws <- paste0("draw_", 0:999)

loc_data <- as.data.table(get_location_metadata(location_set_id = 35, gbd_round_id = 6))
loc_data <- loc_data[, c("location_id", "location_name", "ihme_loc_id", "region_name", "super_region_name")]

# LOADING DATA

md <- as.data.table(get_bundle_version(7526, export = T, transform = T))
mp <- as.data.table(get_bundle_version(7523, export = T, transform = T))

drop <- setdiff(names(mp), names(md))
mp[, drop] <- NULL
mp[, "seq_parent" := as.numeric(NA)]

migraine <- rbind(mp, md)
migraine[, "crosswalk_parent_seq" := as.numeric(NA)]
migraine <- migraine[is_outlier != 1]

migraine[, GBD := ifelse(cv_recall_lifetime != 1 & cv_students != 1 & cv_type_assumed != 1 & cv_poor_response != 1 & cv_recall_not_1yr != 1 & cv_headache_survey_method != 1 & cv_headache_diagnostic_criteria != 1 & cv_not_represent != 1 & cv_valid_diagnostic_inst != 1 & cv_sample_method != 1, "GBD_", NA)]
migraine[, recallLifetime := ifelse(cv_recall_lifetime != 0, "recallLifetime_", NA)]
migraine[, surveyMethod := ifelse(cv_headache_survey_method != 0, "surveyMethod_", NA)]
migraine[, diagnosticCriteria := ifelse(cv_headache_diagnostic_criteria != 0, "diagnosticCriteria_", NA)]
migraine[, diagnosticInst := ifelse(cv_valid_diagnostic_inst != 0, "diagnosticInst_", NA)]
migraine[, sampleMethod := ifelse(cv_sample_method != 0, "sampleMethod_", NA)]
migraine[, recallNot1yr := ifelse(cv_recall_not_1yr != 0, "recallNot1yr_", NA)]
migraine[, poorResponse := ifelse(cv_poor_response != 0, "poorResponse_", NA)]
migraine[, notRepresent := ifelse(cv_not_represent != 0, "notRepresent_", NA)]
migraine[, student := ifelse(cv_students != 0, "student_", NA)]
migraine[, typeAssumed := ifelse(cv_type_assumed != 0, "typeAssumed_", NA)]

migraine[, case_def:= paste2(GBD, recallLifetime, surveyMethod, diagnosticCriteria, diagnosticInst, sampleMethod, recallNot1yr, poorResponse, notRepresent, student, typeAssumed, sep = "")]

# APPEND REGION, SUPER REGION, ETC

migraine <- merge(migraine, loc_data, by = "location_id")
migraine[, location_name.x := NULL]
migraine[, ihme_loc_id.x := NULL]
setnames(migraine, c("ihme_loc_id.y", "location_name.y"), c("ihme_loc_id", "location_name"))
migraine$country <- substr(migraine$ihme_loc_id, 0, 3)

## FILL OUT MEAN/CASES/SAMPLE SIZE
## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS

migraine$cases <- as.numeric(migraine$cases)
migraine$sample_size <- as.numeric(migraine$sample_size)
migraine <- get_cases_sample_size(migraine)
migraine <- get_se(migraine)

original_w_cd <- copy(migraine)

# SEX SPLIT
test <- find_sex_match(migraine)
test2 <- calc_sex_ratios(test)

model_name <- paste0("mpdcombo_sexsplit_", date)

sex_model <- run_mr_brt(
  output_dir = 'FILEPATH',
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
test <- age_sex_split(migraine)

# APPLY SEX SPLIT
predict_sex <- split_data(test, sex_model)

pdf(paste0('FILEPATH'))
graph_predictions(predict_sex$graph)
dev.off()

write.xlsx(predict_sex$final, 'FILEPATH')
test <- read.xlsx('FILEPATH')


# DROP EXTRANEOUS COLUMNS
migraine <- as.data.table(copy(test))
migraine <- migraine[, c("nid", "sex", "year_start", "year_end", "age_start", "age_end", "location_id", "location_name", "ihme_loc_id", "country", "region_name", "super_region_name", "measure", "mean", "lower", "upper", "standard_error", "case_def")]

# CREATE CD MATCHES BY DEMOGRAPHICS
migraine[, demographics := paste2(sex, country, measure, sep = "_")]

migraine_cds <- list(unique(migraine$case_def))

migraine[, year_mean := (year_start + year_end)/2]
migraine[, age_mean := (age_start + age_end)/2]

migraine$case_def <- as.factor(migraine$case_def)
migraine$case_def <- factor(migraine$case_def, levels(migraine$case_def)[c(20, 1:19, 21:70)])
migraine <- migraine[order(case_def)]

list <- lapply(unique(migraine$case_def), function(i) {
  subset(migraine, case_def == i)
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
rm_pairss <- c() # clear out matched cds with 0 observations
for (i in pairss) {
  if (nrow(get(i)) < 1) {
    rm_pairss <- c(rm_pairss, paste0(i))
  }
}
rm(list = rm_pairss, envir = .GlobalEnv)
pairss <- grep("paired", names(.GlobalEnv), value = T)

migraine_matched <- copy(paired_diagnosticCriteria_student__student_)
migraine_matched <- migraine_matched[0,]

for (i in 1:length(pairss)) {
  migraine_matched <- rbind(migraine_matched, get(pairss[i]))
}

nrow(migraine_matched[nid.denom == nid.num])
nrow(migraine_matched[nid.denom != nid.num])

migraine_matched <- migraine_matched[abs(year_mean.denom - year_mean.num) < 6]
# migraine_matched <- migraine_matched[abs(age_mean.denom - age_mean.num) < 6]
migraine_matched <- migraine_matched[abs(age_start.denom - age_start.num) < 6 & abs(age_end.denom - age_end.num) <6]
# migraine_matched <- migraine_matched[age_start.denom == age_start.num & age_end.denom == age_end.num]

nrow(migraine_matched[nid.denom == nid.num])
nrow(migraine_matched[nid.denom != nid.num])

unique(migraine_matched$case_def.num)
unique(migraine_matched$case_def.denom)
remaining_cd <- unique(c(unique(as.character(migraine_matched$case_def.num)), unique(as.character(migraine_matched$case_def.denom))))

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

migraine_matched <- migraine_matched %>%
  mutate(
    ratio = mean.num / mean.denom,
    ref_se = standard_error.denom, #(upper.denom - lower.denom) / 3.92,
    alt_se = standard_error.num, #(upper.num - lower.num) / 3.92,
    ratio_se =
      sqrt((mean.num^2 / mean.denom^2) * (alt_se^2/mean.num^2 + ref_se^2/mean.denom^2))
  )

migraine_matched <- as.data.table(migraine_matched)
migraine_matched[, id := paste0(nid.num, " (", country.num, ": ", sex.num, " ", age_start.num, "-", age_end.num, ") - ", nid.denom, " (", country.denom, ": ", sex.denom, " ", age_start.denom, "-", age_end.denom, ")")]
migraine_matched[, id_var := paste0(nid.num, ":", nid.denom)]

# WRITING FILES
write.xlsx(migraine_matched, paste0(main_dir, 'FILEPATH', date, ".xlsx"))


# SET UP DUMMY VARIABLES
df <- as.data.table(read_xlsx(paste0('FILEPATH'))

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

write.xlsx(df2, 'FILEPATH')

# MR BRT PREP

# original extracted data
# -- read in data frame
# -- specify how to identify reference studies
# -- identify mean, standard error and covariate variables

dat_original <- as.data.table(copy(original_w_cd))

dat_original[, GBD := ifelse(!is.na(GBD), 1, 0)]
dat_original[, recallLifetime := ifelse(!is.na(recallLifetime), 1, 0)]
dat_original[, surveyMethod := ifelse(!is.na(surveyMethod), 1, 0)]
dat_original[, diagnosticCriteria := ifelse(!is.na(diagnosticCriteria), 1, 0)]
dat_original[, diagnosticInst := ifelse(!is.na(diagnosticInst), 1, 0)]
dat_original[, sampleMethod := ifelse(!is.na(sampleMethod), 1, 0)]
dat_original[, recallNot1yr := ifelse(!is.na(recallNot1yr), 1, 0)]
dat_original[, poorResponse := ifelse(!is.na(poorResponse), 1, 0)]
dat_original[, notRepresent := ifelse(!is.na(notRepresent), 1, 0)]
dat_original[, student := ifelse(!is.na(student), 1, 0)]
dat_original[, typeAssumed := ifelse(!is.na(typeAssumed), 1, 0)]

reference_var <- "GBD"
reference_value <- 1
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- nonGBD_case_defs # can be a vector of names

# data for meta-regression model
# -- read in data frame
# -- identify variables for ratio and SE
#    NOTE: ratio must be specified as reference/alternative

dat_metareg <- read_xlsx('FILEPATH')
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

# REMOVE DUMMY VARIABLES FOR WHICH THERE IS NO VARIATION
for (i in nonGBD_case_defs) if (length(unique(tmp_metareg[, i])) < 2) tmp_metareg[, i] <- NULL

# tmp_metareg[, c("sampleMethod", "diagnosticCriteria")] <- NULL #"sampleMethod"

varyingcomps <- NULL
for (i in nonGBD_case_defs) if (i %in% names(tmp_metareg)) varyingcomps <- append(varyingcomps, i)

# cov_list <- lapply(nonGBD_case_defs, function(x) cov_info(x, "X"))

cov_list <- lapply(varyingcomps, function(x) cov_info(x, "X"))

# RUN MR BRT

model_name <- "crosswalk_network_mpdcombo_withpriors"
tmp_metareg <- tmp_metareg[is.finite(tmp_metareg$ratio) & is.finite(tmp_metareg$ratio_log) & is.finite(tmp_metareg$ratio_se) & is.finite(tmp_metareg$ratio_se_log), ]

fit1 <- run_mr_brt(
  output_dir = 'FILEPATH',
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

# 'final_data' is the original extracted data plus the new variables
final_data <- cbind(
  dat_original,
  tmp_orig3[, c("ref", "mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted")]
)

final_data <- as.data.table(final_data)
write.xlsx(final_data, paste0('FILEPATH', model_name, "/final_result_", date, ".xlsx"))

##################################

summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

graph_combos <- function(model, predictions){
  data <- as.data.table(model$train_data)
  data[, ratio_name := paste0(alt, " / ", ref)]
  # if (length(names(data)[grep("^X", names(data))]) > 0){
  #   name_change <- names(data)[grepl("^X", names(data))]
  #   setnames(data, name_change, gsub("^X", "", name_change))
  # }
  # setnames(data, "X1066", "1066", skip_absent = T)
  data[, `:=` (log_ratio_l = ratio_log - 1.96*ratio_se_log, log_ratio_u = ratio_log + 1.96*ratio_se_log)]
  preds <- as.data.table(predictions$model_draws)
  preds <- summaries(preds, draws)
  xcov_names <- names(preds)[grepl("^X", names(preds))]
  setnames(preds, xcov_names, gsub("^X_", "", xcov_names))
  preds <- merge(preds, unique(data[, c("ratio_name", varyingcomps), with = F]), by = varyingcomps)
  fit_graph <- function(n){
    rn <- unique(preds)[, ratio_name][n]
    ratio_mean <- unique(preds)[ratio_name == rn, mean]
    ratio_lower <- unique(preds)[ratio_name == rn, lower]
    ratio_upper <- unique(preds)[ratio_name == rn, upper]
    graph_dt <- copy(data[ratio_name == rn])
    gg <- ggplot() +
      geom_rect(data = graph_dt[1,], xmin = ratio_lower, xmax = ratio_upper, ymin = -Inf, ymax = Inf, alpha = 0.2, fill = "darkorchid") +
      geom_point(data = graph_dt, aes(x = ratio_log, y = id, color = as.factor(w))) +
      geom_errorbarh(data = graph_dt, aes(x = ratio_log, y = id, xmin = log_ratio_l, xmax = log_ratio_u, color = as.factor(w))) +
      geom_vline(xintercept = ratio_mean, linetype = "dashed", color = "darkorchid") +
      geom_vline(xintercept = 0) +
      labs(x = "Log Effect Size", y = "") +
      xlim(-3, 5.5) +
      scale_color_manual(name = "", values = c("1" = "midnightblue", "0" = "red"), labels = c("0" = "Trimmed", "1" = "Included")) +
      ggtitle(paste0("Model fit for ratio ", rn)) +
      theme_classic() +
      theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(),
            axis.text.y = element_text(size = 5))
    return(gg)
  }
  fit_graphs <- lapply(1:length(preds[, unique(ratio_name)]), fit_graph)
  return(fit_graphs)
}

graphs_modelfit <- graph_combos(model = fit1, predictions = pred1)

pdf(paste0('FILEPATH', model_name, "/fit_graphs.pdf"), width = 12)
graphs_modelfit
dev.off()
