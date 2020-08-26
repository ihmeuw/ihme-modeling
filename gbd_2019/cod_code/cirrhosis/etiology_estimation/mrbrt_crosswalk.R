#########################################################################
### Purpose: CROSSWALKING GBD 2019
#########################################################################

# Cirrhosis NASH and Cryptogenic Crosswalk
rm(list=ls())

pacman::p_load(data.table, openxlsx, ggplot2)
library(mortdb)
library(msm)

# SET OBJECTS -------------------------------------------------------------

cir_dir <- "FILEPATH"

functions_dir <- "FILEPATH"
mrbrt_helper_dir <- "FILEPATH"
mrbrt_dir <- "FILEPATH"
cv_drop <- c("")
draws <- paste0("draw_", 0:999)

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_location_metadata.R"))
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/get_bundle_version.R")

source(paste0(functions_dir, "get_population.R"))

functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
            "load_mr_brt_preds_function", "plot_mr_brt_function")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}

# GET METADATA ------------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 22)
age_dt <- get_age_metadata(12)
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]

# DATA PROCESSING FUNCTIONS -----------------------------------------------

## FILL OUT MEAN/CASES/SAMPLE SIZE
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  return(dt)
}

calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size), sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

prep_data_matches <- function(raw_dt) {
  dt <- copy(raw_dt)
  dt <- dt[grepl("Y|N", case_name_specific)]
  cases <- c("Y", "N")
  for (case in cases) {
    dt[, paste0("cv_", case) := ifelse(case_name_specific == case, 1, 0)]
  }
  dt[, id_var := .GRP, by=.(location_id, nid, sex, age_start, age_end, year_start, year_end)]
  dt[, has_both := sum(cv_Y, cv_N), by = .(id_var)]
  crosswalk <- dt[has_both == 2 & note_modeler != "child", ]
  adjust <- dt[has_both == 1 & cv_Y == 1, ]
  return(list(crosswalk, adjust))
}

prep_data_denom <- function(raw_dt){
  dt <- copy(raw_dt)
  dt <- unique(dt[, .(cases2 = sum(cases), sample_size2 = sample_size), by = c("nid", "location_id", "sex")])
  dt[, mean2 := cases2/sample_size2]
  z <- qnorm(0.975)
  dt[, standard_error2 := sqrt(mean2*(1-mean2)/sample_size2 + z^2/(4*sample_size2^2))]
  return(dt)
}

prep_data_comb <- function(raw_dt, denom_dt) {
  prep_dt <- copy(raw_dt)
  prep_dt <- prep_dt[cv_Y == 1, .(nid,location_id, sex, mean, cases, sample_size, standard_error, age_start, age_end, year_start, year_end)]
  denom_dt <- copy(denom_dt)
  comb_dt <- merge(prep_dt, denom_dt, by = c("nid", "location_id", "sex"))
  return(comb_dt)
}

create_differences <- function(ratio_dt){
  dt <- copy(ratio_dt)
  dt <- dt[!mean > 1]
  dt[, `:=` (logit_mean = qlogis(mean), logit_mean2 = qlogis(mean2))]
  dt$logit_se <- sapply(1:nrow(dt), function(i){
    mean_i <- as.numeric(dt[i, "mean"])
    se_i <- as.numeric(dt[i, "standard_error"])
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  })
  dt$logit_se2 <- sapply(1:nrow(dt), function(i){
    mean_i <- as.numeric(dt[i, "mean2"])
    se_i <- as.numeric(dt[i, "standard_error2"])
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  })
  dt[, ldiff := logit_mean2 - logit_mean]
  dt[, ldiff_se := sqrt(logit_se^2 + logit_se2^2)]
  return(dt)
}

mid_year_age <- function(ratio_dt) {
  dt <- copy(ratio_dt)
  dt[, mid_year := (year_start + year_end )/ 2]
  dt[, mid_age := (age_start + age_end)/2]
  return(dt)
}


create_mrbrtdt <- function(match_dt, vars = cvs){
  dt <- copy(match_dt)
  vars <- gsub("^cv_", "", vars)
  for (var in vars){
    dt[, (var) := as.numeric(grepl(var, def2)) - as.numeric(grepl(var, def))]
    if (nrow(dt[!get(var) == 0]) == 0){
      dt[, c(var) := NULL]
      vars <- vars[!vars == var]
    }
  }
  loc_map <- copy(loc_dt[, .(location_ascii_name, location_id)])
  dt <- merge(dt, loc_map, by = "location_id")
  setnames(loc_map, c("location_id", "location_ascii_name"), c("location_id2", "location_ascii_name2"))
  dt <- merge(dt, loc_map, by = "location_id2")
  dt[, id := paste0(nid2, " (", location_ascii_name2, ": ", sex2, " ", age_start2, "-", age_end2, ") - ", nid, " (", location_ascii_name, ": ", sex, " ", age_start, "-", age_end, ")")]
  dt <- dt[, c("id", "ldiff", "ldiff_se", "def", "def2", vars), with = F]
  return(list(dt, vars))
}



# RUN DATA PREP --------------------------------------------------------------

cir_dt <- read.xlsx(FILEPATH)
cir_dt <- get_cases_sample_size(cir_dt)
cir_dt <- get_se(cir_dt)
cir_prep <- prep_data_matches(cir_dt)
cir_prep_dt <- cir_prep[[1]]
adjust_dt <- cir_prep[[2]]
cir_denom_dt <- prep_data_denom(cir_prep_dt)
cir_dt_comb <- prep_data_comb(cir_prep_dt, cir_denom_dt)
cir_dt_diff <- create_differences(cir_dt_comb)

# WRITE AND UPLOAD CRYPTOGENIC DATA TO BE CONVERTED TO NASH -------------------
upload_cryptogenic_data <- function(adjust_dt) {
  adj <- copy(adjust_dt)
  adj <- adj[, -c("cv_N", "cv_Y", "id_var", "has_both")]
  adj[, bundle_id := 3056]
  write.xlsx(adj, FILEPATH, row.names=FALSE, showNA = FALSE, sheetName = "extraction")
  result <- upload_bundle_data(bundle_id = 3056,
                               decomp_step,
                               file_path)
  print(sprintf('Request status: %s', result$request_status))
}

# RUN MR BRT MODEL  -----------------------------------------------------------
model_name <- OBJECT

if (file.exists(FILEPATH)){
   cir_model <- readr::read_rds(FILEPATH)
 } else {
  cir_model <- run_mr_brt(
    output_dir = mrbrt_dir,
    model_label = model_name,
    remove_x_intercept = F,
    data = cir_dt_diff,
    mean_var = "ldiff",
    se_var = "ldiff_se",
    # covs = cov_list,
    study_id = "nid")
   readr::write_rds(cir_model, FILEPATH)
}


# PREDICTIONS ----------------------------------------------------------
summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

cir_preds <- predict_mr_brt(cir_model, newdata = data.table(X_intercept=1), write_draws = T)

get_preds_adjustment <- function(raw_data, model){
  dt <- copy(raw_data)
  cv_cols <- names(dt)[grepl("^cv", names(dt))]
  cv_cols <- cv_cols[!cv_cols %in% cv_drop]
  dt <- unique(dplyr::select(dt, cv_cols))
  setnames(dt, names(dt), gsub("^cv_", "", names(dt)))
  dt[, sum := rowSums(.SD), .SDcols = names(dt)]
  dt <- dt[!sum == 0]
  dt[, sum := NULL]
  preds <- predict_mr_brt(model, newdata = dt, write_draws = T)
  pred_dt <- as.data.table(preds$model_draws)
  pred_dt[, ladj := rowMeans(.SD), .SDcols = draws]
  pred_dt[, ladj_se := apply(.SD, 1, sd), .SDcols = draws]
  pred_dt[, c(draws, "Z_intercept") := NULL]
  setnames(pred_dt, names(pred_dt), gsub("^X", "cv", names(pred_dt)))
  return(pred_dt)
}

make_adjustment <- function(data_dt, ratio_dt, network = F){
  if (network == T) {
    cvs <- names(ratio_dt)[grepl("^cv", names(ratio_dt))]
    dt <- merge(data_dt, ratio_dt, by = cvs, all.x = T)
  } else {
    setnames(ratio_dt, "cv_intercept", paste0("cv_", mrbrt_vars))
    cvs <- names(ratio_dt)[grepl("^cv", names(ratio_dt))]
    dt <- merge(data_dt, ratio_dt, by = cvs, all.x = T)
  }
  adjust_dt <- copy(dt[!is.na(ladj) & !mean == 0])
  adjustse_dt <- copy(dt[!is.na(ladj) & mean == 0])
  noadjust_dt <- copy(dt[is.na(ladj)])
  noadjust_dt[, c("ladj", "ladj_se") := NULL]
  ## ADJUST MEANS
  adjust_dt[, lmean := qlogis(mean)]
  adjust_dt$l_se <- sapply(1:nrow(adjust_dt), function(i){
    mean_i <- as.numeric(adjust_dt[i, "mean"])
    se_i <- as.numeric(adjust_dt[i, "standard_error"])
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  })
  adjust_dt[, lmean_adj := lmean - ladj]
  adjust_dt[!is.nan(l_se), lmean_adj_se := sqrt(ladj_se^2 + l_se^2)]
  adjust_dt[, mean_adj := plogis(lmean_adj)]
  adjust_dt$standard_error_adj <- sapply(1:nrow(adjust_dt), function(i){
    mean_i <- as.numeric(adjust_dt[i, "lmean_adj"])
    se_i <- as.numeric(adjust_dt[i, "lmean_adj_se"])
    deltamethod(~exp(x1)/(1+exp(x1)), mean_i, se_i^2)
  })
  adjust_dt[is.nan(l_se), standard_error_adj := standard_error] ## KEEP STANDARD ERROR THE SAME IF CAN'T RECALCULATE BECAUSE MEAN IS 1

  full_dt <- copy(adjust_dt)
  full_dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  full_dt[, `:=` (mean = mean_adj, standard_error = standard_error_adj, upper = "", lower = "", seq = "",
                  cases = "", uncertainty_type_value = "", effective_sample_size = "",
                  note_modeler = paste0(note_modeler, " | crosswalked with logit(difference): ", round(ladj, 2), " (",
                                        round(ladj_se, 2), ")"))]
  extra_cols <- setdiff(names(full_dt), names(noadjust_dt))
  full_dt[, c(extra_cols) := NULL]

  ## ADJUST SE FOR ZERO MEANS
  adjustse_dt$adj_se <- sapply(1:nrow(adjustse_dt), function(i){
    mean_i <- as.numeric(adjustse_dt[i, "ladj"])
    se_i <- as.numeric(adjustse_dt[i, "ladj_se"])
    deltamethod(~exp(x1)/(1+exp(x1)), mean_i, se_i^2)
  })
  adjustse_dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  adjustse_dt[, `:=` (standard_error = sqrt(standard_error^2 + adj_se^2), upper = "",
                      lower = "", seq = "", cases = "", sample_size = "",
                      uncertainty_type_value = "", effective_sample_size = "",
                      note_modeler = paste0(note_modeler, " | uncertainty from crosswalk added"))]
  extra_cols <- setdiff(names(adjustse_dt), names(noadjust_dt))
  adjustse_dt[, c(extra_cols) := NULL]

  final_dt <- rbind(noadjust_dt, adjustse_dt, full_dt, fill = T, use.names = T)
  return(list(epidb = final_dt, vetting_dt = adjust_dt))
}



# PREDICTION --------------------------------------------------------------

message("adjusting data")
adjust_dt <- copy(bundle_other_new)
adjust_dt[case_name_specific == "Y", cv_Y := 1]
adjust_dt[case_name_specific == "O", cv_Y := 0]
adjust_preds <- get_preds_adjustment(raw_data = adjust_dt, model = cir_model)
adjust_dt <- get_cases_sample_size(adjust_dt)
adjust_dt <- get_se(adjust_dt)
adjust_dt$crosswalk_parent_seq <- NA
adjusted <- make_adjustment(data_dt = adjust_dt, ratio_dt = adjust_preds)

# PREP ~OTHER~ CROSSWALK VERSION

prep_crosswalk_versions <- function(adj_dt) {
  adj <- copy(adj_dt)
  adj_nids <- adj$nid
  adj$adjusted <- 1
  old <- get_bundle_version(5366)
  old_nids <- old$nid
  drop <- intersect(old_nids, adj_nids)
  old_keep <- old[!(nid %in% drop & case_name_specific == "Y"), ]
  combined <- rbind(old_keep, adj, fill = TRUE)
  return(combined)
}

adj_other_complete <- prep_crosswalk_versions(adj_dt)


# GET NASH PROPORTION FROM CRYPTOGENIC
get_nash_estimates <- function(final_cryptogenic, original) {
  fc <- copy(adjusted$epidb[cv_crypto_only == 1, ])
  setnames(fc, c("mean", "standard_error"), c("mean_adj", "standard_error_adj"))
  fc <- fc[, c("mean_adj", "standard_error_adj", "nid", "sex", "location_id", "age_start", "age_end", "year_start", "year_end")]
  orig <- copy(bundle_other_new[cv_crypto_only == 1, ])
  dt <- merge(orig, fc, by = c("nid", "sex", "location_id", "age_start", "age_end", "year_start", "year_end"))
  dt[, mean_nash := mean - mean_adj]
  dt[, se_adj := sqrt(standard_error^2 + standard_error_adj^2)]
  dt <- unique(dt)
  dt[, `:=` (cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA, seq = NA, bundle_id = 6704, case_name = "N")]
  dt[, `:=` (mean_adj = NULL, mean = NULL, standard_error = NULL, standard_error_adj = NULL) ]
  dt[, `:=` (note_modeler = "crosswalked cryptogenic estimates and subtracted adjusted from original for NASH estimates") ]
  setnames(dt, c("mean_nash", "se_adj"), c("mean", "standard_error"))
  return(dt)
}

nash <- get_nash_estimates(final_cryptogenic = adjusted_data, original = bundle_other_new)
merge_cols <- bundle_nash[nash_crosswalked == 1, .(nid, location_id, location_name, sex, age_start, year_start, origin_seq)]
setnames(merge_cols, "origin_seq", "new_seq")
nash_seq <- merge(nash, merge_cols, by = c("nid", "location_id", "location_name", "sex", "age_start", "year_start"))
bundle_nash <- bundle_nash[is.na(nash_crosswalked), nash_crosswalked := 0]
bundle_keep <- bundle_nash[nash_crosswalked == 0, ]
nash_final <- rbind(bundle_keep, nash_seq, fill = TRUE)
nash_final <- nash_final[is.na(seq), `:=` (crosswalk_parent_seq = new_seq, origin_seq = new_seq)]
nash_final$new_seq <- NULL