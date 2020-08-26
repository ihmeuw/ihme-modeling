##########################################################################
### Purpose: MRBRT CROSSWALK GBD 2019
##########################################################################
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- FILEPATH
  h <- "~/"
  l <- FILEPATH
} else {
  j <- "J:/"
  h <- "H:/"
  l <- "L:/"
}

pacman::p_load(data.table, openxlsx, ggplot2)
library(msm)
library(Hmisc)
library(mortdb, lib = FILEPATH)

# SET OBJECTS -------------------------------------------------------------

b_id <- OBJECT
a_cause <- OBJECT
version <- OBJECT
mrbrt_dir <- DIRECTORY


functions_dir <- FILEPATH
mrbrt_helper_dir <- FILEPATH
cv_drop <- OBJECT
draws <- paste0("draw_", 0:999)

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_location_metadata.R"))
source(paste0(functions_dir, "get_age_metadata.R"))
source("FILEPATH/get_ids.R")
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_crosswalk_version.R")

source(paste0(functions_dir, "get_population.R"))

functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
            "load_mr_brt_preds_function", "plot_mr_brt_function")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}

# GET METADATA ------------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 22)
age_dt <- FILE_PATH
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
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  return(dt)
}

calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size), sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

## GET DEFINITIONS 
get_definitions <- function(ref_dt){
  dt <- copy(ref_dt)
  cvs <- names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop]
  cvs <- cvs[!cvs %in% cv_drop] 
  dt[, definition := ""]
  for (cv in cvs){
    dt[get(cv) == 1, definition := paste0(definition, "_", cv)]
  }
  dt[definition == "", definition := "reference"]
  return(list(dt, cvs))
}

subnat_to_nat <- function(subnat_dt, subnat){
  dt <- copy(subnat_dt)
  dt[, ihme_loc_id := NULL]
  dt <- merge(dt, loc_dt[, .(location_id, level, parent_id, location_type, ihme_loc_id)], by = "location_id")
  dt[, loc_match := location_id]
  if (subnat == T) {
    dt[level >= 4, loc_match := parent_id]
    dt[level == 6, loc_match := 4749] ## PAIR UTLAS TO ENGLAND
    dt[level == 5 & grepl("GBR", ihme_loc_id), loc_match := 4749] ## PAIR ENGLAND REGIONS TO ENGLAND
    dt[level == 5 & location_type %in% c("urbanicity", "admin2"), loc_match := 163] ## PAIR INDIA URBAN/RURAL TO INDIA
    dt[level == 5 & grepl("CHN", ihme_loc_id), loc_match := 6]
    dt[level == 5 & grepl("KEN", ihme_loc_id), loc_match := 180]
    message("Matching subnational locations to other subnational locations")
  } else {
    message("NOT matching subnationals locations to other subnational locations")
  }
  return(dt)
}

calc_year <- function(year_dt){
  dt <- copy(year_dt)
  dt[, year_match := (year_start+year_end)/2]
  return(dt)
}

## GET UNIQUE "AGE SERIES" FOR THE PURPOSE OF AGGREGATING
get_age_combos <- function(agematch_dt){
  dt <- copy(agematch_dt)
  by_vars <- c("nid", "location_id","year_match", "sex", "measure", names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop])
  dt[, age_n := .GRP, by = by_vars]
  small_dt <- copy(dt[, c(by_vars, "age_start", "age_end","age_n"), with = F])
  return(list(dt, small_dt))
}

## HELPER FUNCTION TO ROUND TO NEAREST AGE START/AGE END IN GBD AGE GROUPS
get_closest_age <- function(i, var, start = T, dt){
  age <- dt[i, get(var)]
  if (start == T){
    age_dt[, age_group_years_start][which.min(abs(age_dt[, age_group_years_start] - age))]
  } else if (start == F){
    age_dt[, age_group_years_end][which.min(abs(age_dt[, age_group_years_end] - age))]
  }
}

## ACTUALLY AGGREGATE!
aggregation <- function(start_match, end_match, dt = pull_dt){
  agg <- nrow(dt[start_match == age_start & end_match == age_end]) == 0 ## FLAGS TO AGGREGATE EACH SIDE OF THE RATIO
  agg1 <- nrow(dt[start_match == age_start2 & end_match == age_end2]) == 0
  z <- qnorm(0.975)
  if (agg == T){ ## AGGREGATE FIRST SIDE
    row_dt <- unique(dt[age_start >= start_match & age_end <= end_match], by = c("age_start", "age_end"))
    row_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size))]
    row_dt[, `:=` (mean = cases/sample_size, age_start = min(age_start), age_end = max(age_end))]
    row_dt[measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    row_dt[measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    row_dt[measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
    row_dt <- dplyr::select(row_dt, names(row_dt)[!grepl("2$", names(row_dt))])
    row_dt <- unique(row_dt, by = "nid")
  } else { ## OR LEAVE THE SAME
    row_dt <- unique(dt[age_start == start_match & age_end == end_match], by = c("age_start", "age_end"))
    row_dt <- dplyr::select(row_dt, names(row_dt)[!grepl("2$", names(row_dt))])
  }
  if (agg1 == T){ ## AGGREGATE SECOND SIDE
    row_dt2 <- unique(dt[age_start2 >= start_match & age_end2 <= end_match], by = c("age_start2", "age_end2"))
    row_dt2[, `:=` (cases2 = sum(cases2), sample_size2 = sum(sample_size2))]
    row_dt2[, `:=` (mean2 = cases2/sample_size2, age_start2 = min(age_start2), age_end2 = max(age_end2))]
    row_dt2[measure == "prevalence", standard_error2 := sqrt(mean2*(1-mean2)/sample_size2 + z^2/(4*sample_size2^2))]
    row_dt2[measure == "incidence" & cases2 < 5, standard_error2 := ((5-mean2*sample_size2)/sample_size2+mean2*sample_size2*sqrt(5/sample_size2^2))/5]
    row_dt2[measure == "incidence" & cases2 >= 5, standard_error2 := sqrt(mean2/sample_size2)]
    row_dt2 <- dplyr::select(row_dt2, names(row_dt2)[grepl("2$", names(row_dt2))])
    row_dt2 <- unique(row_dt2, by = "nid2")
  } else { ## OR LEAVE THE SAME
    row_dt2 <- unique(dt[age_start2 == start_match & age_end2 == end_match], by = c("age_start2", "age_end2"))
    row_dt2 <- dplyr::select(row_dt2, names(row_dt2)[grepl("2$", names(row_dt2))])
  }
  new_row <- cbind(row_dt, row_dt2) ## BUT THE SIDES BACK TOGETHER
  return(new_row)
}

## SET UP AGGREGATION
aggregate_tomatch <- function(match_dt, id){
  print(id)
  pull_dt <- copy(match_dt[agg_id == id])
  start_matches <- pull_dt[age_start_match == T, age_start] ## GET ALL START MATCHES (NOTE: NOT SURE WHAT WILL HAPPEN IF YOU HAVE TWO START MATCHES ON THE SAME AGE)
  allend_matches <- pull_dt[age_end_match == T, unique(age_end)] ## GET ALL END MATCHES
  end_matches <- c() ## MAKE SURE ACTUAL END MATCHES MAP 1:1 WITH START MATCHES
  for (x in 1:length(start_matches)){
    match <- allend_matches[allend_matches > start_matches[x]]
    match <- match[which.min(match-start_matches[x])]
    end_matches <- c(end_matches, match)
  }
  aggregated <- rbindlist(lapply(1:length(start_matches), function(x) aggregation(start_match = start_matches[x],  end_match = end_matches[x], dt = pull_dt)))
  return(aggregated)
}

## FULL FUNCTION TO GET MATCHES
get_matches <- function(n, pair_dt, year_span = 10, age_span = 5){
  dt <- copy(pair_dt)
  pair <- pairs[,n]
  print(pair)
  if (pair[1] == "_cv_marketscan" | pair[2] == "_cv_marketscan") year_span <- 20
  keep_vars <- c("nid", "location_id", "sex", "year_start", "year_end", "measure", "age_start", "age_end",
                 "mean", "standard_error", "cases", "sample_size", "loc_match", "year_match", "age_n")
  dt1 <- copy(dt[definition == pair[1]])
  dt1 <- dplyr::select(dt1, keep_vars)
  dt2 <- copy(dt[definition == pair[2]])
  dt2 <- dplyr::select(dt2, keep_vars)
  setnames(dt2, keep_vars[!keep_vars  == "loc_match"], paste0(keep_vars[!keep_vars  == "loc_match"], "2"))
  matched <- merge(dt1, dt2, by = "loc_match", allow.cartesian = T) ## INITIAL CARTISIAN MERGE ONLY MATCHING ON LOCATION
  matched <- matched[sex == sex2 & measure == measure2 & year_match %between% list(year_match2 - year_span/2, year_match2 + year_span/2)] ## FILETER OUT SEX, MEASURE, YEAR
  if (pair[1] == "_cv_marketscan" | pair[2] == "_cv_marketscan"){ ## EXACT LOCS FOR MARKETSCAN BUT CURRENTLY ALL ARE EXACT LOCS
    matched <- matched[location_id == location_id2]
  }
  matched[, age_match := age_start %between% list(age_start2 - age_span/2, age_start2 + age_span/2) & age_end %between% list(age_end2 - age_span/2, age_end2 + age_span/2)]
  unmatched <- copy(matched[age_match == F])
  ## AGE ROUNDING
  unmatched$age_start <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_start", dt = unmatched))
  unmatched$age_end <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_end", start = F, dt = unmatched))
  unmatched$age_start2 <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_start2", dt = unmatched))
  unmatched$age_end2 <- sapply(1:nrow(unmatched), function(i) get_closest_age(i, var = "age_end2", start = F, dt = unmatched))

  ## FIND WHERE IT IS POSSIBLE TO AGGREGATE
  unmatched[, age_start_match := age_start == age_start2]
  unmatched[, age_end_match := age_end == age_end2]
  unmatched[, sum_start := sum(age_start_match), by = c("age_n", "age_n2")]
  unmatched[, sum_end := sum(age_end_match), by = c("age_n", "age_n2")]
  agg_matches <- copy(unmatched[sum_start > 0 & sum_end > 0 & !is.na(cases) & !is.na(sample_size) & !is.na(cases2) & !is.na(sample_size2)])
  agg_matches[, agg_id := .GRP, by = c("age_n", "age_n2")]

  ## AGGREGATE IF THERE ARE PLACES TO AGGREGATE OTHERWISE DO NOTHING
  if (nrow(agg_matches) > 0){
    aggregated <- rbindlist(lapply(1:agg_matches[, max(agg_id)], function(x) aggregate_tomatch(match_dt = agg_matches, id = x)))
    aggregated[, c("age_start_match", "age_end_match", "sum_start", "sum_end", "agg_id") := NULL]
    final_match <- rbind(matched[age_match == T], aggregated)
  } else {
    final_match <- copy(matched[age_match == T])
  }

  final_match[, c("age_match") := NULL]
  final_match[, `:=` (def = pair[1], def2 = pair[2])] ## LABEL WITH DEFINITIONS
  return(final_match)
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

create_mrbrtdt <- function(match_dt, vars = cvs, age = F){

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
  if (age == F){
    dt <- dt[, c("id", "ldiff", "ldiff_se", "def", "def2", vars), with = F]
  } else if (age == T){
    dt <- dt[, c("id", "age", "ldiff", "ldiff_se", "def", "def2", vars), with = F]
  }
  return(list(dt, vars))
}



# RUN DATA SETUP ----------------------------------------------------------
hep_dt <- copy(original_hep)
hep_dt <- hep_dt[!is_outlier == 1 & measure %in% c("prevalence")]
hep_dt <- get_cases_sample_size(hep_dt)
hep_dt <- get_se(hep_dt)
hep_dt <- calculate_cases_fromse(hep_dt)
hep_dt <- hep_dt[!is.na(sample_size)]
defs <- get_definitions(hep_dt)
hep_dt <- defs[[1]]
cvs <- defs[[2]]
hep_dt <- subnat_to_nat(hep_dt, subnat = F)
hep_dt <- calc_year(hep_dt)
age_dts <- get_age_combos(hep_dt)
hep_dt <- age_dts[[1]]
age_match_dt <- age_dts[[2]] ## DON'T ACTUALLY NEED THIS BUT FOUND IT HELPFUL FOR VETTING
pairs <- combn(hep_dt[, unique(definition)], 2)
matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = hep_dt)))
diff <- create_differences(matches)
nrow(diff[(grepl("Inf", ldiff))])
diff <- diff[!(grepl("Inf", ldiff))]
mrbrt_setup <- create_mrbrtdt(diff, cvs, age = F)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]

# RUN MR-BRT MODEL --------------------------------------------------------

model_name <- MODEL_NAME

cov_list <- lapply(mrbrt_vars, function(x) cov_info(x, "X"))

if (file.exists(FILE_PATH)){
  hep_model <- readr::read_rds(FILE_PATH)
} else { hep_model <- run_mr_brt(
       output_dir = mrbrt_dir,
       model_label = model_name,
       remove_x_intercept = F,
       data = mrbrt_dt,
       mean_var = "ldiff",
       se_var = "ldiff_se",
       # covs = cov_list,
       study_id = "id",
       method = "trim_maxL",
       trim_pct = 0.3,
       overwrite_previous = T
     )
     readr::write_rds(hep_model, FILE_PATH)
}

get_preds <- function(model, covs){
  preds <- unique(mrbrt_dt[, c(covs), with = F])
  predicts <- predict_mr_brt(model, newdata = preds, write_draws = T)
  return(predicts)
}

hep_predicts <- get_preds(hep_model, mrbrt_vars)

summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

# PREDICTION --------------------------------------------------------------
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
                  cases = "", sample_size = "", uncertainty_type_value = "", effective_sample_size = "",
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
                      note_modeler = paste0(note_modeler, " | uncertainty from network analysis added"))]
  extra_cols <- setdiff(names(adjustse_dt), names(noadjust_dt))
  adjustse_dt[, c(extra_cols) := NULL]

  final_dt <- rbind(noadjust_dt, adjustse_dt, full_dt, fill = T, use.names = T)
  return(list(epidb = final_dt, vetting_dt = adjust_dt))
}

adjust_dt <- copy(data)
adjust_preds <- get_preds_adjustment(raw_data = adjust_dt, model = hep_model)
adjust_dt <- get_cases_sample_size(adjust_dt)
adjust_dt <- get_se(adjust_dt)
adjusted <- make_adjustment(data_dt = adjust_dt, ratio_dt = adjust_preds, network = F)