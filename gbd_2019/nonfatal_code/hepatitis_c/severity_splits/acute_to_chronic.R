##########################################
# Purpose: Conduct MR BRT Analysis to assess HCV prevalence ratio for HCV-RNA and anti-HCV
##########################################

rm(list=ls())

pacman::p_load(data.table, openxlsx, ggplot2, plyr)
library(msm)
library(Hmisc)
library(mortdb)


# SET OBJECTS -------------------------------------------------------------

mrbrt_dir <- FILEPATH
draws <- paste0("draw_", 0:999)

# Objects for get model results 
ages <- c(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)
sexes <- c(1, 2)

# READ IN DATA -----------------------------------------------------------
hep_dt <- as.data.table(read.xlsx(FILEPATH))


# SOURCE FUNCTIONS --------------------------------------------------------

functions_dir <- FILEPATH
mrbrt_helper_dir <- FILEPATH
source(paste0(functions_dir, "get_location_metadata.R"))
source(paste0(functions_dir, "get_age_metadata.R"))

source(paste0(functions_dir, "get_population.R"))
source(paste0(functions_dir, "interpolate.R"))

functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
            "load_mr_brt_preds_function", "plot_mr_brt_function")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}

# GET METADATA ------------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 22)
super_region_dt <- loc_dt[, .(location_id, super_region_id, super_region_name)]
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
  #dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
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


## GET DEFINITIONS (ALL BASED ON CV'S - WHERE ALL 0'S IS REFERENCE)
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

## HELPER FUNCTION TO ROUND TO NEAREST AGE START/AGE END IN GBD-LAND
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
get_matches <- function(n = 1, pair_dt, year_span = 10, age_span = 5){
  dt <- copy(pair_dt)
  pair <- pairs[,n]
  print(pair)
  keep_vars <- c("nid", "location_id", "sex", "year_start", "year_end", "measure", "age_start", "age_end",
                 "mean", "standard_error", "cases", "sample_size", "loc_match", "year_match", "age_n")
  dt1 <- copy(dt[definition == pair[1]])
  dt1 <- dplyr::select(dt1, keep_vars)
  dt2 <- copy(dt[definition == pair[2]])
  dt2 <- dplyr::select(dt2, keep_vars)
  setnames(dt2, keep_vars[!keep_vars  == "loc_match" & !keep_vars == "sample_size" & !keep_vars == "nid"], 
           paste0(keep_vars[!keep_vars  == "loc_match" & !keep_vars == "sample_size" & !keep_vars == "nid"], "2"))
  matched <- merge(dt1, dt2, by = c("loc_match", "sample_size", "nid"), allow.cartesian = T) ## INITIAL CARTISIAN MERGE ONLY MATCHING ON LOCATION
  matched[, `:=` (def = pair[1], def2 = pair[2])] ## LABEL WITH DEFINITIONS
  return(matched)
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
  dt[, age := (age_start + age_end + age_start2 + age_end2) / 4]
  dt[, id := paste0(nid, " (", location_ascii_name, ": ", sex, " ", age_start, "-", age_end, ")")]
  if (age == F){
    dt <- dt[, c("id", "ldiff", "ldiff_se", "def", "def2", "super_region_id", "super_region_name", vars), with = F]
  } else if (age == T){
    dt <- dt[, c("id", "age", "ldiff", "ldiff_se", "def", "def2", vars), with = F]
  }
  return(list(dt, vars))
}


# RUN DATA SETUP ----------------------------------------------------------

message("Formatting data")
hep_dt <- get_cases_sample_size(hep_dt)
hep_dt <- get_se(hep_dt)
hep_dt <- calculate_cases_fromse(hep_dt)
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
nrow(diff[(grepl("Inf", logit_mean))])
count((diff[(grepl("Inf", logit_mean))]), "age_start")
diff <- diff[!(grepl("Inf", logit_mean))]
diff <- diff[ldiff >= 0]
diff_sr <- merge(diff, super_region_dt, by = "location_id")
mrbrt_setup <- create_mrbrtdt(diff_sr)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]
 
# RUN MR BRT AS REGRESSION  ----------------------------------------

model_name <- MODEL_NAME



cov_list <- lapply(covs, create_covs)

if (file.exists(FILEPATH)){
  dual_model <- readr::read_rds(FILEPATH)
} else {
  dual_model <- run_mr_brt(
  output_dir = mrbrt_dir,
  model_label = model_name,
  remove_x_intercept = F,
  data = mrbrt_dt,
  mean_var = "ldiff",
  se_var = "ldiff_se",
  # covs = cov_list,
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.1,
  overwrite_previous = F
  )
  readr::write_rds(dual_model, FILEPATH)
}

get_preds <- function(model, covs){
  preds <- unique(mrbrt_dt[, c(covs), with = F])
  predicts <- predict_mr_brt(model, newdata = preds, write_draws = T)
  return(predicts)
}

dual_predicts <- get_preds(dual_model, mrbrt_vars)

summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum <- as.data.table(sum)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

preds <- summaries(dual_predicts$model_draws, draws)


# PREDICTION --------------------------------------------------------------

get_preds_adjustment <- function(raw_data, model){
  dt <- copy(raw_data)
  dt <- unique(dplyr::select(dt, draws))
  preds <- predict_mr_brt(model, newdata = dt, write_draws = T)
  pred_dt <- as.data.table(preds$model_draws)
  pred_dt[, ladj := rowMeans(.SD), .SDcols = draws]
  pred_dt[, ladj_se := apply(.SD, 1, sd), .SDcols = draws]
  pred_dt[, "Z_intercept" := NULL]
  setnames(pred_dt, names(pred_dt), gsub("^X", "cv", names(pred_dt)))
  return(pred_dt)
}

l_draws <- paste0("l_draw_", 0:999)


adjust_dismod_draws <- function(dismod_draws){
  dt <- copy(dismod_draws)
  dt <- copy(dt[, (keep_cols), with = FALSE])
  dt[, (l_draws) := lapply(.SD, function(x) qlogis(x)), .SDcols = draws_test, by = keep_cols]
  dt[, (draws_test) := NULL]
}

make_adjustment <- function(data_dt, ratio_dt){
  cvs <- names(ratio_dt)[grepl("^cv", names(ratio_dt))]
  dt <- merge(data_dt, ratio_dt, by = cvs, all.x = T)
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

# PREDICTION 
message("adjusting data")
adjust_dt <- interpolate(gbd_id_type='modelable_entity_id', 
                         gbd_id=18672,
                         source = "epi",
                         location_id = LOCATION, 
                         age_group_id = ages,
                         measure_id = 5, 
                         status = "latest",
                         reporting_year_start = 1990, 
                         reporting_year_end = 2019,
                         gbd_round_id = 6, 
                         decomp_step=decomp_step)
setnames(adjust_dt)                               
adjust_preds <- get_preds_adjustment(raw_data = adjust_dt, model = dual_model)
adjust_dt <- get_cases_sample_size(adjust_dt)
adjust_dt <- get_se(adjust_dt)
adjusted <- make_adjustment(data_dt = adjust_dt, ratio_dt = adjust_preds)

