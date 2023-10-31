##########################################################################
### Author: USER
### Date: 07/18/2020
### Project: GBD Nonfatal Estimation
### Purpose: Data prep 2020
##########################################################################
rm(list=ls())

library(pacman)
pacman::p_load(data.table, openxlsx, ggplot2, Hmisc, msm, magrittr, reticulate, plyr) 

library(crosswalk, lib.loc = "FILEPATH")
library(metafor, lib.loc = "FILEPATH")
library(mortdb, lib = "FILEPATH")

## Standard IHME Functions
functions_dir <- "FILEPATH"
functs <- c("get_location_metadata", "get_population","get_age_metadata", 
            "get_ids", "get_outputs","get_draws", "get_cod_data",
            "get_bundle_data", "upload_bundle_data", "get_bundle_version", 
            "save_bundle_version", "get_crosswalk_version", "save_crosswalk_version")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))

## initialize vars
date <- gsub("-", "_", Sys.Date())
draws <- paste0("draw_", 0:999)
user <- "USERNAME"

## Standard crosswalking functions: ----
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  dt[is.na(lower) & measure == "prevalence", lower := mean - (1.96 * standard_error)]
  dt[is.na(upper) & measure == "prevalence", upper := mean + (1.96 * standard_error)]
  return(dt)
}
## GET DEFINITIONS (ALL BASED ON COVARIATES - WHERE ALL 0'S IS REFERENCE)
get_definitions <- function(ref_dt, cv_drop){
  dt <- copy(ref_dt)
  cvs <- names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop]
  cvs <- cvs[!cvs %in% cv_drop] 
  dt[, definition := ""]
  for (cv in cvs){
    dt[get(cv) == 1, definition := paste0(cv)] 
  }
  dt[definition == "", definition := "reference"]
  return(list(dt, cvs))
}
find_sex_match <- function(dt, cv_drop){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("prevalence", "incidence")]
  match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end",
                  names(sex_dt)[grepl("^cv_", names(sex_dt)) & !names(sex_dt) %in% cv_drop])
  sex_dt[, match_n := .N, by = match_vars]
  sex_dt <- sex_dt[match_n == 2]
  keep_vars <- c(match_vars, "sex", "mean", "standard_error")
  sex_dt[, id := .GRP, by = match_vars]
  sex_dt <- dplyr::select(sex_dt, keep_vars)
  sex_dt <- data.table::dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"), fun.aggregate = mean)
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0]
  sex_dt[, id := .GRP, by = c("nid", "location_id")]
  return(sex_dt)
}
calc_sex_ratios <- function(dt){
  ratio_dt <- copy(dt)
  ratio_dt[, `:=` (ratio = mean_Female/mean_Male,
                   ratio_se = sqrt((mean_Female^2 / mean_Male^2) * (standard_error_Female^2/mean_Female^2 + standard_error_Male^2/mean_Male^2)))]
  ratio_dt[, log_ratio := log(ratio)]
  ratio_dt$log_se <- sapply(1:nrow(ratio_dt), function(i) {
    mean_i <- ratio_dt[i, "ratio"]
    se_i <- ratio_dt[i, "ratio_se"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  return(ratio_dt)
}
age_sex_split <- function(raw_dt){
  df_split <- copy(raw_dt)
  cvs <- names(df_split)[grepl("^cv", names(df_split))]
  df_split[, split := length(specificity[specificity == "age,sex"])]
  df_split <- df_split[specificity %in% c("age", "sex") & split == 0,]
  
  ## CALCULATE CASES MALE/FEMALE
  df_split[, cases_total:= sum(cases), by = c("nid", "group", "specificity", "measure", "location_id", cvs)]
  df_split[, prop_cases := cases / cases_total]
  
  ## CALCULATE PROP MALE/FEMALE
  df_split[, ss_total:= sum(sample_size), by = c("nid", "group", "specificity", "measure", "location_id", cvs)] 
  df_split[, prop_ss := sample_size / ss_total]
  
  ## CALCULATE STANDARD ERROR
  df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  df_split[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  ## RATIO
  df_split[, ratio := prop_cases / prop_ss]
  df_split[, se_ratio:= sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) )]
  df_ratio <- df_split[specificity == "sex", c("nid", "group", "sex", "location_id", "measure", "ratio", "se_ratio", "prop_cases", "prop_ss", "year_start", "year_end", cvs), with = F]
  
  ## CREATE NEW OBSERVATIONS
  age.sex <- copy(df_split[specificity == "age"])
  age.sex[,specificity := "age,sex"]
  age.sex[,c("ratio", "se_ratio", "split",  "cases_total",  "prop_cases",  "ss_total",	"prop_ss",	"se_cases",	"se_ss") := NULL]
  male <- copy(age.sex[, sex := "Male"])
  female <- copy(age.sex[, sex := "Female"])
  
  age.sex <- rbind(male, female)
  age.sex <- merge(age.sex, df_ratio, by = c("nid", "group", "sex", "measure", "location_id", "year_start", "year_end", cvs))
  
  ## CALCULATE MEANS
  age.sex[, standard_error := sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2)]
  age.sex[, mean_orig:= mean] 
  age.sex[, mean := mean * ratio]
  age.sex[, cases := cases * prop_cases]
  age.sex[, sample_size := sample_size * prop_ss]
  age.sex[,note_modeler := paste(note_modeler, " | age,sex split using sex ratio", round(ratio, digits = 2))]
  age.sex[mean > 1, `:=` (group_review = 0, exclude_xwalk = 1, note_modeler = paste0(note_modeler, " | group reviewed out because age-sex split over 1"))]
  age.sex[,c("ratio", "se_ratio", "prop_cases", "prop_ss") := NULL]
  age.sex[, c("lower", "upper", "uncertainty_type_value","effective_sample_size") := ""]
  
  ## CREATE UNIQUE GROUP TO GET PARENTS
  age.sex.m <- age.sex[,c("nid","group", "measure", "location_id", "year_start", "year_end", cvs), with=F]
  age.sex.m <- unique(age.sex.m, by=c("nid","group", "measure", "location_id", "year_start", "year_end", cvs))
  
  ## GET PARENTS
  parent <- merge(age.sex.m, raw_dt, by= c("nid","group", "measure", "location_id", "year_start", "year_end", cvs))
  parent[specificity == "age" | specificity == "sex", group_review:=0]
  parent <- parent[specificity %in% c("age", "sex")]
  parent[, note_modeler := paste0(note_modeler, " | parent data, has been age-sex split")]
  
  ## FINAL DATA
  original <- raw_dt[!seq %in% parent$seq]
  total <- rbind(original, age.sex, fill = T)
  
  ## MESSAGE AND RETURN
  if (nrow(parent) == 0) print(paste0("nothing to age-sex split"))
  else print(paste0("split ", length(parent[, unique(nid)]), " nids"))
  return(total)
}
split_data <- function(dt, model){
  tosplit_dt <- copy(dt)
  nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female") | (sex == "Both" & group_review == 0)]
  tosplit_dt <- tosplit_dt[sex == "Both" & (group_review == 1 | is.na(group_review))]
  tosplit_dt[, midyear := floor((year_start + year_end)/2)]
  preds <- predict_mr_brt(model, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T)
  pred_draws <- as.data.table(preds$model_draws)
  pred_draws[, c("X_intercept", "Z_intercept") := NULL]
  pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
  ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
  ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)
  pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                           year_ids = tosplit_dt[, unique(midyear)], location_ids = tosplit_dt[, unique(location_id)])
  pops[age_group_years_end == 125, age_group_years_end := 99]
  tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt, population = pops), mc.cores = 9))
  tosplit_dt <- tosplit_dt[!is.na(both_N)]
  tosplit_dt[, merge := 1]
  pred_draws[, merge := 1]
  split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
  split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
  split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
  split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
  male_dt <- copy(split_dt)
  male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = "", lower = "",
                  cases = "", sample_size = "", uncertainty_type_value = "", sex = "Male",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                        ratio_se, ")"))]
  if ("seq" %in% names(male_dt)) {
    male_dt[, crosswalk_parent_seq := seq]
    male_dt[, seq := NA]
  }
  male_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  male_dt <- dplyr::select(male_dt, names(dt))
  male_dt[, midyear := NULL]
  female_dt <- copy(split_dt)
  female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = "", lower = "",
                    cases = "", sample_size = "", uncertainty_type_value = "", sex = "Female",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                          ratio_se, ")"))]
  if ("seq" %in% names(female_dt)) {
    female_dt[, crosswalk_parent_seq := seq]
    female_dt[, seq := NA]
  }
  female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  female_dt <- dplyr::select(female_dt, names(dt))
  female_dt[, midyear := NULL]
  total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))
  return(list(final = total_dt, graph = split_dt))
}
get_row <- function(n, dt, population){
  row <- copy(dt)[n]
  pops <- population
  pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                          age_group_years_start >= row[, age_start] & age_group_years_end <= row[, age_end]])
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex")]
  row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
              both_N = agg[sex == "Both", pop_sum])]
  return(row)
}
graph_predictions <- function(dt){
  graph_dt <- copy(dt[measure == "prevalence", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
  graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
  graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"), allow.cartesian = T)
  graph_dt[, N := (mean*(1-mean)/error^2)]
  graph_dt[is.nan(N), N := 0]
  wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
  graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  graph_dt[, midage := (age_end + age_start)/2]
  ages <- c(60, 70, 80, 90)
  graph_dt[, age_group := cut2(midage, ages)]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper)) +
    labs(x = "Both Sex Mean", y = " Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Sex Split Means Compared to Both Sex Mean by Age") +
    scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "purple")) +
    theme_classic()
  return(gg_sex)
}
aggregate_marketscan <- function(mark_dt){ 
  dt <- copy(mark_dt)
  marketscan_dt <- copy(dt[cv_marketscan == 1 | cv_marketscan_all_2000 == 1])
  unagg_marketscan_dt <- copy(marketscan_dt) 
  non_marketscan_dt <- copy(dt[!cv_marketscan == 1 & !cv_marketscan_all_2000 == 1])
  by_vars <- c("age_start", "age_end", "year_start", "year_end", "sex")
  marketscan_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = by_vars]
  marketscan_dt[, `:=` (location_id = 102, location_name = "United States of America", mean = cases/sample_size,
                        lower = NA, upper = NA)]
  z <- qnorm(0.975)
  marketscan_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  marketscan_dt <- unique(marketscan_dt, by = by_vars)
  full_dt <- rbind(non_marketscan_dt, marketscan_dt, unagg_marketscan_dt) 
  return(full_dt)
}
paste2 <- function(...,sep=", ") {
  L <- list(...)
  L <- lapply(L,function(x) {x[is.na(x)] <- ""; x})
  gsub(paste0("(^",sep,"|",sep,"$)"),"",
       gsub(paste0(sep,sep),sep,
            do.call(paste,c(L,list(sep=sep)))))
}
make_adjustment <- function(model, full_dt){
  reference_def<- "reference"
  adjust_dt <- copy(full_dt[mean != 0, ])
  adjustse_dt <- copy(full_dt[mean == 0 & case_def != reference_def, ])
  noadjust_dt <- copy(full_dt[mean == 0 & case_def == reference_def])
  
  # Get predicted mean and standard error 
  preds <- model$fixed_vars
  preds <- as.data.frame(preds)
  preds <- rbind(preds, model$beta_sd)
  cvs <- unique(full_dt$case_def)
  cvs <- cvs[cvs != reference_def]
  
  # Adjust data points based on mrbrt 
  adjust_dt[, c("mean_adj", "standard_error_adj", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
    fit_object = model,       # result of CWModel()
    df = adjust_dt,            # original data with observations to be adjusted
    orig_dorms = "case_def", # name of column with (all) definition/method levels
    orig_vals_mean = "mean",  # original mean
    orig_vals_se = "standard_error"  # standard error of original mean
  )
  adjust_dt$crosswalk_parent_seq <- adjust_dt$seq
  graph_adjust <- copy(adjust_dt)
  
  for (cv in cvs) {
    ladj <- preds[cv][[1]][1]
    ladj_se <- preds[cv][[1]][2]
    adjust_dt[case_def == cv & diff != 0, `:=` (mean = mean_adj, standard_error = standard_error_adj, 
                                                note_modeler = paste0(note_modeler, " | crosswalked with logit(difference): ", 
                                                                      round(ladj, 2), " (", round(ladj_se, 2), ")"), 
                                                cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA, effective_sample_size = NA)] 
    adjust_dt[is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq, seq = NA)]
  }
  
  ## ADJUST STANDARD ERRORS FOR ZERO MEANS
  for (cv in cvs) {
    ladj <- preds[cv][[1]][1]
    ladj_se <- preds[cv][[1]][2]
    adjustse_dt[case_def == cv , `:=` (ladj = ladj, ladj_se = ladj_se)]
    adjustse_dt$adj_se <- sapply(1:nrow(adjustse_dt), function(i){
      mean_i <- as.numeric(adjustse_dt[i, "ladj"])
      se_i <- as.numeric(adjustse_dt[i, "ladj_se"])
      deltamethod(~exp(x1)/(1+exp(x1)), mean_i, se_i^2)
    })
    adjustse_dt[case_def== cv, `:=` (standard_error = sqrt(standard_error^2 + adj_se^2), 
                                     note_modeler = paste0(note_modeler, " | uncertainty from network analysis added"), 
                                     cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA, effective_sample_size = NA)]
    adjustse_dt[is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq, seq = NA)]
  } 
  
  # Combine everything, make sure there are no drops, drop extra columns 
  dt_all <- rbind(noadjust_dt, adjustse_dt, adjust_dt, fill = T, use.names = T)
  if (nrow(full_dt) == nrow(dt_all)) {
    message("All NIDs present")
  } else {
    message("Dropped NIDs. Stop!")
  }
  
  extra_cols <- setdiff(names(dt_all), names(full_dt))
  final_dt <- copy(dt_all)
  final_dt[, (extra_cols) := NULL]
  return(list(epidb = final_dt, vetting_dt = graph_adjust))
}
#2019 Functions:
subnat_to_nat <- function(subnat_dt){
  dt <- copy(subnat_dt)
  dt[, loc_match := location_id]
  dt[level >= 4, loc_match := parent_id]
  dt[level == 6, loc_match := 4749] ## PAIR UTLAS TO ENGLAND
  dt[level == 5 & grepl("GBR", ihme_loc_id), loc_match := 4749] ## PAIR ENGLAND REGIONS TO ENGLAND
  dt[level == 5 & location_type %in% c("urbanicity", "admin2"), loc_match := 163] ## PAIR INDIA URBAN/RURAL TO INDIA
  dt[level == 5 & grepl("CHN", ihme_loc_id), loc_match := 6]
  dt[level == 5 & grepl("KEN", ihme_loc_id), loc_match := 180]
  return(dt)
}
calc_year <- function(year_dt){
  dt <- copy(year_dt)
  dt[, year_match := (year_start+year_end)/2]
  return(dt)
}
get_age_combos <- function(agematch_dt){
  dt <- copy(agematch_dt)
  by_vars <- c("nid", "location_id","year_match", "sex", "measure", names(dem_dt)[grepl("^cv_", names(dem_dt)) & !names(dem_dt) %in% cv_drop])
  dt[, age_n := .GRP, by = by_vars]
  small_dt <- copy(dt[, c(by_vars, "age_start", "age_end","age_n"), with = F])
  return(list(dt, small_dt))
} ## GET UNIQUE "AGE SERIES" FOR THE PURPOSE OF AGGREGATING
get_matches_recall_lifetime <- function(n, pair_dt, year_span = 5, age_span = 5){ 
  dt <- copy(pair_dt)
  pair <- pairs[,n]
  print(pair)
  keep_vars <- c("nid", "location_id", "sex", "year_start", "year_end", "measure", "age_start", "age_end",
                 "mean", "standard_error", "cases", "sample_size", "loc_match", "year_match", "age_n", "location_name", "region_name", "super_region_name")
  dt1 <- copy(dt[definition == pair[1]])
  dt1 <- dplyr::select(dt1, keep_vars)
  dt2 <- copy(dt[definition == pair[2]])
  dt2 <- dplyr::select(dt2, keep_vars)
  setnames(dt2, keep_vars[!keep_vars  == "loc_match"], paste0(keep_vars[!keep_vars  == "loc_match"], "2"))
  matched <- merge(dt1, dt2, by = "loc_match", allow.cartesian = T) 
  matched <- matched[sex == sex2 & measure == measure2 &
                       data.table::between(year_match, year_match2 - year_span/2, year_match2 + year_span/2)] ## FILTER OUT SEX, MEASURE, YEAR
  matched[, age_match := (data.table::between(age_start, age_start2 - age_span/2, age_start2 + age_span/2) & data.table::between(age_end, age_end2 - age_span/2, age_end2 + age_span/2))]
  unmatched <- copy(matched[age_match == F])
  ## AGE ROUNDING
  unmatched$age_start <- closest_match(unmatched, age_dt, "age_start", "age_group_years_start")[,age_group_years_start]
  unmatched$age_end <- closest_match(unmatched, age_dt, "age_end", "age_group_years_end")[,age_group_years_end]
  unmatched$age_start2 <- closest_match(unmatched, age_dt, "age_start2", "age_group_years_start")[,age_group_years_start]
  unmatched$age_end2 <- closest_match(unmatched, age_dt, "age_end2", "age_group_years_end")[,age_group_years_end]
  
  ## FIND WHERE IT IS POSSIBLE TO AGGREGATE
  unmatched[, age_start_match := age_start == age_start2]
  unmatched[, age_end_match := age_end == age_end2]
  unmatched[, sum_start := sum(age_start_match), by = c("age_n", "age_n2")]
  unmatched[, sum_end := sum(age_end_match), by = c("age_n", "age_n2")]
  agg_matches <- copy(unmatched[sum_start > 0 & sum_end > 0 & !is.na(cases) & !is.na(sample_size) & !is.na(cases2) & !is.na(sample_size2)])
  agg_matches[, agg_id := .GRP, by = c("age_n", "age_n2")]
  
  ## AGGREGATE IF THERE ARE PLACES TO AGGREGATE OTHERWISE DO NOTHING
  if (nrow(agg_matches) > 0){
    print(paste("Starting aggregating to match", Sys.time()))
    aggregated <- rbindlist(lapply(c(1:4, 6:7, 9:14, 18, 20, 21, 24, 26:31, 33:34, 36:46, 49, 52, 53:max(agg_matches$agg_id)), function(x) aggregate_tomatch(match_dt = agg_matches, id = x)))
    aggregated[, c("age_start_match", "age_end_match", "sum_start", "sum_end", "agg_id") := NULL]
    final_match <- rbind(matched[age_match == T], aggregated)
  } else {
    final_match <- copy(matched[age_match == T])
  }
  final_match[, c("age_match") := NULL]
  final_match[, `:=` (def = pair[1], def2 = pair[2])] ## LABEL WITH DEFINITIONS
  return(final_match)
}
get_matches_marketscan_2000 <- function(n, pair_dt, year_span = 20, age_span = 5){ 
  dt <- copy(pair_dt)
  pair <- pairs[,n]
  print(pair)
  keep_vars <- c("nid", "location_id", "sex", "year_start", "year_end", "measure", "age_start", "age_end",
                 "mean", "standard_error", "cases", "sample_size", "loc_match", "year_match", "age_n", "location_name", "region_name", "super_region_name")
  dt1 <- copy(dt[definition == pair[1]])
  dt1 <- dplyr::select(dt1, keep_vars)
  dt2 <- copy(dt[definition == pair[2]])
  dt2 <- dplyr::select(dt2, keep_vars)
  setnames(dt2, keep_vars[!keep_vars  == "loc_match"], paste0(keep_vars[!keep_vars  == "loc_match"], "2"))
  matched <- merge(dt1, dt2, by = "loc_match", allow.cartesian = T) 
  matched <- matched[sex == sex2 & measure == measure2 & data.table::between(year_match, year_match2 - year_span/2, year_match2 + year_span/2)] ## FILTER OUT SEX, MEASURE, YEAR
  matched[, age_match := (data.table::between(age_start, age_start2 - age_span/2, age_start2 + age_span/2) & data.table::between(age_end, age_end2 - age_span/2, age_end2 + age_span/2))]
  unmatched <- copy(matched[age_match == F])
  ## AGE ROUNDING
  unmatched$age_start <- closest_match(unmatched, age_dt, "age_start", "age_group_years_start")[,age_group_years_start]
  unmatched$age_end <- closest_match(unmatched, age_dt, "age_end", "age_group_years_end")[,age_group_years_end]
  unmatched$age_start2 <- closest_match(unmatched, age_dt, "age_start2", "age_group_years_start")[,age_group_years_start]
  unmatched$age_end2 <- closest_match(unmatched, age_dt, "age_end2", "age_group_years_end")[,age_group_years_end]
  
  ## FIND WHERE IT IS POSSIBLE TO AGGREGATE
  unmatched[, age_start_match := age_start == age_start2]
  unmatched[, age_end_match := age_end == age_end2]
  unmatched[, sum_start := sum(age_start_match), by = c("age_n", "age_n2")]
  unmatched[, sum_end := sum(age_end_match), by = c("age_n", "age_n2")]
  agg_matches <- copy(unmatched[sum_start > 0 & sum_end > 0 & !is.na(cases) & !is.na(sample_size) & !is.na(cases2) & !is.na(sample_size2)])
  agg_matches[, agg_id := .GRP, by = c("age_n", "age_n2")]
  
  ## AGGREGATE IF THERE ARE PLACES TO AGGREGATE OTHERWISE DO NOTHING
  if (nrow(agg_matches) > 0){
    print(paste("Starting aggregating to match", Sys.time()))
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
get_matches_marketscan <- function(n, pair_dt, year_span = 5, age_span = 5){ 
  dt <- copy(pair_dt)
  pair <- pairs[,n]
  print(pair)
  keep_vars <- c("nid", "location_id", "sex", "year_start", "year_end", "measure", "age_start", "age_end",
                 "mean", "standard_error", "cases", "sample_size", "loc_match", "year_match", "age_n", "location_name", "region_name", "super_region_name")
  dt1 <- copy(dt[definition == pair[1]])
  dt1 <- dplyr::select(dt1, keep_vars)
  dt2 <- copy(dt[definition == pair[2]])
  dt2 <- dplyr::select(dt2, keep_vars)
  setnames(dt2, keep_vars[!keep_vars  == "loc_match"], paste0(keep_vars[!keep_vars  == "loc_match"], "2"))
  matched <- merge(dt1, dt2, by = "loc_match", allow.cartesian = T) 
  matched <- matched[sex == sex2 & measure == measure2 &
                       data.table::between(year_match, year_match2 - year_span/2, year_match2 + year_span/2)] ## FILTER OUT SEX, MEASURE, YEAR
  matched[, age_match := (data.table::between(age_start, age_start2 - age_span/2, age_start2 + age_span/2) & data.table::between(age_end, age_end2 - age_span/2, age_end2 + age_span/2))]
  unmatched <- copy(matched[age_match == F])
  ## AGE ROUNDING
  ## AGE ROUNDING
  unmatched$age_start <- closest_match(unmatched, age_dt, "age_start", "age_group_years_start")[,age_group_years_start]
  unmatched$age_end <- closest_match(unmatched, age_dt, "age_end", "age_group_years_end")[,age_group_years_end]
  unmatched$age_start2 <- closest_match(unmatched, age_dt, "age_start2", "age_group_years_start")[,age_group_years_start]
  unmatched$age_end2 <- closest_match(unmatched, age_dt, "age_end2", "age_group_years_end")[,age_group_years_end]
  
  ## FIND WHERE IT IS POSSIBLE TO AGGREGATE
  unmatched[, age_start_match := age_start == age_start2]
  unmatched[, age_end_match := age_end == age_end2]
  unmatched[, sum_start := sum(age_start_match), by = c("age_n", "age_n2")]
  unmatched[, sum_end := sum(age_end_match), by = c("age_n", "age_n2")]
  agg_matches <- copy(unmatched[sum_start > 0 & sum_end > 0 & !is.na(cases) & !is.na(sample_size) & !is.na(cases2) & !is.na(sample_size2)])
  agg_matches[, agg_id := .GRP, by = c("age_n", "age_n2")]
  
  ## AGGREGATE IF THERE ARE PLACES TO AGGREGATE OTHERWISE DO NOTHING
  if (nrow(agg_matches) > 0){
    print(paste("Starting aggregating to match", Sys.time()))
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
create_ratios <- function(ratio_dt){
  dt <- copy(ratio_dt)
  dt[, ratio := mean2/mean]
  dt[, ratio_se := sqrt((mean2^2 / mean^2) * (standard_error2^2/mean2^2 + standard_error^2/mean^2))]
  dt[, log_ratio := log(ratio)]
  dt$log_rse <- sapply(1:nrow(dt), function(i) {
    mean_i <- dt[i, "ratio"]
    se_i <- dt[i, "ratio_se"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  return(dt)
}
aggregate_tomatch <- function(match_dt, id){
  print(id)
  pull_dt <- copy(match_dt[agg_id == id])
  start_matches <- pull_dt[age_start_match == T, age_start] ## GET ALL START MATCHES
  allend_matches <- pull_dt[age_end_match == T, unique(age_end)] ## GET ALL END MATCHES
  end_matches <- c() ## MAKE SURE ACTUAL END MATCHES MAP 1:1 WITH START MATCHES
  for (x in 1:length(start_matches)){
    match <- allend_matches[allend_matches > start_matches[x]]
    match <- match[which.min(match-start_matches[x])]
    end_matches <- c(end_matches, match)
  }
  aggregated <- rbindlist(lapply(1:length(start_matches), function(x) aggregation(start_match = start_matches[x],  end_match = end_matches[x], dt = pull_dt)))
  return(aggregated)
}## SET UP AGGREGATION
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
  new_row <- cbind(row_dt, row_dt2) 
  return(new_row)
}## AGGREGATE
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
  dt[, id2 := paste0(nid2, ":", nid)]
  dt <- dt[, c("id", "id2", "logit_diff", "logit_diff_se", "def", "def2", vars), with = F]
  return(list(dt, vars))
}

#USER functions:
closest_match <- function(dt1, dt2, var1, var2){
  #dt1 and dt2 must be data.tables
  #var1 and var2 must be column names from dt1 and dt2 respectively
  if(is.data.table(dt1) == F){
    stop("dt1 is not a data.table")
  }
  if(is.data.table(dt2) == F){
    stop("dt2 is not a data.table")
  }
  if(!var1 %in% colnames(dt1)){
    stop("var1 is not a variable in dt1")
  }
  if(!var2 %in% colnames(dt2)){
    stop("var2 is not a variable in dt2")
  }
  dtOne <- copy(dt1)
  dtTwo <- copy(dt2)
  dtOne[, merge := get(var1)]
  dtTwo[, merge := get(var2)]
  
  merged <- dtTwo[dtOne, roll = "nearest", on = "merge"]
  merged
}

# objects 
loc_dt <- as.data.table(get_location_metadata(location_set_id = 35, gbd_round_id = 7, decomp_step = "iterative"))

age_dt <- get_age_metadata(19)
age_dt[age_group_id == 235, age_group_years_end := 99]


# directories 
plot_dir <- paste0("FILEPATH", date)
flat_file_dir <- paste0("FILEPATH", date)

dir.create(plot_dir)
dir.create(flat_file_dir)

## Reading in data:  
bundle_277_version <- as.data.table(read.xlsx("FILEPATH"))
dt <- copy(bundle_277_version)
#Correcting age_end values:
dt[age_end > 99, age_end := 99]
dt <- dt[group_review == 1 | is.na(group_review),]
dt_not_inc_prev <- dt[!(measure == "prevalence" | measure == "incidence")]
dt <- dt[measure == "prevalence" | measure == "incidence"]

dt[nid == 125385, is_outlier := 1]
dt[nid == 125382 & (age_end == 44 | age_end == 59), is_outlier := 1]

#Outlier Poland National health fund
dt[nid == 397812 | nid == 397813 | nid == 397814, is_outlier := 1]

#Correcting crosswalk values:  
dt[field_citation_value %like% "Truven Health" & year_start == 2000, cv_marketscan_all_2000 := 1]
dt[field_citation_value %like% "Truven Health" & year_start != 2000, cv_marketscan_all_2000 := 0]
dt[field_citation_value %like% "Truven Health" & year_start != 2000, cv_marketscan := 1]

dt[is.na(cv_recall_lifetime), cv_recall_lifetime := 0] 
dt[is.na(cv_marketscan), cv_marketscan := 0]
dt[is.na(cv_marketscan_all_2000), cv_marketscan_all_2000 := 0]

#initializing crosswalk parentsequences
dt[, "crosswalk_parent_seq" := as.numeric(NA)]

# Correcting variable classes 
dt[ , standard_error := as.numeric(standard_error)] 
dt[ , lower := as.numeric(lower)]
dt[ , upper := as.numeric(upper)] 

# # append location data  #Location data already appended
dt <- merge(dt, loc_dt, by = "location_id")
dt[location_name.x == location_name.y, location_name := location_name.x]
dt$country <- substr(dt$ihme_loc_id, 0, 3)


# fill out cases/mean/Sample Size/Standard Error based on uploader formulas 
dt$cases <- as.numeric(dt$cases)
dt$sample_size <- as.numeric(dt$sample_size)
dt <- get_cases_sample_size(dt)
dt <- get_se(dt)

#Sex split: #Using the 2019 sex split: ----
tosplit_dt <- as.data.table(copy(dt))
nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female")] 
tosplit_dt <- tosplit_dt[sex == "Both"]
tosplit_dt[, midyear := floor((year_start + year_end)/2)]

pred_draws <- as.data.table(read.csv("FILEPATH"))

pred_draws[, c("X_intercept", "Z_intercept") := NULL]
pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)

pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                         year_ids = tosplit_dt[, unique(midyear)], location_ids = tosplit_dt[, unique(location_id)])

pops[age_group_years_end == 125, age_group_years_end := 99]

tosplit_dt <- rbindlist(lapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt, population = pops)))
tosplit_dt <- tosplit_dt[!is.na(both_N)]

tosplit_dt[, merge := 1]
pred_draws[, merge := 1]
split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
male_dt <- copy(split_dt)
male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = NA, lower = NA,
                cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male",
                note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                      ratio_se, ")"))]
male_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
male_dt <- dplyr::select(male_dt, names(dt))

female_dt <- copy(split_dt)
female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = NA, lower = NA,
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                        ratio_se, ")"))]

female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
female_dt <- dplyr::select(female_dt, names(dt))

total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))

pdf(paste0(plot_dir, "/sex_split_graph.pdf"))
graph_predictions(split_dt)
dev.off()

# write sex split files
write.csv(total_dt, paste0(flat_file_dir, "/sex_split_data.csv"))
write.csv(dt_not_inc_prev, paste0(flat_file_dir, "/not_inc_prev.csv"))

######################################## RUN DATA SETUP: Recall Lifetime: ----------------------------------------------------------
# Pulling in data and aggregating marketscan data ----
sex_split_dt <- data.table(read.csv(paste0(flat_file_dir, "/sex_split_data.csv")))
dem_dt <- copy(sex_split_dt)
dem_dt <- dem_dt[is_outlier == 0 | is.na(is_outlier), ]

dem_dt <- aggregate_marketscan(dem_dt) 

#Getting references ----

## Dropping Covariates we dont use:
coln <- colnames(dem_dt)
cv_drop <- coln[setdiff(grep("cv_", coln), which(coln %in% c("cv_marketscan_all_2000", "cv_recall_lifetime", "cv_marketscan")))] 

defs <- get_definitions(dem_dt, cv_drop)
dem_dt <- defs[[1]]
cvs <- defs[[2]] 
rm(defs) #Keeping memory usage lower
# Recall lifetime Matches ----
dem_dt[, loc_match := location_id]

dem_dt <- calc_year(dem_dt)
age_dts <- get_age_combos(dem_dt)
dem_dt <- age_dts[[1]]
age_match_dt <- age_dts[[2]] 
pairs <- combn(dem_dt[, unique(definition)], 2) #Combn creates all possible convolutions of its first element (a vector), with length of the second element. i.e. combn(1:3, 2) gives dataframe data.frame(c(1,2), c(1,3), c(2, 3))
matched <- get_matches_recall_lifetime(n = 1, pair_dt = dem_dt, year_span = 5, age_span = 5)

matched <- copy(matched[is.finite(mean) & is.finite(standard_error) & is.finite(mean2) & is.finite(standard_error2), ]) #Making sure all observationsare finite values, no NA, NaN, Inf, etc 
matched <- matched[!mean == 0 & !mean2 == 0, ] 
matched <- matched[!mean == 1 & !mean2 == 1, ] 

#get logit calculates using the delta transform from crosswalk package (transform mean and SE into logit-space)
logit_ref_means <- as.data.table(delta_transform(mean=matched$mean, sd=matched$standard_error, transformation="linear_to_logit"))
setnames(logit_ref_means, c("mean_logit", "sd_logit"), c("logit_ref_mean", "logit_ref_se"))
logit_alt_means <- as.data.table(delta_transform(mean=matched$mean2, sd=matched$standard_error2, transformation="linear_to_logit"))
setnames(logit_alt_means, c("mean_logit", "sd_logit"), c("logit_alt_mean", "logit_alt_se"))

# bind back onto main data table
matched <- cbind(matched, logit_alt_means)
matched <- cbind(matched, logit_ref_means)

# use calculate_diff() to calculate the log difference between matched pairs
matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = matched, 
  alt_mean = "logit_alt_mean", alt_sd = "logit_alt_se",
  ref_mean = "logit_ref_mean", ref_sd = "logit_ref_se" )

#Write files -----
write.csv(matched, paste0(flat_file_dir, "/lifetime_recall_matches.csv"))

#Formatting data for MR-BRT process
mrbrt_setup <- create_mrbrtdt(matched, vars = cvs)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]

#Write files -----
write.csv(mrbrt_dt, paste0(flat_file_dir, "/pre_lifetime_recall_xw_data.csv"))

# Lifetime recall run MR-BRT
df1 <- CWData(
  df = mrbrt_dt,          # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "def2",     # column name of the variable indicating the alternative method
  ref_dorms = "def",     # column name of the variable indicating the reference method
  covs= list(),
  study_id = "id2",    # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

fit1 <- CWModel(
  cwdata = df1,            # object returned by `CWData()`
  obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  inlier_pct= 0.9, 
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept")),
  gold_dorm = "reference"   # the level of `alt_dorms` that indicates it's the gold standard
)


repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = fit1, 
  cwdata = df1,
  continuous_variables = list(),
  obs_method = "cv_recall_lifetime",
  plot_note = "Funnel plot Epilepsy, lifetime recall", 
  plots_dir = plot_dir, 
  file_name = "funnel_plot_epi_lifetime_recall",
  write_file = TRUE
)

# check outputs
fit1$fixed_vars
fit1$gamma

# Lifetime Recall Save results ----
df_result <- fit1$create_result_df()
write.csv(df_result, paste0(flat_file_dir, "/model_results_lifetime_recall.csv"))
py_save_object(object = fit1, filename = paste0(flat_file_dir, "/lifetime_recall_model.pkl"), pickle = "dill")

############################# Setting SE > 1 to 1 ####################
mrbrt_dt <- mrbrt_dt[logit_diff_se> 1, logit_diff_se:= 1]
# Lifetime recall run MR-BRT
df1 <- CWData(
  df = mrbrt_dt,          # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "def2",     # column name of the variable indicating the alternative method
  ref_dorms = "def",     # column name of the variable indicating the reference method
  covs= list(),
  study_id = "id2",    # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

fit1 <- CWModel(
  cwdata = df1,            # object returned by `CWData()`
  obs_type = "diff_logit", 
  inlier_pct= 0.9, 
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept")),
  gold_dorm = "reference"   # the level of `alt_dorms` that indicates it's the gold standard
)


repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = fit1, 
  cwdata = df1,
  continuous_variables = list(),
  obs_method = "cv_recall_lifetime",
  plot_note = "Funnel plot Epilepsy, lifetime recall", 
  plots_dir = plot_dir, 
  file_name = "funnel_plot_epi_lifetime_recall_SE<1",
  write_file = TRUE
)


######################################## RUN DATA SETUP: Marketscan year 2000:  ----------------------------------------------------
# Pulling in data and aggregating marketscan data ----
sex_split_dt <- data.table(read.csv(paste0(flat_file_dir, "/sex_split_data.csv")))
dem_dt <- as.data.table(copy(sex_split_dt))
dem_dt <- dem_dt[!is_outlier == 1, ]

dem_dt <- aggregate_marketscan(dem_dt)
dem_dt[,location_name := location_name.x]
## Getting references ----
coln <- colnames(dem_dt)
cv_drop <- coln[setdiff(grep("cv_", coln), which(coln %in% c("cv_marketscan_all_2000", "cv_recall_lifetime", "cv_marketscan")))] 

defs <- get_definitions(dem_dt, cv_drop)
dem_dt <- defs[[1]]
cvs <- defs[[2]] 
rm(defs) 
# Marketscan 2000 Matches ----
dem_dt[, loc_match := location_id]
dem_dt <- calc_year(dem_dt)
age_dts <- get_age_combos(dem_dt)
dem_dt <- age_dts[[1]]
age_match_dt <- age_dts[[2]] 
pairs <- combn(dem_dt[, unique(definition)], 2) #Combn creates all possible convolutions of its first element (a vector), with length of the second element. i.e. combn(1:3, 2) gives dataframe data.frame(c(1,2), c(1,3), c(2, 3))
matched <- get_matches_marketscan_2000(n = 2, pair_dt = dem_dt, year_span = 20, age_span = 5)

matched <- copy(matched[is.finite(mean) & is.finite(standard_error) & is.finite(mean2) & is.finite(standard_error2), ]) 
matched <- matched[!mean == 0 & !mean2 == 0, ] 
matched <- matched[!mean == 1 & !mean2 == 1, ] 

#get logit calculates using the delta transform from crosswalk package (transform mean and SE into logit-space)
logit_ref_means <- as.data.table(delta_transform(mean=matched$mean, sd=matched$standard_error, transformation="linear_to_logit"))
setnames(logit_ref_means, c("mean_logit", "sd_logit"), c("logit_ref_mean", "logit_ref_se"))
logit_alt_means <- as.data.table(delta_transform(mean=matched$mean2, sd=matched$standard_error2, transformation="linear_to_logit"))
setnames(logit_alt_means, c("mean_logit", "sd_logit"), c("logit_alt_mean", "logit_alt_se"))

# bind back onto main data table
matched <- cbind(matched, logit_alt_means)
matched <- cbind(matched, logit_ref_means)

# use calculate_diff() to calculate the log difference between matched pairs
matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = matched, 
  alt_mean = "logit_alt_mean", alt_sd = "logit_alt_se",
  ref_mean = "logit_ref_mean", ref_sd = "logit_ref_se" )

#Write files -----
write.csv(matched, paste0(flat_file_dir, "/matches_data_marketscan_2000.csv"))

#Formatting data for MR-BRT process
mrbrt_setup <- create_mrbrtdt(matched, vars = cvs)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]

#Write files -----
write.csv(mrbrt_dt, paste0(flat_file_dir, "/pre_xw_data_marketscan_2000.csv"))

# Marketscan 2000 run MR-BRT
df1 <- CWData(
  df = mrbrt_dt,          # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "def2",     # column name of the variable indicating the alternative method
  ref_dorms = "def",     # column name of the variable indicating the reference method
  covs= list(),
  study_id = "id2",    # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

fit1 <- CWModel(
  cwdata = df1,            # object returned by `CWData()`
  obs_type = "diff_logit", 
  inlier_pct= 0.9, 
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept")),
  gold_dorm = "reference"   # the level of `alt_dorms` that indicates it's the gold standard
)


repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = fit1, 
  cwdata = df1,
  continuous_variables = list(),
  obs_method = "cv_marketscan_all_2000",
  plot_note = "Funnel plot Epilepsy, Marketscan 2000", 
  plots_dir = plot_dir, 
  file_name = "funnel_plot_epi_marketscan_2000_test",
  write_file = TRUE
)

# check outputs
fit1$fixed_vars
fit1$gamma

# Marketscan 2000 Save results ----
df_result <- fit1$create_result_df()
write.csv(df_result, paste0(flat_file_dir, "/model_results_marketscan_2000.csv"))
py_save_object(object = fit1, filename = paste0(flat_file_dir, "/model_marketscan_2000.pkl"), pickle = "dill")

####################################### SE > 1 rounded to 1 plot for vision of small observations #############################
mrbrt_dt <- mrbrt_dt[logit_diff_se> 1, logit_diff_se:= 1]

df1 <- CWData(
  df = mrbrt_dt,          # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "def2",     # column name of the variable indicating the alternative method
  ref_dorms = "def",     # column name of the variable indicating the reference method
  covs= list(),
  study_id = "id2",    # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

fit1 <- CWModel(
  cwdata = df1,            # object returned by `CWData()`
  obs_type = "diff_logit", 
  inlier_pct= 0.9, 
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept")),
  gold_dorm = "reference"   # the level of `alt_dorms` that indicates it's the gold standard
) in NMA


repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = fit1, 
  cwdata = df1,
  continuous_variables = list(),
  obs_method = "cv_marketscan_all_2000",
  plot_note = "Funnel plot Epilepsy, Marketscan 2000", 
  plots_dir = plot_dir, 
  file_name = "funnel_plot_epi_marketscan_2000_test_SE<1",
  write_file = TRUE
)

######################################## RUN DATA SETUP: Marketscan all: -----
# Pulling in data and aggregating marketscan data ----
sex_split_dt <- data.table(read.csv(paste0(flat_file_dir, "/sex_split_data.csv")))

dem_dt <- as.data.table(copy(sex_split_dt))
dem_dt <- dem_dt[!is_outlier == 1, ]


dem_dt <- aggregate_marketscan(dem_dt) 
## Getting references ----
coln <- colnames(dem_dt)
cv_drop <- coln[setdiff(grep("cv_", coln), which(coln %in% c("cv_marketscan_all_2000", "cv_recall_lifetime", "cv_marketscan")))]
defs <- get_definitions(dem_dt, cv_drop = cv_drop)
dem_dt <- defs[[1]]
cvs <- defs[[2]] 
rm(defs)

# Marketscan Matches ----
dem_dt[, loc_match := location_id]
dem_dt <- calc_year(dem_dt)
age_dts <- get_age_combos(dem_dt)
dem_dt <- age_dts[[1]]
age_match_dt <- age_dts[[2]] 
pairs <- combn(dem_dt[, unique(definition)], 2) #Combn creates all possible convolutions of its first element (a vector), with length of the second element. i.e. combn(1:3, 2) gives dataframe data.frame(c(1,2), c(1,3), c(2, 3))
matched <- get_matches_marketscan(n = 3, pair_dt = dem_dt, year_span = 15, age_span = 5) 

matched <- copy(matched[is.finite(mean) & is.finite(standard_error) & is.finite(mean2) & is.finite(standard_error2), ])
matched <- matched[!mean == 0 & !mean2 == 0, ] 
matched <- matched[!mean == 1 & !mean2 == 1, ] 

#get logit calculates using the delta transform from crosswalk package (transform mean and SE into logit-space)
logit_ref_means <- as.data.table(delta_transform(mean=matched$mean, sd=matched$standard_error, transformation="linear_to_logit"))
setnames(logit_ref_means, c("mean_logit", "sd_logit"), c("logit_ref_mean", "logit_ref_se"))
logit_alt_means <- as.data.table(delta_transform(mean=matched$mean2, sd=matched$standard_error2, transformation="linear_to_logit"))
setnames(logit_alt_means, c("mean_logit", "sd_logit"), c("logit_alt_mean", "logit_alt_se"))

# bind back onto main data table
matched <- cbind(matched, logit_alt_means)
matched <- cbind(matched, logit_ref_means)

# use calculate_diff() to calculate the log difference between matched pairs
matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = matched, 
  alt_mean = "logit_alt_mean", alt_sd = "logit_alt_se",
  ref_mean = "logit_ref_mean", ref_sd = "logit_ref_se" )

#Write files -----
write.csv(matched, paste0(flat_file_dir, "/matches_data_marketscan.csv"))

mrbrt_setup <- create_mrbrtdt(matched, vars = cvs)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]

#Write files -----
write.csv(mrbrt_dt, paste0(flat_file_dir, "/pre_xw_data_marketscan.csv"))

# Marketscan run MR-BRT
df1 <- CWData(
  df = mrbrt_dt,          # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "def2",     # column name of the variable indicating the alternative method
  ref_dorms = "def",     # column name of the variable indicating the reference method
  covs= list(),
  study_id = "id2",    # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

fit1 <- CWModel(
  cwdata = df1,            # object returned by `CWData()`
  obs_type = "diff_logit", # is not working # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  inlier_pct= 0.9, 
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept")),
  gold_dorm = "reference"   # the level of `alt_dorms` that indicates it's the gold standard
)


repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = fit1, 
  cwdata = df1,
  continuous_variables = list(),
  obs_method = "cv_marketscan",
  plot_note = "Funnel plot Epilepsy, Marketscan", 
  plots_dir = plot_dir, 
  file_name = "funnel_plot_epi_marketscan",
  write_file = TRUE
)

# check outputs
fit1$fixed_vars
fit1$gamma

# Marketscan Save results ----
df_result <- fit1$create_result_df()
write.csv(df_result, paste0(flat_file_dir, "/model_results_marketscan.csv"))
py_save_object(object = fit1, filename = paste0(flat_file_dir, "/model_marketscan.pkl"), pickle = "dill")

################################### Marketscan SD >1 rounded to 1 plot ######################
mrbrt_dt <- mrbrt_dt[logit_diff_se> 1, logit_diff_se:= 1]  
# Marketscan 2000 run MR-BRT
df1 <- CWData(
  df = mrbrt_dt,          # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "def2",     # column name of the variable indicating the alternative method
  ref_dorms = "def",     # column name of the variable indicating the reference method
  covs= list(),
  study_id = "id2",    # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

fit1 <- CWModel(
  cwdata = df1,            # object returned by `CWData()`
  obs_type = "diff_logit", # is not working # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  inlier_pct= 0.9, 
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept")),
  gold_dorm = "reference"   # the level of `alt_dorms` that indicates it's the gold standard
) in NMA


repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = fit1, 
  cwdata = df1,
  continuous_variables = list(),
  obs_method = "cv_marketscan",
  plot_note = "Funnel plot Epilepsy, Marketscan", 
  plots_dir = plot_dir, 
  file_name = "funnel_plot_epi_marketscan_SD<1",
  write_file = TRUE
)

################################ Applying crosswalks ---------------
#Using most recent crosswalks:
model_dir <- flat_file_dir
fit_marketscan <- py_load_object(filename = paste0(model_dir, "/model_marketscan.pkl"), pickle = "dill")
fit_lifetime_recall <- py_load_object(filename = paste0(model_dir, "/lifetime_recall_model.pkl"), pickle = "dill")
fit_marketscan_2000 <- py_load_object(filename = paste0(model_dir, "/model_marketscan_2000.pkl"), pickle = "dill")

sex_split_dt <- data.table(read.csv(paste0(flat_file_dir, "/sex_split_data.csv")))
dem_dt <- copy(sex_split_dt)

## Dropping Covariates we dont use:
coln <- colnames(dem_dt)
cv_drop <- coln[setdiff(grep("cv_", coln), which(coln %in% c("cv_marketscan_all_2000", "cv_recall_lifetime", "cv_marketscan")))] 
defs <- get_definitions(dem_dt, cv_drop = cv_drop)
dem_dt <- defs[[1]]
cvs <- defs[[2]]

dem_dt[, case_def := definition]

to_lifetime_xw <- copy(dem_dt[definition == "cv_recall_lifetime", ])
to_marketscan_xw <- copy(dem_dt[definition == "cv_marketscan", ])
to_marketscan_2000_xw <- copy(dem_dt[definition == "cv_marketscan_all_2000", ])
no_xw <- copy(dem_dt[definition != "cv_recall_lifetime" & definition != "cv_marketscan" & definition != "cv_marketscan_all_2000"])

nrow(to_lifetime_xw) + nrow(to_marketscan_xw) + nrow(to_marketscan_2000_xw) + nrow(no_xw) == nrow(dem_dt) 

adjusted_marketscan <- make_adjustment(fit_marketscan, to_marketscan_xw)
adjusted_marketscan_2000 <- make_adjustment(fit_marketscan_2000, to_marketscan_2000_xw)
to_lifetime_xw$crosswalk_parent_seq <- as.numeric(to_lifetime_xw$crosswalk_parent_seq) 
adjusted_lifetime <- make_adjustment(fit_lifetime_recall, to_lifetime_xw)

#Binding the data all back together after adjustment
full_dt <- data.table(do.call("rbind.fill", list(adjusted_marketscan$epidb, adjusted_marketscan_2000$epidb, adjusted_lifetime$epidb, no_xw, dt_not_inc_prev)))

#check:
nrow(full_dt) == nrow(dem_dt) 

full_dt[location_id == 95, group_review := 0]
full_dt <- full_dt[group_review == 1 | is.na(group_review)] 
full_dt[standard_error > 1, standard_error := 1]

#outlier Poland claims:
full_dt[nid %in% c(397812, 397813, 397814), is_outlier := 1] 

write.csv(full_dt, paste0(flat_file_dir, "adjusted_data.csv"))

write.csv(adjusted_lifetime$vetting_dt, paste0(flat_file_dir, "/adjusted_data_lifetime.csv"))
write.csv(adjusted_marketscan$vetting_dt, paste0(flat_file_dir, "/adjusted_data_marketscan.csv"))
write.csv(adjusted_marketscan_2000$vetting_dt, paste0(flat_file_dir, "/adjusted_data_marketscan_2000.csv"))

full_dt[, crosswalk_parent_seq := seq]
full_dt[, seq := ""]

age_red_dt <- full_dt[age_end - age_start < 25,]

write.xlsx(age_red_dt, paste0(flat_file_dir, "age_diff_less_than_25", date,".xlsx"), sheetName= "extraction")

save_crosswalk_version(28946, paste0(flat_file_dir, "age_diff_less_than_25", date,".xlsx"),
                       description = paste0("crosswalked, sex-specific data for for age split model correct ", date, "previous sex split SSA data group reivewed logit xwalks"))

#Matches plots for year/age span selection
sex_split_dt <- data.table(read.csv(paste0(flat_file_dir, "/sex_split_data.csv")))

dem_dt <- as.data.table(copy(sex_split_dt))
dem_dt <- dem_dt[!is_outlier == 1, ]

dem_dt <- aggregate_marketscan(dem_dt) 
## Getting references ----
coln <- colnames(dem_dt)
cv_drop <- coln[setdiff(grep("cv_", coln), which(coln %in% c("cv_marketscan_all_2000", "cv_recall_lifetime", "cv_marketscan")))] 
defs <- get_definitions(dem_dt, cv_drop = cv_drop)
dem_dt <- defs[[1]]
cvs <- defs[[2]]
rm(defs) #Keeping memory usage lower

# Marketscan Matches ----
dem_dt[, loc_match := location_id]
dem_dt <- calc_year(dem_dt)
age_dts <- get_age_combos(dem_dt)
dem_dt <- age_dts[[1]]
age_match_dt <- age_dts[[2]] 
pairs <- combn(dem_dt[, unique(definition)], 2)

create_comps <- function(full_dt, combin_dt, cv){
  cb_dt <- copy(combin_dt)
  dt <- copy(full_dt)
  cb_dt[,Poss := nrow(dt[definition == cv,])*nrow(dt[definition == "reference"])]
  if(cv == "cv_recall_lifetime"){
    for(i in 1:nrow(cb_dt)){
      dt2 <- get_matches_recall_lifetime(1, full_dt, cb_dt[i, year_span], cb_dt[i, age_span])[mean != 0 & mean2 != 0,]
      invisible(cb_dt[i, matches := nrow(dt2)])
      invisible(cb_dt[i, studies := length(unique(dt2[,nid2]))])
      invisible(cb_dt[i, num_loc := length(unique(dt2[, location_name]))])
      invisible(cb_dt[i, num_reg :=length(unique(dt2[, region_name]))])
      invisible(cb_dt[i, num_sup_reg := length(unique(dt2[, super_region_name]))])
      print(i)
    }
  } else if (cv == "cv_marketscan"){
    for(i in 1:nrow(cb_dt)){
      dt2 <- get_matches_marketscan(3, full_dt, cb_dt[i, year_span], cb_dt[i, age_span])[mean != 0 & mean2 != 0,]
      invisible(cb_dt[i, matches := nrow(dt2)])
      invisible(cb_dt[i, studies := length(unique(dt2[,nid2]))])
      invisible(cb_dt[i, num_loc := length(unique(dt2[, location_name]))])
      invisible(cb_dt[i, num_reg :=length(unique(dt2[, region_name]))])
      invisible(cb_dt[i, num_sup_reg := length(unique(dt2[, super_region_name]))])
      print(i)
    }
  } else if (cv == "cv_marketscan_all_2000"){
    for(i in 1:nrow(cb_dt)){
      dt2 <- get_matches_marketscan_2000(2, full_dt, cb_dt[i, year_span], cb_dt[i, age_span])[mean != 0 & mean2 != 0,]
      invisible(cb_dt[i, matches := nrow(dt2)])
      invisible(cb_dt[i, studies := length(unique(dt2[,nid2]))])
      invisible(cb_dt[i, num_loc := length(unique(dt2[, location_name]))])
      invisible(cb_dt[i, num_reg :=length(unique(dt2[, region_name]))])
      invisible(cb_dt[i, num_sup_reg := length(unique(dt2[, super_region_name]))])
      print(i)
    }
  }
  invisible(cb_dt)
}

combin_dt <- data.table(expand.grid(year_span = seq(5, 20, by = 5), age_span = seq(5, 20, by = 5)))
full_dt <- copy(dem_dt)



combin_recall_lifetime_dt <- create_comps(dem_dt, combin_dt, cv = "cv_recall_lifetime") 
combin_marketscan_dt <- create_comps(dem_dt, combin_dt, cv = "cv_marketscan")
combin_marketscan_2000_dt <- create_comps(dem_dt, combin_dt, cv = "cv_marketscan_all_2000")
#
#
View(combin_recall_lifetime_dt)
View(combin_marketscan_dt)

#
ggplot(combin_marketscan_dt, aes(year_span, age_span, matches)) +
  geom_raster(aes(fill = matches), hjust = 0.5, vjust = 0.5, interpolate = F) +
  geom_contour(aes(z = matches))

