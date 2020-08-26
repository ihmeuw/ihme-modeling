#########################################################################
### Purpose Crosswalk Marketscan Data to Hospital Data
##########################################################################

rm(list=ls())

pacman::p_load(data.table, openxlsx, ggplot2)
library(mortdb, lib = FILEPATH)
library(msm)
library(Hmisc)
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------
b_id <- OBJECT
decomp_step <- OBJECT
new_bundle_version_id <- OBJECT

functions_dir <- "FILEPATH"
mrbrt_helper_dir <- "FILEPATH"
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_crosswalk_version.R")
mrbrt_dir <- "FILEPATH"
cv_drop <- c("cv_survey", "cv_hospital", "cv_literature", "cv_marketscan_all_2000", "cv_marketscan_inp_2000", "cv_marketscan_all_2010",
            "cv_marketscan_inp_2010", "cv_marketscan_all_2012", "cv_marketscan_inp_2012", "cv_inpatient") # Bundle 899
draws <- paste0("draw_", 0:999)



# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_location_metadata.R"))
source(paste0(functions_dir, "get_age_metadata.R"))

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

fix_cvs <- function(raw_dt, outlier) {
  dt <- copy(raw_dt)
  dt <- dt[grepl("MarketScan", field_citation_value) & grepl("2000", field_citation_value), cv_marketscan_2000 :=1 ]
  dt <- dt[grepl("MarketScan", field_citation_value) & !grepl("2000", field_citation_value), cv_marketscan_other := 1]
  cvs <- names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop]
  for (cov in cvs) {
    dt[is.na(get(cov)), paste(cov) := 0]
  }
  if (outlier == TRUE) {
    dt <- dt[!is_outlier == 1 & measure %in% c("prevalence")]
  }
  return(dt)
}

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

## HELPER FUNCTION TO ROUND TO NEAREST AGE START/AGE END FOR GBD AGE GROUPS
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
  matched <- matched[sex == sex2 & measure == measure2 &
                       between(year_match, year_match2 - year_span/2, year_match2 + year_span/2)] ## FILTER OUT SEX, MEASURE, YEAR
  if (pair[1] == "_cv_marketscan" | pair[2] == "_cv_marketscan"){ ## EXACT LOCS FOR MARKETSCAN BUT CURRENTLY ALL ARE EXACT LOCS
    matched <- matched[location_id == location_id2]
  }
  matched[, age_match := (between(age_start, age_start2 - age_span/2, age_start2 + age_span/2) & between(age_end, age_end2 - age_span/2, age_end2 + age_span/2))]
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

mid_year_age <- function(ratio_dt) {
  dt <- copy(ratio_dt)
  dt[, mid_year := (year_start + year_end )/ 2]
  dt[, mid_age := (age_start + age_end)/2]
  return(dt)
}

create_mrbrtdt <- function(match_dt, vars = cvs, age = T){
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
  dt[, id := paste0(nid2, " (", location_ascii_name2, ": ", sex2, " ", age_start2, "-", age_end2, ") - ", nid, " (", location_ascii_name, ": ", sex, " ", age_start, "-", age_end, ")")]
  if (age == F){
    dt <- dt[, c("id", "ldiff", "ldiff_se", "def", "def2", vars), with = F]
  } else if (age == T){
    dt <- dt[, c("id", "age", "ldiff", "ldiff_se", "def", "def2", vars), with = F]
  }
  return(list(dt, vars))
}

get_preds <- function(model, covs){
  preds <- unique(mrbrt_dt[, c(covs), with = F])
  predicts <- predict_mr_brt(model, newdata = preds, write_draws = T)
  return(predicts)
}



# RUN DATA SETUP ----------------------------------------------------------

message("Formatting data")
bundle_dt <- get_bundle_version(new_bundle_version_id)
cir_dt <- copy(bundle_dt)
cir_dt <- fix_cvs(cir_dt, outlier = F)
cir_dt <- get_cases_sample_size(cir_dt)
cir_dt <- get_se(cir_dt)
cir_dt <- calculate_cases_fromse(cir_dt)
defs <- get_definitions(cir_dt)
cir_dt <- defs[[1]]
cvs <- defs[[2]]
cir_dt <- subnat_to_nat(cir_dt, subnat = F)
cir_dt <- calc_year(cir_dt)
age_dts <- get_age_combos(cir_dt)
cir_dt <- age_dts[[1]]
age_match_dt <- age_dts[[2]] ## DON'T ACTUALLY NEED THIS BUT FOUND IT HELPFUL FOR VETTING
pairs <- combn(cir_dt[, unique(definition)], 2)
matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = cir_dt)))
diff <- create_differences(matches)
nrow(diff[(grepl("Inf", ldiff))])
count((diff[(grepl("Inf", ldiff))]), "age_start")
diff <- diff[!(grepl("Inf", ldiff))]
mrbrt_setup <- create_mrbrtdt(diff)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]


# RUN MR-BRT MODEL -------------------------------------------------------

mkscn_matches <- copy(diff[def == "reference" & def2 == "_cv_marketscan_2000", ]) # select def as other or 2000
cvs <- "_cv_marketscan_2000"
mkscn_setup <- create_mrbrtdt(mkscn_matches, cvs,age = T)
mrbrt_mkscn_dt <- mkscn_setup[[1]]


if (file.exists("FILEPATH")){
  cir_model <- readr::read_rds("FILEPATH")
} else {
  cir_model <- run_mr_brt(
    output_dir = mrbrt_dir,
    model_label = mkscn_name,
    remove_x_intercept = F,
    data = mrbrt_mkscn_dt,
    mean_var = "ldiff",
    se_var = "ldiff_se",
    covs = list(cov_info("age", "X", degree = 3,
                         n_i_knots = 5, bspline_gprior_mean = "0, 0, 0, 0, 0, 0",
                         bspline_gprior_var = "1e-5, inf, inf, inf, inf, 1e-5")),
    study_id = "id",
    trim_pct = 0.1,
    method = "trim_maxL",
    overwrite_previous = T
  )
  readr::write_rds(cir_model, "FILEPATH")
}


pred_dt <- expand.grid(age = seq(0, 115, by = 5))

mkscn_preds <- predict_mr_brt(cir_model, newdata = pred_dt, write_draws = T)

mkscn_graph <- graph_mrbrt_results(results = cir_model, predicts = mkscn_preds)

pred_dt <- data.table(read.csv(paste0(mrbrt_dir, mkscn_name, "/model_draws.csv")))
pred_dt[, logadj := rowMeans(.SD), .SDcols = draws]
pred_dt[, logadj_se := apply(.SD, 1, sd), .SDcols = draws]
pred_dt[, c(draws, "Z_intercept") := NULL]
setnames(pred_dt, names(pred_dt), gsub("^X_", "", names(pred_dt)))
pred_dt$adjust_for <- sapply(1:nrow(pred_dt), function(i){
  names(pred_dt[i])[which(pred_dt[i] == 1, arr.ind=T)[, "col"]]})
pred_table <- copy(pred_dt)



# PREDICTION --------------------------------------------------------------


# ADJUST MARKETSCAN -------------------------------------------------------

get_mkscn_adjustment <- function(input_data){ 
  pred_dt <- data.table(read.csv("FILEPATH"))
  pred_dt[, ladj := rowMeans(.SD), .SDcols = draws]
  pred_dt[, ladj_se := apply(.SD, 1, sd), .SDcols = draws]
  pred_dt[, c(draws, "Z_intercept", "X_intercept") := NULL]
  setnames(pred_dt, "X_age", "age")
  return(pred_dt)
}

adjust_mkscn <- function(data_dt, ratio_dt){
  dt <- copy(data_dt)
  dt[, age := (age_start + age_end) / 2]
  dt <- merge(dt, ratio_dt, by = "age", all.x = T)
  adjust_dt <- copy(dt[!is.na(ladj) & !mean == 0])
  adjustse_dt <- copy(dt[!is.na(ladj) & mean == 0])

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
  adjust_dt[is.nan(l_se), standard_error_adj := standard_error]

  adjust_dt[, `:=` (crosswalk_parent_seq = seq)]
  adjust_dt$note_modeler <- NA
  adjust_dt[, `:=` (mean = mean_adj, standard_error = standard_error_adj, upper = "", lower = "", seq = "",
                    cases = "", sample_size = "", uncertainty_type_value = "", effective_sample_size = "",
                    note_modeler = paste0(note_modeler, " | crosswalked with logit(difference): ", round(ladj, 2), " (",
                                          round(ladj_se, 2), ")"))]
  extra_cols <- c("ladj", "ladj_se", "lmean", "l_se", "lmean_adj", "lmean_adj_se", "mean_adj", "standard_error_adj", "age")
  adjust_dt[, c(extra_cols) := NULL]

  ## ADJUST SE FOR ZERO MEANS
  adjustse_dt$adj_se <- sapply(1:nrow(adjustse_dt), function(i){
    mean_i <- as.numeric(adjustse_dt[i, "ladj"])
    se_i <- as.numeric(adjustse_dt[i, "ladj_se"])
    deltamethod(~exp(x1)/(1+exp(x1)), mean_i, se_i^2)
  })
  adjustse_dt$crosswalk_parent_seq <- adjustse_dt$seq
  adjustse_dt[is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq)]
  adjustse_dt[, `:=` (standard_error = sqrt(standard_error^2 + adj_se^2), upper = "",
                      lower = "", seq = "", cases = "", sample_size = "",
                      uncertainty_type_value = "", effective_sample_size = "",
                      note_modeler = "uncertainty from network analysis added")]
  extra_cols <- c("adj_se")
  adjustse_dt[, c(extra_cols) := NULL]

  final_dt <- rbind(adjust_dt, adjustse_dt, fill = T, use.names = T)
  return(list(adjusted = final_dt, vetting = full_dt))
}

mkscn_prediction_plot <- function(vetting_data){
   dt <- copy(vetting_data)
   dt <- merge(dt, loc_dt[, .(location_id, super_region_name)], by = "location_id")
   dt[, `:=` (N_adj = (mean_adj*(1-mean_adj)/standard_error_adj^2),
              N = (mean*(1-mean)/standard_error^2))]
   wilson_norm <- as.data.table(binconf(dt$mean*dt$N, dt$N, method = "wilson"))
   wilson_adj <- as.data.table(binconf(dt$mean_adj*dt$N_adj, dt$N_adj, method = "wilson"))
   dt[, `:=` (lower = wilson_norm$Lower, upper = wilson_norm$Upper,
              lower_adj = wilson_adj$Lower, upper_adj = wilson_adj$Upper)]
   dt[, midage := (age_end + age_start)/2]
   ages <- c(60, 70, 80, 90)
   dt[, age_group := cut2(midage, ages)]
   gg_funct <- function(graph_dt){
     gg <- ggplot(graph_dt, aes(x = mean, y = mean_adj, color = as.factor(year_start), shape = as.factor(sex))) +
       geom_point() +
       facet_wrap(~measure+age_group, ncol = 5) +
       geom_errorbar(aes(ymin = lower_adj, ymax = upper_adj)) +
       geom_errorbarh(aes(xmin = lower, xmax = upper)) +
       scale_color_brewer(palette = "Spectral", name = "Year") +
       labs(x = "Unadjusted means", y = "Adjusted means") +
       theme_classic()
     return(gg)
   }
   state_funct <- function(state_dt){
     gg <- gg_funct(graph_dt = state_dt) + ggtitle(state_dt[, unique(location_name)])
     return(gg)
   }
   state_plots <- lapply(dt[, unique(location_name)], function(x) state_funct(state_dt = dt[location_name == x]))
   return(state_plots)
 }


message("adjusting data")
new_bundle_dt <- get_bundle_version(new_bundle_version_id)
mkscn_data <- copy(new_bundle_dt)
mkscn_name <- OBJECT
mkscn_data <- fix_cvs(mkscn_data, outlier = F)
mkscn_data1 <- mkscn_data[cv_marketscan_other == 1, ] # choose between other or 2000 depending on which model is being run
mkscn_adjust <- get_mkscn_adjustment(mkscn_data)
mkscn_adjusted <- adjust_mkscn(mkscn_data1, mkscn_adjust)
unadjusted <- mkscn_data[cv_marketscan_other == 0, ]
all_data_final <- rbind(mkscn_adjusted$adjusted, unadjusted, fill = T)
setnames(all_data_final, c("cv_marketscan_other", "cv_marketscan_2000"), c("marketscan_other", "marketscan_2000"))
