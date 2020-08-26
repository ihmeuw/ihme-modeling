
# FUNCTIONS ---------------------------------------------------------------

## GET DEFINITIONS (ALL BASED ON CV'S - WHERE ALL 0'S IS REFERENCE)
get_definitions <- function(ref_dt){
  dt <- copy(ref_dt)
  cvs <- names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop]
  cvs <- cvs[!cvs %in% cv_drop] ## I DROPPED CV'S I DIDN'T WANT TO USE IN THE CV_DROP OBJECT
  dt[, definition := ""]
  for (cv in cvs){
    dt[get(cv) == 1, definition := paste0(definition, "_", cv)]
  }
  dt[definition == "", definition := "reference"]
  return(list(dt, cvs))
}

## MAPPING SUBNATIONALS TO NATIONAL LOCATIONS
subnat_to_nat <- function(subnat_dt){
  dt <- copy(subnat_dt)
  dt[, ihme_loc_id := NULL]
  dt <- merge(dt, loc_dt[, .(location_id, level, parent_id, location_type, ihme_loc_id)], by = "location_id")
  dt[, loc_match := location_id]
  dt[level >= 4, loc_match := parent_id]
  dt[level == 6, loc_match := ID] ## PAIR UTLAS TO ENGLAND
  dt[level == 5 & grepl("GBR", ihme_loc_id), loc_match := ID] ## PAIR ENGLAND REGIONS TO ENGLAND
  dt[level == 5 & location_type %in% c("urbanicity", "admin2"), loc_match := ID] ## PAIR INDIA URBAN/RURAL TO INDIA
  dt[level == 5 & grepl("CHN", ihme_loc_id), loc_match := ID]
  dt[level == 5 & grepl("KEN", ihme_loc_id), loc_match := ID]
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

## HELPER FUNCTION TO ROUND TO NEAREST AGE START/AGE END USED IN GBD
get_closest_age <- function(i, var, start = T, dt){
  age <- dt[i, get(var)]
  if (start == T){
    age_dt[, age_group_years_start][which.min(abs(age_dt[, age_group_years_start] - age))]
  } else if (start == F){
    age_dt[, age_group_years_end][which.min(abs(age_dt[, age_group_years_end] - age))]
  }
}

## ACTUALLY AGGREGATE
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
  new_row <- cbind(row_dt, row_dt2) ## PUT THE SIDES BACK TOGETHER
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
  matched <- matched[sex == sex2 & measure == measure2 & year_match >=year_match2-year_span/2 &
                       year_match <= year_match2 + year_span/2] ## FILTER OUT SEX, MEASURE, YEAR
  if (pair[1] == "_cv_marketscan" | pair[2] == "_cv_marketscan"){ ## EXACT LOCS FOR MARKETSCAN BUT CURRENTLY ALL ARE EXACT LOCS
    matched <- matched[location_id == location_id2]
  }
  matched[, age_match := (age_start >= age_start2 - age_span/2 & age_start <= age_start2 + age_span/2) &
            (age_end >= age_end2 - age_span/2 & age_end <= age_end2 + age_span/2)]
  unmatched <- copy(matched[age_match == F])
  unmatched <- unmatched[!age_start >= 100]
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

graph_combos <- function(model, predictions){
  data <- as.data.table(model$train_data)
  data[, diff_name := paste0(def2, " - ", def)]
  if (length(names(data)[grep("^X", names(data))]) > 0){
    name_change <- names(data)[grepl("^X", names(data))]
    setnames(data, name_change, gsub("^X", "", name_change))
  }
  data[, `:=` (ldiff_l = ldiff - 1.96*ldiff_se, ldiff_u = ldiff + 1.96*ldiff_se)]
  preds <- as.data.table(predictions$model_draws)
  preds <- summaries(preds, draws)
  xcov_names <- names(preds)[grepl("^X", names(preds))]
  setnames(preds, xcov_names, gsub("^X_", "", xcov_names))
  preds <- merge(preds, unique(data[, c("diff_name", mrbrt_vars), with = F]), by = mrbrt_vars)
  fit_graph <- function(n){
    diff <- preds[, diff_name][n]
    diff_mean <- preds[diff_name == diff, mean]
    diff_lower <- preds[diff_name == diff, lower]
    diff_upper <- preds[diff_name == diff, upper]
    graph_dt <- copy(data[diff_name == diff])
    graph_dt[, idnum := 1:.N]
    gg <- ggplot() +
      geom_rect(data = graph_dt[1,], xmin = diff_lower, xmax = diff_upper, ymin = -Inf, ymax = Inf, alpha = 0.2, fill = "darkorchid") +
      geom_point(data = graph_dt, aes(x = ldiff, y = as.factor(idnum), color = as.factor(w))) +
      geom_errorbarh(data = graph_dt, aes(y = as.factor(idnum), x = ldiff, xmin = ldiff_l, xmax = ldiff_u, color = as.factor(w))) +
      geom_vline(xintercept = diff_mean, linetype = "dashed", color = "darkorchid") +
      geom_vline(xintercept = 0) +
      labs(x = "Logit Difference", y = "") +
      xlim(-3, 5.5) +
      scale_color_manual(name = "", values = c("1" = "midnightblue", "0" = "red"), labels = c("0" = "Trimmed", "1" = "Included")) +
      scale_y_discrete(labels = graph_dt[, id]) +
      ggtitle(paste0("Model fit for difference ", diff)) +
      theme_classic() +
      theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(),
            axis.text.y = element_text(size = 5))
    return(gg)
  }
  fit_graphs <- lapply(1:length(preds[, unique(diff_name)]), fit_graph)
  return(fit_graphs)
}

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
  adjust_dt[is.nan(l_se), standard_error_adj := standard_error] 
  
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

prediction_plot <- function(vetting_data, by_state = F){
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
    gg <- ggplot(graph_dt, aes(x = mean, y = mean_adj, color = as.factor(super_region_name))) +
      geom_point() +
      facet_wrap(~measure+age_group, ncol = 5) +
      geom_errorbar(aes(ymin = lower_adj, ymax = upper_adj)) +
      geom_errorbarh(aes(xmin = lower, xmax = upper)) +
      scale_color_brewer(palette = "Spectral", name = "Super Region") + 
      labs(x = "Unadjusted means", y = "Adjusted means") +
      theme_classic()
    return(gg)
  }
  if (by_state == F){
    return(gg_funct(graph_dt = dt))
  } else if (by_state == T){
    state_funct <- function(state_dt){
      gg <- gg_funct(graph_dt = state_dt) + ggtitle(state_dt[, unique(location_name)])
      return(gg)
    }
    state_plots <- lapply(dt[, unique(location_name)], function(x) state_funct(state_dt = dt[location_name == x]))
    return(state_plots)
  }
}

# CROSSWALK SETUP ---------------------------------------------------------

message("formatting data")
dem_dt <- predict_sex$final
dem_dt <- aggregate_marketscan(dem_dt)
dem_dt <- get_cases_sample_size(dem_dt)
dem_dt <- get_se(dem_dt)
dem_dt <- calculate_cases_fromse(dem_dt)
defs <- get_definitions(dem_dt)
dem_dt <- defs[[1]]
cvs <- defs[[2]]
dem_dt <- subnat_to_nat(dem_dt)
dem_dt <- calc_year(dem_dt)
age_dts <- get_age_combos(dem_dt)
dem_dt <- age_dts[[1]]
age_match_dt <- age_dts[[2]] ## HELPFUL FOR VETTING
pairs <- combn(dem_dt[, unique(definition)], 2)
message("finding matches")
matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = dem_dt)))
matches <- create_differences(matches)
mrbrt_setup <- create_mrbrtdt(matches)
mrbrt_dt <- mrbrt_setup[[1]]
mrbrt_vars <- mrbrt_setup[[2]]

# RUN NETWORK MODEL -------------------------------------------------------

message("running model")
model_name <- paste0("dementia_network_", date)

create_covs <- function(x){
  if (x == "cutoff_score_diagnosis"){
    return(cov_info(x, "X", uprior_lb = 0))
  } else {
    return(cov_info(x, "X"))
  }
}

cov_list <- lapply(mrbrt_vars, create_covs)

if (file.exists(paste0("FILEPATH"))){
  dem_model <- readr::read_rds(paste0("FILEPATH"))
} else {
  dem_model <- run_mr_brt(
    output_dir = mrbrt_dir,
    model_label = model_name,
    remove_x_intercept = T,
    data = mrbrt_dt,
    mean_var = "ldiff",
    se_var = "ldiff_se",
    covs = cov_list,
    study_id = "id",
    method = "trim_maxL",
    trim_pct = 0.1
  )
  readr::write_rds(dem_model, paste0("FILEPATH"))
}

dem_predicts <- get_preds(dem_model, mrbrt_vars)

forrestplot_graphs <- graph_combos(model = dem_model, predictions = dem_predicts)

# PREDICTION --------------------------------------------------------------

message("adjusting data")
adjust_dt <- predict_sex$final
adjust_dt <- adjust_dt[!cv_marketscan == 1] ## DON'T ADJUST MARKETSCAN DATA
adjust_dt <- adjust_dt[!clinical_data_type == "claims"]
adjust_preds <- get_preds_adjustment(raw_data = adjust_dt, model = dem_model)
  model <- dem_model
  pred_dt <- as.data.table(read.csv((paste0("FILEPATH"))))
  pred_dt <- as.data.table(preds$model_draws)
  pred_dt[, ladj := rowMeans(.SD), .SDcols = draws]
  pred_dt[, ladj_se := apply(.SD, 1, sd), .SDcols = draws]
  pred_dt[, c(draws, "Z_intercept") := NULL]
setnames(pred_dt, names(pred_dt), gsub("^X", "cv", names(pred_dt)))
adjust_dt <- get_cases_sample_size(adjust_dt)
adjust_dt <- get_se(adjust_dt)
adjusted <- make_adjustment(data_dt = adjust_dt, ratio_dt = adjust_preds)

data_plot <- prediction_plot(adjusted$vetting_dt[!cv_marketscan == 1])
ggsave(data_plot, filename = paste0("FILEPATH"), width = 12)

