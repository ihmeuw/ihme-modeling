##########################################################################
### Purpose: Data processing - sex splitting, crosswalking, helper functions
##########################################################################


# GET METADATA ------------------------------------------------------------
source(FILEPATH)
source("FILEPATH/get_location_metadata.R")


loc_dt <- get_location_metadata(location_set_id = 35, gbd_round_id = 7)
super_region_dt <- loc_dt[, .(location_id, super_region_id, super_region_name)]
age_dt <- get_age_metadata(19, gbd_round_id = 7)


# DATA PROCESSING FUNCTIONS -----------------------------------------------
## REPLACE THIS FUNCTION WITH WHATEVER YOU WANT TO USE TO PULL IN DATA
get_file_data <- function(){
  dt <- as.data.table(read.xlsx(file_path))
  dt <- dt[measure %in% c(measures)]
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
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
 return(dt)
}

calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size), sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

find_sex_match <- function(dt){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% measures]
  match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end", 
                  names(sex_dt)[names(sex_dt) %in% sex_covs], 
                  names(sex_dt)[grepl("^cv_", names(sex_dt)) & !names(sex_dt) %in% cv_drop])
  match_vars <- match_vars[match_vars != ""]
  sex_dt[, match_n := .N, by = match_vars]
  sex_dt <- sex_dt[match_n == 2]
  keep_vars <- c(match_vars, "sex", "mean", "standard_error")
  sex_dt[, id := .GRP, by = match_vars]
  sex_dt <- dplyr::select(sex_dt, keep_vars)
  sex_dt <- dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"), fun.aggregate=mean)
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0]
  sex_dt[, study_id := .GRP, by = c("nid", "location_id")]
  sex_dt[, dorm_alt := "Female"]
  sex_dt[, dorm_ref := "Male"]
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

## GET DEFINITIONS (ALL BASED ON CV'S - WHERE ALL 0'S IS REFERENCE)
get_definitions <- function(ref_dt){
  dt <- copy(ref_dt)
  cvs <- names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop & !names(dt) %in% reference]
  cvs <- cvs[!cvs %in% cv_drop] ## I DROPPED CV'S I DIDN'T WANT TO USE IN THE CV_DROP OBJECT
  dt[, definition := ""]
  if (reference == "") {
    message("Matching based on old reference_cov method")
    for (cv in cvs){
    dt[get(cv) == 1, definition := paste0(definition, "_", cv)]
    }
    dt[definition == "", definition := "reference"]
  } else {
    dt[get(reference) == 1, definition := gsub("cv_", "", paste0(reference))]
    for (cv in cvs) {
      dt[get(cv) == 1, definition := paste0(definition, "_", cv)]
    }
  }
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

## AGGREGATE
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
get_matches <- function(n, pair_dt, year_span = 10, age_span = 10){
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
  matched <- merge(dt1, dt2, by = "loc_match", allow.cartesian = T) ## INITIAL CARTESIAN MERGE ONLY MATCHING ON LOCATION
  matched <- matched[sex == sex2 & measure == measure2 & year_match >=year_match2-year_span/2 &
                       year_match <= year_match2 + year_span/2] ## FILETER OUT SEX, MEASURE, YEAR
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
  
  ## AGGREGATE IF THERE ARE PLACES TO AGGREGATE, OTHERWISE DO NOTHING
  if (nrow(agg_matches) > 0){
    aggregated <- rbindlist(lapply(1:agg_matches[, max(agg_id)], function(x) aggregate_tomatch(match_dt = agg_matches, id = x)))
    aggregated[, c("age_start_match", "age_end_match", "sum_start", "sum_end", "agg_id") := NULL]
    final_match <- rbind(matched[age_match == T], aggregated)
  } else {
    final_match <- copy(matched[age_match == T])
  }
  
  final_match[, c("age_match") := NULL]
  final_match[, `:=` (def = pair[1], def2 = pair[2])] ## LABEL WITH DEFINITIONS
  message(paste(nrow(final_match), pair[1], pair[2]))
  return(final_match)
}

create_ratios <- function(ratio_dt){
  dt <- copy(ratio_dt)
  if (logit_transform == T) {
    message("Logit transforming")
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
    drop_count <- nrow(dt[is.na(ldiff)])
    if (drop_count > 0) {
    message(paste0("Dropping zeroes: Drop count of ", drop_count))
    dt <- dt[!is.na(ldiff), ]
    }
  } else {
    message("Log tranforming")
    dt[, ratio := mean2/mean]
    dt[, ratio_se := sqrt((mean2^2 / mean^2) * (standard_error2^2/mean2^2 + standard_error^2/mean^2))]
    dt[, log_ratio := log(ratio)]
    dt$log_rse <- sapply(1:nrow(dt), function(i) {
      mean_i <- dt[i, "ratio"]
      se_i <- dt[i, "ratio_se"]
      deltamethod(~log(x1), mean_i, se_i^2)
    })
    drop_count <- nrow(dt[is.na(log_rse)])
    if (drop_count > 0) {
      message(paste0("Dropping zeroes: Drop count of ", drop_count))
      dt <- dt[!is.na(log_rse), ]
    }
  }
  return(dt)
}

create_dual_ratios <- function(ratio_dt) {
  dt <- copy(ratio_dt)
  message("Log tranforming")
  dt[, ratio := mean/mean2]
  dt[, ratio_se := sqrt((mean^2 / mean2^2) * (standard_error^2/mean^2 + standard_error2^2/mean2^2))]
  dt[, log_ratio := log(ratio)]
  dt$log_rse <- sapply(1:nrow(dt), function(i) {
    mean_i <- dt[i, "ratio"]
    se_i <- dt[i, "ratio_se"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  drop_count <- nrow(dt[is.na(log_rse)])
  if (drop_count > 0) {
    message(paste0("Dropping zeroes: Drop count of ", drop_count))
    dt <- dt[!is.na(log_rse), ]
  return(dt)
  }
}


create_mrbrtdt <- function(match_dt, vars = cvs, age = F){
  dt <- copy(match_dt)
  dt$dorm_ref <- gsub("_cv_", "", dt$def)
  dt$dorm_alt <- gsub("_cv_", "", dt$def2)
  loc_map <- copy(loc_dt[, .(location_ascii_name, location_id)])
  dt <- merge(dt, loc_map, by = "location_id")
  setnames(loc_map, c("location_id", "location_ascii_name"), c("location_id2", "location_ascii_name2"))
  if(nash_cryptogenic == T) {
    message("Nash crytogenic")
    setnames(dt, "location_id", "location_id2")
    dt <- merge(dt, loc_map, by = "location_id2")
    dt[, `:=` (age_start2 = age_start, age_end2 = age_end)]
  } else {
    dt <- merge(dt, loc_map, by = "location_id2")
  }
  dt <- merge(dt, loc_map, by = "location_id2")
  dt[, age := (age_start + age_end + age_start2 + age_end2) / 4]
  dt[, id := paste0(nid, " (", location_ascii_name, ": ", sex, " ", age_start, "-", age_end, ")")]
  if (logit_transform == T) {
    if (age == F){
      dt <- dt[, c("id", "ldiff", "ldiff_se", "dorm_ref", "dorm_alt", "nid"), with = F]
    } else if (age == T){
      dt <- dt[, c("id", "age","ldiff", "ldiff_se", "dorm_ref", "dorm_alt", "nid"), with = F]
    }
  } else{
  if (age == F){
    dt <- dt[, c("id", "log_ratio", "log_rse", "dorm_ref", "dorm_alt", "nid"), with = F]
  } else if (age == T){
    dt <- dt[, c("id", "age", "log_ratio", "log_rse", "dorm_ref", "dorm_alt", "nid"), with = F]
  }
  }
  dt[, study_id := .GRP, by = "nid"]
  return(list(dt, vars))
}

create_dual_mrbrtdt <- function(match_dt, vars = cvs, age = F){
  dt <- copy(match_dt)
  dt$dorm_ref <- gsub("_cv_", "", dt$def2)
  dt$dorm_alt <- gsub("_cv_", "", dt$def)
  loc_map <- copy(loc_dt[, .(location_ascii_name, location_id)])
  dt <- merge(dt, loc_map, by = "location_id")
  setnames(loc_map, c("location_id", "location_ascii_name"), c("location_id2", "location_ascii_name2"))
  dt <- merge(dt, loc_map, by = "location_id2")
  dt[, age := (age_start + age_end + age_start2 + age_end2) / 4]
  dt[, id := paste0(nid, " (", location_ascii_name, ": ", sex, " ", age_start, "-", age_end, ")")]
  if (age == F){
      dt <- dt[, c("id", "log_ratio", "log_rse", "dorm_ref", "dorm_alt", "nid"), with = F]
    } else if (age == T){
      dt <- dt[, c("id", "age", "log_ratio", "log_rse", "dorm_ref", "dorm_alt", "nid"), with = F]
  }
  dt[, study_id := .GRP, by = "nid"]
  return(list(dt, vars))
}


save_mrbrt <- function(model, mrbrt_directory, mod_name) {
  df_result <- model$create_result_df()
  write.csv(df_result, FILEPATH, row.names = FALSE)
  py_save_object(object = model, filename = FILEPATH, pickle = "dill")
}

save_model_RDS <- function(results, path, model_name){
  names <- c("beta", 
             "beta_sd", 
             "constraint_mat",
             "cov_mat",
             "cov_models",
             "cwdata",
             "design_mat",
             "fixed_vars",
             "gamma",
             "gold_dorm", 
             "lt",
             "num_vars",
             "num_vars_per_dorm",
             "obs_type",
             "order_prior",
             "random_vars",
             "relation_mat",
             "var_idx",
             "vars",
             "w")
  model <- list()
  for (name in names){
    if(is.null(results[[name]])) {
      message(name, " is NULL in original object, will not be included in RDS")
    }
    model[[name]] <- results[[name]]
  }
  saveRDS(model, paste0(path, "/", model_name, "/model_object.RDS"))
  message("RDS object saved to ", FILEPATH)
  return(model)
}

create_diagnostic_cov <- function(raw_dt) {
  dt <- copy(raw_dt)
  if (sex_covs == "diagnostic") {
    message("Creating diagnostic cov with antibody as 1 and rna as 0")
    dt[cv_antibody == 1, diagnostic := 1]
    dt[cv_rna == 1, diagnostic := 0]
    dt[is.na(diagnostic), diagnostic := 1]
  } 
}


get_weights <- function(fit) cbind(fit$cwdata$df, data.frame(w = fit$w))

graph_combos <- function(model, logit_transform = T){
  if(logit_transform == T) {
    data <- as.data.table(model$cwdata$df)
    data <- get_weights(model)
    data$w <- round(data$w, 0) # indicate trimming based on percentage 
    data <- as.data.table(data)
    data[, diff_name := paste0(dorm_alt, " - ", dorm_ref)]
    data[, `:=` (ldiff_l = ldiff - 1.96*ldiff_se, ldiff_u = ldiff + 1.96*ldiff_se)]
    preds <- model$fixed_vars
    preds <- as.data.frame(preds)
    preds <- rbind(preds, model$beta_sd)
    for (cov_name in unique(data$dorm_alt)) {
      graph_dt <- data[dorm_alt == cov_name]
      pred_mean <- preds[cov_name][[1]][1]
      pred_se <- preds[cov_name][[1]][2]
      graph_dt[, `:=` (pred_l = pred_mean - 1.96*pred_se, pred_u = pred_mean + 1.96*pred_se)]
      gg_fit <- ggplot() +
        geom_rect(data = graph_dt, aes(xmin = pred_l, xmax = pred_u, ymin = -Inf, ymax = Inf), alpha = 0.2, fill = "mediumorchid3") +
        geom_point(data = graph_dt, aes(x = ldiff, y = as.factor(id), color = as.factor(w))) +
        geom_errorbarh(data = graph_dt, aes(y = as.factor(id), xmin = ldiff_l, xmax = ldiff_u)) +
        geom_vline(xintercept = pred_mean, linetype = "dashed", color = "darkorchid") +
        geom_vline(xintercept = 0) +
        labs(x = "Logit Difference", y = "") +
        scale_color_manual(name = "", values = c("1" = "midnightblue", "0" = "red"), labels = c("0" = "Trimmed", "1" = "Included")) +
        # scale_y_discrete(labels = graph_dt[, id]) +
        ggtitle(paste0("Model fit for difference ", graph_dt$diff)) +
        # xlim(-7, 7) +
        theme_classic() +
        theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(),
              axis.text.y = element_text(size = 5))
      ggsave(gg_fit, filename = FILEPATH, width = 12)
    }
    return(gg_fit)
  } else {
    data <- as.data.table(model$cwdata$df)
    data <- get_weights(model)
    data$w <- round(data$w, 0) # indicate trimming based on percentage 
    data <- as.data.table(data)
    data[, diff_name := paste0(dorm_alt, " - ", dorm_ref)]
    data[, `:=` (log_ratio_l = log_ratio - 1.96*log_rse, log_ratio_u = log_ratio + 1.96*log_rse)]
    preds <- model$fixed_vars
    preds <- as.data.frame(preds)
    preds <- rbind(preds, model$beta_sd)
    for (cov_name in unique(data$dorm_alt)) {
      graph_dt <- data[dorm_alt == cov_name]
      pred_mean <- preds[cov_name][[1]][1]
      pred_se <- preds[cov_name][[1]][2]
      graph_dt[, `:=` (pred_l = pred_mean - 1.96*pred_se, pred_u = pred_mean + 1.96*pred_se)]
      gg_fit <- ggplot() +
        geom_rect(data = graph_dt, aes(xmin = pred_l, xmax = pred_u, ymin = -Inf, ymax = Inf), alpha = 0.2, fill = "mediumorchid3") +
        geom_point(data = graph_dt, aes(x = log_ratio, y = as.factor(id), color = as.factor(w))) +
        geom_errorbarh(data = graph_dt, aes(y = as.factor(id), xmin = log_ratio_l, xmax = log_ratio_u)) +
        geom_vline(xintercept = pred_mean, linetype = "dashed", color = "darkorchid") +
        geom_vline(xintercept = 0) +
        labs(x = "Log Ratio ", y = "") +
        scale_color_manual(name = "", values = c("1" = "midnightblue", "0" = "red"), labels = c("0" = "Trimmed", "1" = "Included")) +
        # scale_y_discrete(labels = graph_dt[, id]) +
        ggtitle(paste0("Model fit for log ratio ", graph_dt$diff_name)) +
        # xlim(-7, 7) +
        theme_classic() +
        theme(axis.title.y = element_blank(), axis.ticks.y = element_blank(), axis.line.y = element_blank(),
              axis.text.y = element_text(size = 5))
      ggsave(gg_fit, filename = FILEPATH, width = 12)
    }
    return(gg_fit)
  }
}


graph_spline <- function(model) {
  data <- as.data.table(model$cwdata$df)
  data <- get_weights(model)
  data$w <- round(data$w, 0) # indicate trimming based on percentage 
  data <- as.data.table(data)
}

plot_sex_by_age <- function(dt) {
  gg_sex_age <- ggplot() + 
                geom_point(data = dt, aes(x = mid_age, y = ratio), color = "red2") +
                # geom_point(data = dt, aes(x = mid_age, y = mean_Male), color = "blue2") + 
                # facet_wrap(~super_region_name) +  
                ylim(0, 10) +
                labs(title = "Female/Male Ratio by Mid Age") + xlab("Mid Age") + ylab("Prevalence")
  ggsave(FILEPATH, gg_sex_age)
  return(gg_sex_age)
}


make_adjustment <- function(model, full_dt, offset){
  adjust_dt <- copy(full_dt[mean != 0, ])
  adjustse_dt <- copy(full_dt[mean == 0 & definition != reference_def, ])
  noadjust_dt <- copy(full_dt[mean == 0 & definition == reference_def])
  
  # Get pred mean and se 
  preds <- model$fixed_vars
  preds <- as.data.frame(preds)
  preds <- rbind(preds, model$beta_sd)
  cvs <- unique(full_dt$definition)
  cvs <- cvs[cvs != "reference"]
  
  # Adjust data points based on mrbrt and nonzero 
  adjust_dt[, c("mean_adj", "standard_error_adj", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
    fit_object = model,       # result of CWModel()
    df = adjust_dt,            # original data with obs to be adjusted
    orig_dorms = "definition", # name of column with (all) def/method levels
    orig_vals_mean = "mean",  # original mean
    orig_vals_se = "standard_error"  # standard error of original mean
  )
  adjust_dt$crosswalk_parent_seq <- adjust_dt$seq
  adjust_dt$seq <- NA
  graph_adjust <- copy(adjust_dt)
  
  for (cv in cvs) {
    ladj <- preds[cv][[1]][1]
    ladj_se <- preds[cv][[1]][2]
    adjust_dt[definition == cv & diff != 0, `:=` (mean = mean_adj, standard_error = standard_error_adj, 
              note_modeler = paste0(note_modeler, " | crosswalked with logit(difference): ", 
                                    round(ladj, 2), " (", round(ladj_se, 2), ")"), 
                              cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA, effective_sample_size = NA)] 
    adjust_dt[is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq, seq = NA)]
  }
  
  ## ADJUST SE FOR ZERO MEANS
  p <- 1
  for (cv in cvs) {
    offset <- 1e-7
    i <- p 
    p <- p + 1
    # convert the mean and se of the original data to the space that the model was fit in
    adjustse_dt[definition == paste0(cv), c("logit_mean", "logit_se") := data.table(delta_transform(mean = mean + offset, sd = standard_error, transform = "linear_to_logit"))] 
    logit_pred_se <- sqrt((results$beta_sd)[i]^2 + results$gamma)
    adjustse_dt$logit_pred_se <- logit_pred_se
    # take the square root of the sum of the varainces where the variances are logit standard error of the orig data and se of the model
    # logit_se is the logit standard error of the original data point 
    # logit_pred_se is the se of the estimated beta squared and the gamma 
    adjustse_dt[definition == paste0(cv), logit_standard_error := sqrt(logit_se^2 + logit_pred_se^2)] 
    adjustse_dt[definition == paste0(cv), c("mean_adj", "standard_error_adj") := data.table(delta_transform(mean = logit_mean, sd = logit_standard_error, transform = "logit_to_linear"))] # convert it back into normal space
    adjustse_dt[definition == paste0(cv), `:=` (mean = 0, standard_error = standard_error_adj,
                        note_modeler = paste0(note_modeler, " | uncertainty from network analysis added"),
                        cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA, effective_sample_size = NA)]
    adjustse_dt[, crosswalk_parent_seq := as.numeric(crosswalk_parent_seq)]
    adjustse_dt[definition == paste0(cv) & is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq, seq = NA)]
  }
  
  # Combine everything, make sure there are no drops, drop extra cols 
  dt_all <- rbind(noadjust_dt, adjustse_dt, adjust_dt, fill = T, use.names = T)
  if (nrow(full_dt) == nrow(dt_all)) {
    message("All NIDs present")
  } else {
    missing <- setdiff(full_dt$nid, dt_all$nid)
    message(paste("Dropped NIDs:", missing, " Stop!"))
  }
  
  extra_cols <- setdiff(names(dt_all), names(full_dt))
  final_dt <- copy(dt_all)
  final_dt[, (extra_cols) := NULL]
  return(list(epidb = final_dt, vetting_dt = graph_adjust))
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
  cvs <- unique(dt$definition)
  cvs <- cvs[cvs != "reference"]
  for (cv in cvs){
    graph_dt <- dt[definition == cv]
    gg_pred <- ggplot(graph_dt, aes(x = mean, y = mean_adj, color = as.factor(super_region_name))) +
      geom_point() +
      geom_errorbar(aes(ymin = lower_adj, ymax = upper_adj)) +
      geom_errorbarh(aes(xmin = lower, xmax = upper)) +
      scale_color_brewer(palette = "Spectral", name = "Super Region") +
      labs(x = "Unadjusted means", y = "Adjusted means") +
      ggtitle(paste0("Adjusted vs Unadjusted Means: ", cv)) +
      theme_classic()
    ggsave(gg_pred, filename = FILEPATH, width = 12)
  }
  return(gg_pred)
}

adjustment_plot <- function(vetting_data) {
  dt <- copy(vetting_data) 
  dt[, midage := round((age_start + age_end) / 2, 0)]
  gg_orig <- ggplot(dt, aes(x = midage, y = mean, color = as.factor(definition))) + geom_point() +
      ggtitle("Unadjusted Means")
  gg_adj <- ggplot(dt, aes(x = midage, y = mean_adj, color = as.factor(definition))) + geom_point() + 
      ggtitle("Adjusted Means")
  ggsave(gg_orig, filename =FILEPATH, width = 12)
  ggsave(gg_adj, filename = FILEPATH, width = 12)
}


# NASH - CRYPOTGENIC CROSSWALK FUNCTIONS 
## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
prep_data_matches <- function(raw_dt) {
  dt <- copy(raw_dt)
  dt <- dt[grepl("Y|N", case_name_specific)]
  cases <- c("Y", "N")
  for (case in cases) {
    dt[, paste0("cv_", case) := ifelse(case_name_specific == case, 1, 0)]
  }
  dt[, id := .GRP, by=.(location_id, nid, sex, age_start, age_end, year_start, year_end)]
  dt[, has_both := sum(cv_Y, cv_N), by = .(id)]
  drop_nids1 <- dt[grepl("crosswalk", case_definition), unique(nid)]
  drop_nids2 <- dt[cases == 0, unique(nid)]
  drop_nids3 <- 439918 
  drop_nids <- c(drop_nids1, drop_nids2, drop_nids3)
  crosswalk <- dt[has_both == 2, ]
  crosswalk <- crosswalk[!(nid %in% drop_nids)]
  crosswalk <- crosswalk[!(nid == 439909 & sex %in% c("Male", "Female"))]
  crosswalk <- crosswalk[!grepl("child", note_modeler), ]
  # Find the places with crytogenic only 
  adjust_nids <- dt[cv_N == 1 & mean == 0 & grepl("crosswalk", case_definition), unique(nid)]
  adjust <- copy(dt)
  adjust <- adjust[cv_Y == 1 & nid %in% adjust_nids]
  return(list(crosswalk, adjust))
}

prep_data_denom <- function(raw_dt){
  dt <- copy(raw_dt)
  dt <- unique(dt[, .(cases2 = sum(cases), sample_size2 = sample_size, id_var = id_var), by = c("nid", "location_id", "sex", "year_start", "year_end", "age_start", "age_end")])
  dt[, mean2 := cases2/sample_size2]
  z <- qnorm(0.975)
  dt[, standard_error2 := sqrt(mean2*(1-mean2)/sample_size2 + z^2/(4*sample_size2^2))]
  return(dt)
}

prep_data_comb <- function(raw_dt, denom_dt) {
  prep_dt <- copy(raw_dt)
  prep_dt <- prep_dt[cv_Y == 1, .(nid,location_id, sex, mean, cases, sample_size, standard_error, age_start, age_end, year_start, year_end, id_var)]
  denom_dt <- copy(denom_dt)
  comb_dt <- merge(prep_dt, denom_dt, by = c("nid", "location_id", "sex", "year_start", "year_end", "age_start", "age_end", "id_var"))
  return(comb_dt)
}

# GET NASH PROPORTION FROM CRYPTOGENIC
make_nash_estimates <- function(vetting, epidb,  original_data) {
  original <- copy(original_data)
  cryptogenic_keep <- copy(epidb)
  adjusted_dt <- copy(vetting)
  adjusted_dt[, cases_adj := mean_adj * sample_size]
  
  # Get seqs and NIDs that were adjusted to figure out corresponding NASH
  # Convert mean to mean_adj to adjust NASH values 
  y_id_var <- unique(adjusted_dt$id_var)
  y_nids <- unique(adjusted_dt$nid)
  keep_cols <- c("mean_adj", "standard_error_adj", "cases_adj", "mean", "standard_error", "cases",
                 "nid", "sex", "location_id", "age_start", "age_end", "year_start", "year_end", "site_memo")
  adjusted_dt <- adjusted_dt[, ..keep_cols]
  merge_cols <- c("nid", "sex", "location_id", "age_start", "age_end", "year_start", "year_end", "site_memo")
  
  to_adjust <- copy(original[case_name == "N" & nid %in% y_nids])
  to_adjust[, c("cases", "mean", "standard_error") := NULL]
  dt <- merge(to_adjust, adjusted_dt, by = merge_cols, all.y = T)
  n_id_var <- unique(dt$id_var)
  
  # Subtract adjusted cryptogenic cases and assign to NASH 
  dt[, cases_nash := cases - cases_adj]
  dt[, se_adj := sqrt(standard_error^2 + standard_error_adj^2)]
  dt <- unique(dt)
  dt$crosswalk_parent_seq <- dt$seq
  dt$mean_value <- NA
  dt[, `:=` (lower = NA, upper = NA, uncertainty_type_value = NA, 
             seq = NA, bundle_id = 6704, case_name = "N")]
  dt[, `:=` (mean_adj = NULL, mean = NULL, standard_error = NULL, standard_error_adj = NULL, cases_adj = NULL, cases = NULL) ]
  dt[, `:=` (note_modeler = "crosswalked cryptogenic estimates and subtracted adjusted from original for NASH estimates") ]
  dt$nash_crosswalk <- 1
  setnames(dt, c("cases_nash", "se_adj", "mean_value"), c("cases", "standard_error", "mean"))
  
  # Create full adjusted dataset 
  original1 <- original[!(id_var %in% y_id_var)] # drop seqs that are cryptogenic adjusted 
  original2 <- original1[!(id_var %in% n_id_var)] # drop seqs that are nash adjusted
  
  full_dt <- rbind(original2, cryptogenic_keep, dt, fill = TRUE)
  return(full_dt)
}

collapse_other <- function(collapsed) {
  collapsed <- get_cases_sample_size(collapsed)
  collapsed <- get_se(collapsed)
  collapsed[, `:=` (cases = sum(cases), standard_error = sqrt(sum(standard_error^2)),
                    note_modeler = paste0(note_modeler, "| cases sum of other and cryptogenic after crosswalk, se adjusted from other and crypto")),
            by = list(nid, location_id, sex, age_start, age_end, year_start, year_end , sample_size)]
  collapsed[, `:=` (mean = NA, lower = NA, upper = NA, uncertainty_type_value = NA)]
  collapsed <- collapsed[case_name_specific == "O", ]
  collapsed <- unique(collapsed)
} 



# CIRRHOSIS FUNCTIONS ----------------
fix_mkscn_cvs <- function(raw_dt, outlier) {
  dt <- copy(raw_dt)
  dt <- dt[grepl("MarketScan", field_citation_value) & grepl("2000", field_citation_value), cv_ms2000 :=1 ]
  dt <- dt[grepl("MarketScan", field_citation_value) & !grepl("2000", field_citation_value), cv_marketscan := 1]
  dt <- dt[grepl("HCUP", field_citation_value), cv_hospital := 1]
  # cvs <- names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop]
  # for (cov in cvs) {
  #   dt[is.na(get(cov)), paste(cov) := 0]
  # }
  return(dt)
}

append_pdf <- function(dir, starts_with) {
  files <- list.files(dir, pattern = paste0("^", starts_with), full.names = T)
  files <- paste(files, collapse = " ")
  cmd <- FILEPATH
  system(cmd)
}

