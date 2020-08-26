# FUNCTIONS ---------------------------------------------------------------
library(mortdb, lib = "FILEPATH")
find_sex_match <- function(dt, measure_vars){
  cvs<-names(dt)[names(dt)%like%"cv" & !(names(dt)%in%cv_drop)]
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% measure_vars]
  match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end",
                  names(sex_dt)[grepl("^cv_", names(sex_dt)) & !names(sex_dt) %in% cv_drop])
  sex_dt[, match_n := .N, by = match_vars]
  sex_dt <- sex_dt[match_n == 2]
  keep_vars <- c(match_vars, "sex", "mean", "standard_error")
  sex_dt[, id := .GRP, by = match_vars]
  sex_dt <- dplyr::select(sex_dt, keep_vars)
  sex_dt <- unique(sex_dt, by = c(cvs, "nid", "location_id", "age_start", "age_end", "measure", "year_start", "year_end", "sex"))
  sex_dt <- dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"))
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0]
  sex_dt[, id := .GRP, by = c("nid", "location_id")]
  return(sex_dt)
}

calc_sex_ratios <- function(dt){
  ratio_dt <- copy(dt)
  ratio_dt[, midage := (age_start + age_end)/2]
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

get_row <- function(n, dt, pop_dt){
  row_dt <- copy(dt)
  row <- row_dt[n]
  pops_sub <- pop_dt[location_id == row[, location_id] & year_id == row[, midyear] &
                       age_group_years_start >= row[, age_start] & age_group_years_end <= row[, age_end]]
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex")]
  row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
              both_N = agg[sex == "Both", pop_sum])]
  return(row)
}

sex_split_data <- function(dt, model, rr = F){
  tosplit_dt <- copy(dt)
  nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female")]
  tosplit_dt <- tosplit_dt[sex == "Both"]
  tosplit_dt[, midyear := floor((year_start + year_end)/2)]
  tosplit_dt[, midage := floor((age_start + age_end)/2)]
  tosplit_dt[, youth := ifelse(midage<20, 1, 0)]
  preds <- predict_mr_brt(model, newdata = data.table(X_intercept = 1, Z_intercept = 1, youth = c(0,1)), write_draws = T)
  pred_draws <- as.data.table(preds$model_draws)
  pred_draws[, c("X_intercept", "Z_intercept") := NULL]
  setnames(pred_draws, "X_youth", "youth")
  pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
  pred_draws <- pred_draws[, ratio_mean:=round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)]
  pred_draws <- pred_draws[, ratio_se:=round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)]
  pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                           year_ids = tosplit_dt[, unique(midyear)], location_ids = tosplit_dt[, unique(location_id)])
  pops[age_group_years_end == 125, age_group_years_end := 99]
  tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt, pop_dt = pops), mc.cores = 9))
  tosplit_dt <- tosplit_dt[!is.na(both_N)] 
  tosplit_dt[, merge := 1]
  pred_draws[, merge := 1]
  split_dt <- merge(tosplit_dt, pred_draws, by = c("merge", "youth"), allow.cartesian = T)
  split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
  split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
  split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
  if (rr == F){
    z <- qnorm(0.975)
    split_dt[mean == 0, sample_sizem := sample_size * male_N/both_N]
    split_dt[mean == 0 & measure == "prevalence", male_standard_error := sqrt(mean*(1-mean)/sample_sizem + z^2/(4*sample_sizem^2))]
    split_dt[mean == 0 & measure == "incidence", male_standard_error := ((5-mean*sample_sizem)/sample_sizem+mean*sample_sizem*sqrt(5/sample_sizem^2))/5]
    split_dt[mean == 0, sample_sizef := sample_size * female_N/both_N]
    split_dt[mean == 0 & measure == "prevalence", female_standard_error := sqrt(mean*(1-mean)/sample_sizef + z^2/(4*sample_sizef^2))]
    split_dt[mean == 0 & measure == "incidence", female_standard_error := ((5-mean*sample_sizef)/sample_sizef+mean*sample_sizef*sqrt(5/sample_sizef^2))/5]
    split_dt[, c("sample_sizem", "sample_sizef") := NULL]
  }
  male_dt <- copy(split_dt)
  male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = "", lower = "", crosswalk_parent_seq = seq,
                  cases = "", sample_size = "", uncertainty_type_value = "", sex = "Male", uncertainty_type = "",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                        ratio_se, ")"))]
  male_dt[, seq := NA]
  male_dt[specificity == "age", specificity := "age,sex"][specificity %in% c("total", "", NA), specificity := "sex"]
  male_dt[is.na(group) & is.na(group_review), `:=` (group_review = 1, group = 1)]
  male_dt <- dplyr::select(male_dt, c(names(dt), "crosswalk_parent_seq"))
  female_dt <- copy(split_dt)
  female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = "", lower = "", uncertainty_type = "",
                    cases = "", sample_size = "", uncertainty_type_value = "", sex = "Female", crosswalk_parent_seq = seq,
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                          ratio_se, ")"))]
  female_dt[, seq := NA]
  female_dt[specificity == "age", specificity := "age,sex"][specificity %in% c("total", "", NA), specificity := "sex"]
  female_dt[is.na(group) & is.na(group_review), `:=` (group_review = 1, group = 1)]
  female_dt <- dplyr::select(female_dt, c(names(dt), "crosswalk_parent_seq"))
  total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt), fill = T)
  return(list(final = total_dt, graph = split_dt))
}

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
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

## CALCULATE CASES FROM STANDARD ERROR
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

graph_predictions <- function(dt, rr = F){
    graph_dt <- copy(dt[measure == "prevalence", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error, nid, location_id)])
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean", "nid", "location_id"), measure.vars = c("male_mean", "female_mean"))
  graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean", "nid", "location_id"), measure.vars = c("male_standard_error", "female_standard_error"))
  graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable", "nid", "location_id"))
    graph_dt[, N := (mean*(1-mean)/error^2)]
    wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
    graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  graph_dt[, midage := (age_end + age_start)/2]
  ages <- c(10, 20, 30, 40, 50, 60, 70, 80, 90)
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

plot_mr_brt_custom <- function(model_object, continuous_vars = NULL, dose_vars = NULL, print_cmd = FALSE) {

  library("ihme", lib.loc = "FILEPATH")

  wd <- model_object[["working_dir"]]

  contvars <- paste(continuous_vars, collapse = " ")
  dosevars <- paste(dose_vars, collapse = " ")

  if (!file.exists(paste0(wd, "model_coefs.csv"))) {
    stop(paste0("No model outputs found at '", wd, "'"))
  }

  contvars_string <- ifelse(
    !is.null(continuous_vars), paste0("--continuous_variables ", contvars), "" )
  dosevars_string <- ifelse(
    !is.null(dose_vars), paste0("--dose_variable ", dosevars), "" )

  cmd <- paste(
    c("export PATH='FILEPATH'",
      "source 'FILEPATH'",
      paste(
        "python 'FILEPATH'",
        "--mr_dir", wd,
        contvars_string,
        dosevars_string
      )
    ), collapse = " && "
  )

  print(cmd)
  cat("To generate plots, run the following command in a qlogin session:")
  cat("\n\n", cmd, "\n\n")
  cat("Outputs will be available in:", wd)

  path <- paste0(wd, "tmp.txt")
  readr::write_file(paste("/bin/bash -c", cmd), path)

  #Job specifications
  username <- "USERNAME"
  m_mem_free <- "-l m_mem_free=2G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=00:05:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q all.q"
  shell_script <- path
  script <- path
  errors_flag <- paste0("-e 'FILEPATH'")
  outputs_flag <- paste0("-o 'FILEPATH'")
  job_text <- strsplit(wd, "/")
  job_name <- paste0("-N funnel_",job_text[[1]][7] )

  job <- paste("qsub", m_mem_free, fthread, runtime_flag,
               jdrive_flag, queue_flag,
               job_name, "USERNAME", outputs_flag, errors_flag, shell_script, script)

  system(job)

}

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

subnat_to_nat <- function(subnat_dt){
  dt <- copy(subnat_dt)
  dt[, ihme_loc_id := NULL]
  dt <- merge(dt, loc_dt[, .(location_id, level, parent_id, location_type, ihme_loc_id)], by = "location_id")
  dt[, loc_match := location_id]
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

## HELPER FUNCTION TO ROUND TO NEAREST AGE START/AGE END IN GBD
get_closest_age <- function(i, var, start = T, dt){
  age <- dt[i, get(var)]
  if (start == T){
    age_dt[, age_group_years_start][which.min(abs(age_dt[, age_group_years_start] - age))]
  } else if (start == F){
    age_dt[, age_group_years_end][which.min(abs(age_dt[, age_group_years_end] - age))]
  }
}

## AGGREGATING
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
  matched <- matched[sex == sex2 & measure == measure2 & year_match >=year_match2-year_span/2 &
                       year_match <= year_match2 + year_span/2] ## FILETER OUT SEX, MEASURE, YEAR
  if (pair[1] == "_cv_marketscan" | pair[2] == "_cv_marketscan"){ ## EXACT LOCS FOR MARKETSCAN BUT CURRENTLY ALL ARE EXACT LOCS
    matched <- matched[location_id == location_id2]
  }
  matched[, age_match := (age_start >= age_start2 - age_span/2 & age_start <= age_start2 + age_span/2) &
            (age_end >= age_end2 - age_span/2 & age_end <= age_end2 + age_span/2)]
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
    dt <- dt[, c("nid", "nid2","id", "ldiff", "ldiff_se", "def", "def2", vars), with = F]
  } else if (age == T){
    dt <- dt[, c("nid", "nid2", "id", "age", "ldiff", "ldiff_se", "def", "def2", vars), with = F]
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
  data<-data[, w:=round(w)]
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
  ages <- c(30, 50, 70)
  dt[, age_group := cut2(midage, ages)]
  gg_funct <- function(graph_dt){
    gg <- ggplot(graph_dt, aes(x = mean, y = mean_adj, color = as.factor(super_region_name))) +
      geom_point() +
      geom_abline() +
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

summaries <- function(dt, draw_vars){
  sum <- copy(dt)
  sum[, mean := rowMeans(.SD), .SDcols = draw_vars]
  sum[, lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draw_vars]
  sum[, upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draw_vars]
  sum[, c(draw_vars) := NULL]
  return(sum)
}

## MAKE SURE DATA IS FORMATTED CORRECTLY
format_data <- function(unformatted_dt, sex_dt){
  dt <- copy(unformatted_dt)
  dt[, `:=` (mean = as.numeric(mean), sample_size = as.numeric(sample_size), cases = as.numeric(cases),
             age_start = as.numeric(age_start), age_end = as.numeric(age_end), year_start = as.numeric(year_start))]
  dt <- dt[measure %in% c("prevalence", "incidence"),]
  dt <- dt[(age_end-age_start)>25,]
  dt <- dt[!mean == 0, ] ##don't split points with zero prevalence
  dt <- merge(dt, sex_dt, by = "sex")
  dt[measure == "prevalence", measure_id := 5]
  dt[measure == "incidence", measure_id := 6]
  dt[, year_id := round((year_start + year_end)/2, 0)] ##so that can merge on year later
  return(dt)
}

## CREATE NEW AGE ROWS
expand_age <- function(small_dt, age_dt = ages){
  dt <- copy(small_dt)

  ## ROUND AGE GROUPS
  dt[, age_start := age_start - age_start %%5]
  dt[, age_end := age_end - age_end %%5 + 4]
  dt <- dt[age_end > 99, age_end := 99]

  ## EXPAND FOR AGE
  dt[, n.age:=(age_end+1 - age_start)/5]
  dt[, age_start_floor:=age_start]
  expanded <- rep(dt$id, dt$n.age) %>% data.table("id" = .)
  split <- merge(expanded, dt, by="id", all=T)
  split[, age.rep := 1:.N - 1, by =.(id)]
  split[, age_start:= age_start+age.rep*5]
  split[, age_end :=  age_start + 4]
  split <- merge(split, age_dt, by = c("age_start", "age_end"), all.x = T)
  split[age_start == 0 & age_end == 4, age_group_id := 1]
  split <- split[age_group_id %in% age | age_group_id == 1] ##don't keep where age group id isn't estimated for cause
  return(split)
}

## GET DISMOD AGE PATTERN
get_age_pattern <- function(locs, id, age_groups, vid){
  age_pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = id, ## USING 2010 AGE PATTERN BECAUSE LIKELY HAVE MORE DATA FOR 2010
                           measure_id = c(5, 6), location_id = locs, source = "epi",
                           version_id = vid, sex_id = c(1,2), gbd_round_id = 6, decomp_step = "step2",
                           age_group_id = age_groups, year_id = 2010) ##imposing age pattern
  if (vid == 399752) {
    age_pattern_temp<-age_pattern[age_group_id == 20]
    age_pattern<-age_pattern[!(age_group_id%in%c(30, 31, 32, 235))]
    age_pattern_temp<-age_pattern_temp[, age_group_id:=30]
    age_pattern<-rbind(age_pattern, age_pattern_temp)
    age_pattern_temp<-age_pattern_temp[, age_group_id:=31]
    age_pattern<-rbind(age_pattern, age_pattern_temp)
    age_pattern_temp<-age_pattern_temp[, age_group_id:=32]
    age_pattern<-rbind(age_pattern, age_pattern_temp)
    age_pattern_temp<-age_pattern_temp[, age_group_id:=235]
    age_pattern<-rbind(age_pattern, age_pattern_temp)
  }
  us_population <- get_population(location_id = locs, year_id = 2010, sex_id = c(1, 2),
                                  age_group_id = age_groups, decomp_step = "step1", gbd_round_id = 6)
  us_population <- us_population[, .(age_group_id, sex_id, population, location_id)]
  age_pattern[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
  age_pattern[, rate_dis := rowMeans(.SD), .SDcols = draws]
  age_pattern[, (draws) := NULL]
  age_pattern <- age_pattern[ ,.(sex_id, measure_id, age_group_id, location_id, se_dismod, rate_dis)]

  ## AGE GROUP 1 (SUM POPULATION WEIGHTED RATES)
  age_1 <- copy(age_pattern)
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5), ]
  se <- copy(age_1)
  se <- se[age_group_id==5, .(measure_id, sex_id, se_dismod, location_id)] 
  age_1 <- merge(age_1, us_population, by = c("age_group_id", "sex_id", "location_id"))
  age_1[, total_pop := sum(population), by = c("sex_id", "measure_id", "location_id")]
  age_1[, frac_pop := population / total_pop]
  age_1[, weight_rate := rate_dis * frac_pop]
  age_1[, rate_dis := sum(weight_rate), by = c("sex_id", "measure_id", "location_id")]
  age_1 <- unique(age_1, by = c("sex_id", "measure_id", "location_id"))
  age_1 <- age_1[, .(age_group_id, sex_id, measure_id, location_id, rate_dis)]
  age_1 <- merge(age_1, se, by = c("sex_id", "measure_id", "location_id"))
  age_1[, age_group_id := 1]
  age_pattern <- age_pattern[!age_group_id %in% c(2,3,4,5)]
  age_pattern <- rbind(age_pattern, age_1)

  ## CASES AND SAMPLE SIZE
  age_pattern[measure_id == 5, sample_size_us := rate_dis * (1-rate_dis)/se_dismod^2]
  age_pattern[measure_id == 6, sample_size_us := rate_dis/se_dismod^2]
  age_pattern[, cases_us := sample_size_us * rate_dis]
  age_pattern[is.nan(sample_size_us), sample_size_us := 0] ##if all draws are 0 can't calculate cases and sample size b/c se = 0, but should both be 0
  age_pattern[is.nan(cases_us), cases_us := 0]

  ## GET SEX ID 3
  sex_3 <- copy(age_pattern)
  sex_3[, cases_us := sum(cases_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, sample_size_us := sum(sample_size_us), by = c("age_group_id", "measure_id", "location_id")]
  sex_3[, rate_dis := cases_us/sample_size_us]
  sex_3[measure_id == 5, se_dismod := sqrt(rate_dis*(1-rate_dis)/sample_size_us)] ##back calculate cases and sample size
  sex_3[measure_id == 6, se_dismod := sqrt(cases_us)/sample_size_us]
  sex_3[is.nan(rate_dis), rate_dis := 0] ##if sample_size is 0 can't calculate rate and standard error, but should both be 0
  sex_3[is.nan(se_dismod), se_dismod := 0]
  sex_3 <- unique(sex_3, by = c("age_group_id", "measure_id", "location_id"))
  sex_3[, sex_id := 3]
  age_pattern <- rbind(age_pattern, sex_3)

  age_pattern[, super_region_id := location_id]
  age_pattern <- age_pattern[ ,.(age_group_id, sex_id, measure_id, cases_us, sample_size_us, rate_dis, se_dismod, super_region_id)]
  return(age_pattern)
}

## GET POPULATION STRUCTURE
get_pop_structure <- function(locs, years, age_groups){
  populations <- get_population(location_id = locs, year_id = years,decomp_step = "step1",
                                sex_id = c(1, 2, 3), age_group_id = age_groups, gbd_round_id = 6)
  age_1 <- copy(populations) ##create age group id 1 by collapsing lower age groups
  age_1 <- age_1[age_group_id %in% c(2, 3, 4, 5)]
  age_1[, population := sum(population), by = c("location_id", "year_id", "sex_id")]
  age_1 <- unique(age_1, by = c("location_id", "year_id", "sex_id"))
  age_1[, age_group_id := 1]
  populations <- populations[!age_group_id %in% c(2, 3, 4, 5)]
  populations <- rbind(populations, age_1)  ##add age group id 1 back on
  return(populations)
}

## ACTUALLY SPLIT THE DATA
split_data <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[, total_pop := sum(population), by = "id"]
  dt[, sample_size := (population / total_pop) * sample_size]
  dt[, cases_dis := sample_size * rate_dis]
  dt[, total_cases_dis := sum(cases_dis), by = "id"]
  dt[, total_sample_size := sum(sample_size), by = "id"]
  dt[, all_age_rate := total_cases_dis/total_sample_size]
  dt[, ratio := mean / all_age_rate]
  dt[, mean := ratio * rate_dis]
  dt[, cases := mean * sample_size]
  return(dt)
}

## FORMAT DATA TO FINISH
format_data_forfinal <- function(unformatted_dt, location_split_id, region, original_dt){
  dt <- copy(unformatted_dt)
  dt[, group := 1]
  dt[, specificity := "age,sex"]
  dt[, group_review := 1]
  dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  blank_vars <- c("lower", "upper", "effective_sample_size", "standard_error", "uncertainty_type", "uncertainty_type_value", "seq")
  dt[, (blank_vars) := NA]
  dt <- get_se(dt)
  #dt <- col_order(dt)
  if (region ==T) {
    dt[, note_modeler := paste0(note_modeler, "| age split using the super region age pattern", date)]
  } else {
    dt[, note_modeler := paste0(note_modeler, "| age split using the age pattern from location id ", location_split_id, " ", date)]
  }
  split_ids <- dt[, unique(id)]
  dt <- rbind(original_dt[!id %in% split_ids], dt, fill = T)
  dt <- dt[, c(names(df)), with = F]
  return(dt)
}

age_split <- function(gbd_id, df, age, region_pattern, location_pattern_id, vid, gbd_round_id = 6){

  ## GET TABLES
  sex_names <- get_ids(table = "sex")
  ages <- get_age_metadata(12, gbd_round_id = gbd_round_id)
  setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
  ages[, age_group_weight_value := NULL]
  ages[age_start >= 1, age_end := age_end - 1]
  ages[age_end == 124, age_end := 99]
  super_region_dt <- get_location_metadata(location_set_id = 22)
  super_region_dt <- super_region_dt[, .(location_id, super_region_id)]

  ## SAVE ORIGINAL DATA (DOESN'T REQUIRE ALL DATA TO HAVE SEQS)
  original <- copy(df)
  original[, id := 1:.N]

  ## FORMAT DATA
  dt <- format_data(original, sex_dt = sex_names)
  dt <- get_cases_sample_size(dt)
  dt <- get_se(dt)
  dt <- calculate_cases_fromse(dt)

  ## EXPAND AGE
  split_dt <- expand_age(dt, age_dt = ages)

  ## GET PULL LOCATIONS
  if (region_pattern == T){
    split_dt <- merge(split_dt, super_region_dt, by = "location_id")
    super_regions <- unique(split_dt$super_region_id) ##get super regions for dismod results
    locations <- super_regions
  } else {
    locations <- location_pattern_id
  }

  ##GET LOCS AND POPS
  pop_locs <- unique(split_dt$location_id)
  pop_years <- unique(split_dt$year_id)

  ## GET AGE PATTERN
  print("getting age pattern")
  age_pattern <- get_age_pattern(locs = locations, id = gbd_id, age_groups = age, vid = vid)

  if (region_pattern == T) {
    age_pattern1 <- copy(age_pattern)
    split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
  } else {
    age_pattern1 <- copy(age_pattern)
    split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
  }

  ## GET POPULATION INFO
  print("getting pop structure")
  pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
  split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))

  #####CALCULATE AGE SPLIT POINTS#######################################################################
  ## CREATE NEW POINTS
  print("splitting data")
  split_dt <- split_data(split_dt)
  ######################################################################################################

  final_dt <- format_data_forfinal(split_dt, location_split_id = location_pattern_id, region = region_pattern,
                                   original_dt = original)

  ## BREAK IF NO ROWS
  if (nrow(final_dt) == 0){
    print("nothing in bundle to age-sex split")
    break
  }
  return(final_dt)
}
