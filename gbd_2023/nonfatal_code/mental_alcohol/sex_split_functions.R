# FUNCTIONS ---------------------------------------------------------------

library(mortdb, lib = "FILEPATH")

## GET CASES FROM SAMPLE SIZE
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

## SEX MATCHING 
find_sex_match <- function(dt, measure_vars){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% measure_vars]
  match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end",
                  names(sex_dt)[grepl("^cv_", names(sex_dt)) & !names(sex_dt) %in% cv_drop])
  sex_dt[, match_n := .N, by = match_vars]
  sex_dt <- sex_dt[match_n == 2]
  keep_vars <- c(match_vars, "sex", "mean", "standard_error")
  sex_dt[, id := .GRP, by = match_vars]
  sex_dt <- dplyr::select(sex_dt, keep_vars)
  sex_dt <- unique(sex_dt, by = c("nid", "location_id", "age_start", "age_end", "measure", "year_start", "year_end", "sex"))
  sex_dt <- dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"))
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0]
  sex_dt[, id := .GRP, by = c("nid", "location_id")]
  return(sex_dt)
}

## CALCULATE SEX RATIOS 
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

## GET POPULATION  
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

## SPLIT DATA
split_data <- function(dt, model, rr = F){
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
  tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt, pop_dt = pops), mc.cores = 9))
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


## GRAPH PREDICTIONS 
graph_predictions <- function(dt, rr = F){
  if (rr == F){
    graph_dt <- copy(dt[measure == "prevalence", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])
  } else if (rr == T){
    graph_dt <- copy(dt[, .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])
  }
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
  graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
  graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"))
  if (rr == F){
    graph_dt[, N := (mean*(1-mean)/error^2)]
    wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
    graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  } else if (rr == T){
    graph_dt[, `:=` (lower = value-1.96*error, upper = value+1.96*error)]
  }
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










