
# FUNCTIONS ---------------------------------------------------------------

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
    #facet_wrap(~age_group) +
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
  
  if (!file.exists(paste0("FILEPATH"))) {
    stop(paste0("No model outputs found at '", wd, "'"))
  }
  
  contvars_string <- ifelse(
    !is.null(continuous_vars), paste0("--continuous_variables ", contvars), "" )
  dosevars_string <- ifelse(
    !is.null(dose_vars), paste0("--dose_variable ", dosevars), "" )
  
  cmd <- paste(
    c("export PATH='FILEPATH",
      "source FILEPATH",
      paste(
        "python FILEPATH",
        "--mr_dir", wd,
        contvars_string,
        dosevars_string
      )
    ), collapse = " && "
  )
  
  print(cmd)
  cat("To generate plots, run the following command in a qlogin session:")
  cat("FILEPATH", cmd, "FILEPATH")
  cat("Outputs will be available in:", wd)
  
  path <- paste0("FILEPATH")
  readr::write_file(paste("FILEPATH"))
  
  #Job specifications
  username <- Sys.getenv("USERNAME")
  m_mem_free <- "ID"
  fthread <- "ID"
  runtime_flag <- "ID"
  jdrive_flag <- "ID"
  queue_flag <- "ID"
  shell_script <- ADDRESS
  script <- path
  errors_flag <- paste0("FILEPATH", Sys.getenv("USERNAME"), "/errors")
  outputs_flag <- paste0("FILEPATH", Sys.getenv("USERNAME"), "/output")
  job_text <- strsplit(wd, "/")
  job_name <- paste0("-N funnel_",job_text[[1]][7] )
  
  job <- paste("qsub", m_mem_free, fthread, runtime_flag,
               jdrive_flag, queue_flag,
               job_name, "PROJECT", outputs_flag, errors_flag, shell_script, script)
  
  system(job)
  
}

# RUN SEX SPLIT -----------------------------------------------------------

park_sex_dt <- copy(dt)
park_sex_dt <- park_sex_dt[measure %in% c("prevalence", "incidence")]
park_sex_dt <- get_cases_sample_size(park_sex_dt)
park_sex_dt <- get_se(park_sex_dt)
park_sex_dt <- calculate_cases_fromse(park_sex_dt)
park_sex_matches <- find_sex_match(park_sex_dt, measure_vars = c("prevalence", "incidence"))
message("calculating ratios")
mrbrt_sex_dt <- calc_sex_ratios(park_sex_matches)
park_sex_dt <- age_sex_split(park_sex_dt) 

model_name <- paste0("park_sexsplit_", date)

if (file.exists(paste0("FILEPATH"))){
  sex_model <- readr::read_rds(paste0("FILEPATH"))
} else {
  sex_model <- run_mr_brt(
    output_dir = mrbrt_dir,
    model_label = paste0(model_name),
    data = mrbrt_sex_dt,
    mean_var = "log_ratio",
    se_var = "log_se",
    study_id = "id",
    method = "trim_maxL",
    trim_pct = 0.4
  )
  readr::write_rds(sex_model, paste0("FILEPATH"))
  plot_mr_brt_custom(sex_model)
}

# SPLIT DATA --------------------------------------------------------------

message("predicting new values")
predict_sex <- split_data(park_sex_dt, sex_model)

predict_graph <- graph_predictions(predict_sex$graph)
ggsave(predict_graph, filename = paste0("FILEPATH"))


# MTSTANDARD DATA ---------------------------------------------------------

message("running mtstandard risk ratio")
mtstandard_dt <- copy(dt)
mtstandard_dt <- mtstandard_dt[measure == "mtstandard"]
mtstandard_matches <- find_sex_match(mtstandard_dt, measure_vars = "mtstandard")
mrbrt_mtstandard_dt <- calc_sex_ratios(mtstandard_matches)

mtstandard_name <- paste0("parkinson_mtstandard_", date)

if (file.exists(paste0("FILEPATH"))){
  sex_mt_model <- readr::read_rds(paste0("FILEPATH"))
} else {
  sex_mt_model <- run_mr_brt(
    output_dir = "FILEPATH",
    model_label = paste0(mtstandard_name),
    data = mrbrt_mtstandard_dt,
    mean_var = "log_ratio",
    se_var = "log_se",
    study_id = "id",
    method = "remL"
  )
  readr::write_rds(sex_mt_model, paste0("FILEPATH"))
  plot_mr_brt_custom(sex_mt_model)
}

predict_mt <- split_data(mtstandard_dt, sex_mt_model, rr = T)

predict_mt_graph <- graph_predictions(predict_mt$graph, rr = T)
ggsave(predict_mt_graph, filename = paste0("FILEPATH"))

# SPLIT BY REPLICATION ----------------------------------------------------

splitrep <- copy(dt)
splitrep <- splitrep[measure %in% c("mtwith", "relrisk")]
splitrepf <- copy(splitrep)
splitrepf[, sex := "Female"]
splitrepm <- copy(splitrep)
splitrepm[, sex := "Male"]
splitrep <- rbind(splitrepm, splitrepf)
splitrep[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
splitrep[, seq := NA]