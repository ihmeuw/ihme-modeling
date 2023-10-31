# Hearing data processing functions ----------

age_sex_split <- function(raw_dt){
  df_split <- copy(raw_dt)
  cvs <- "thresh"
  df_split[, split := length(specificity[specificity == "age,sex"])]
  df_split <- df_split[specificity %in% c("age", "sex") & split == 0,]
  
  ## CALC CASES MALE/FEMALE
  df_split[, cases_total:= sum(cases), by = c("nid", "group", "specificity", "measure", "location_id", "thresh", cvs)] 
  df_split[, prop_cases := cases / cases_total]
  
  ## CALC PROP MALE/FEMALE
  df_split[, ss_total:= sum(sample_size), by = c("nid", "group", "specificity", "measure", "location_id", "thresh", cvs)] #identifiers
  df_split[, prop_ss := sample_size / ss_total]
  
  ## CALC SE
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
  
  ## CALC MEANS
  age.sex[, standard_error := sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2)]
  age.sex[, mean_orig:= mean] #added this line for plotting purposes
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
  parent <- merge(age.sex.m, raw_dt, by= c("nid","group", "measure", "location_id", "year_start", "year_end", cvs)) #identifiers, same as above
  parent[specificity == "age" | specificity == "sex", group_review:=0]
  parent <- parent[specificity %in% c("age", "sex")]
  parent[, note_modeler := paste0(note_modeler, " | parent data, has been age-sex split")]
  
  ## FINAL DATA
  original <- raw_dt[!seq %in% parent$seq]
  total <- rbind(original, age.sex, fill = T) ## DON'T RETURN PARENTS
  
  # add a check for unique number of NIDs pre- and post- split
  ## MESSAGE AND RETURN
  if (nrow(parent) == 0) print(paste0("nothing to age-sex split"))
  else print(paste0("split ", length(parent[, unique(nid)]), " nids"))
  return(total)
}
sex_split_data <- function(dt, model, newdata){
  dt[ ,crosswalk_parent_seq := NA]   
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
                           year_ids = tosplit_dt[, unique(midyear)], location_ids = tosplit_dt[, unique(location_id)], sex_ids = 1:3)
  pops[age_group_years_end == 125, age_group_years_end := 99] 
  tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt, pop_dt = pops), mc.cores = 9)) 
  tosplit_dt <- tosplit_dt[!is.na(both_N)] 
  tosplit_dt[, merge := 1]
  pred_draws[, merge := 1]
  split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
  split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
  split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]  ###averaging draws for males and females, mean and SE
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]  
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
  split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
  split_dt[ , crosswalk_parent_seq := seq]
  split_dt[!is.na(crosswalk_parent_seq), seq := NA] 
  male_dt <- copy(split_dt)
  male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = "", lower = "",
                  cases = "", sample_size = "", uncertainty_type_value = "", sex = "Male",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                        ratio_se, ")"))]
  male_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  male_dt <- dplyr::select(male_dt, names(dt))
  female_dt <- copy(split_dt)
  female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = "", lower = "",
                    cases = "", sample_size = "", uncertainty_type_value = "", sex = "Female",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",    ###splitting into male and female dts
                                          ratio_se, ")"))]
  female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  female_dt <- dplyr::select(female_dt, names(dt))
  total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))
  return(list(final = total_dt, graph = split_dt))
} 
graph_predictions <- function(dt, ref_thresh){
  graph_dt <- copy(dt[measure == "prevalence", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])    #graphing prevalence by male/female means and standard error
  graph_dt <- graph_dt[!is.na(mean),] 
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
  graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
  graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"), allow.cartesian = TRUE)
  graph_dt[, N := (mean*(1-mean)/error^2)]
  graph_dt <-graph_dt[!is.nan(N),] 
  wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))  
  graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  graph_dt[, midage := (age_end + age_start)/2]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper)) +
    labs(x = "Both Sex Mean", y = "Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle(paste0(ref_thresh, " Sex-Split Means Compared to Both-Sex Mean")) +
    scale_color_manual(name = "Sex", values = c("Male" = "purple", "Female" = "orange")) +
    theme_classic()
  return(list(plot = gg_sex, dt = graph_dt))
}
get_se <- function(raw_dt){                                                                      
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)     
}
calculate_cases_fromse <- function(raw_dt){                                                  
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size), sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt) 
}
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]                                                  
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]                         
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt) 
}
split_data_from_dir <- function(dt, model){
  dt[age_end< 1, age_end_original:= age_end]
  tosplit_dt <- copy(dt)
  tosplit_dt[age_end<1, age_end:= 1] 
  nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female") | (sex == "Both" & group_review == 0)]
  tosplit_dt <- tosplit_dt[sex == "Both" & (group_review == 1 | is.na(group_review))]
  tosplit_dt[, midyear := floor((year_start + year_end)/2)]
  preds <- read.csv(paste0(model, "model_draws.csv"))
  pred_draws <- as.data.table(preds)
  pred_draws[, c("X_intercept", "Z_intercept") := NULL]
  draws <- grep("draw", names(pred_draws), value = T)
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
  total_dt[!is.na(age_end_original), age_end:= age_end_original] 
  total_dt[, age_end_original:= NULL] 
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