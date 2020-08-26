##########################################################################
### This script sex splits both sex data
##########################################################################

pacman::p_load(data.table, openxlsx, ggplot2)
library(msm, lib.loc = "FILEPATH")
library(Hmisc, lib.loc = "FILEPATH")
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------
cause_name = "CD"
dem_dir <-"FILEPATH"
functions_dir <- "FILEPATH"
cv_drop <- c("cv_diag_exam", "cv_outpt_usa", "cv_outpt_swe", "cv_marketscan_data",  "cv_literature", "cv_marketscan_inp_2000",  "cv_marketscan_all_2000",  "cv_marketscan_all_2010",  "cv_marketscan_inp_2010", "cv_marketscan_inp_2012", "cv_marketscan_all_2012",  "cv_survey", "cv_marketscan_all_2011", "cv_marketscan_inp_2011","cv_marketscan_all_2013", "cv_marketscan_inp_2013","cv_marketscan_all_2014", "cv_marketscan_inp_2014","cv_marketscan_all_2015", "cv_marketscan_inp_2015","cv_outpatient", "cv_inpatient")
mrbrt_helper_dir <- "FILEPATH"
mrbrt_dir <- "FILEPATH"
draws <- paste0("draw_", 0:999)


# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_location_metadata.R"))
source(paste0(functions_dir, "get_age_metadata.R"))
source(paste0(functions_dir, "get_population.R"))
library(mortdb, lib = "FILEPATH")

functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function", 
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function", 
            "load_mr_brt_preds_function")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}

# GET METADATA ------------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 22)
age_dt <- get_age_metadata(12)
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]


# ANALYTICAL FUNCTIONS ----------------------------------------------------

## REPLACE THIS FUNCTION WITH WHATEVER YOU WANT TO USE TO PULL IN DATA

get_dem_data <- function(){
  dt <- fread("FILEPATH FOR CROSSWALKD DATA")
  dt <- dt[!is_outlier == 1 & measure %in% c("prevalence", "incidence")] 
  return(dt)
}
get_split_data <- function(){
  dt <- fread(paste0("FILEPATH FOR BOTH SEX DATA"))
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
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}


## CALCULATE SEX RATIOS 
find_sex_match <- function(dt){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("prevalence", "incidence")]
  match_vars <- c("bundle_id", "location_name", "nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end",
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


# RUN SEX SPLIT -----------------------------------------------------------

dem_sex_dt <- get_dem_data()
dem_sex_dt <- get_cases_sample_size(dem_sex_dt)
dem_sex_dt <- get_se(dem_sex_dt)
dem_sex_matches <- find_sex_match(dem_sex_dt)
mrbrt_sex_dt <- calc_sex_ratios(dem_sex_matches)
mrbrt_sex_dt[, id := .GRP, by = c("nid")]

model_name <- paste0("CD_sexsplit_", date)

mrbrt_sex_dt <- subset(mrbrt_sex_dt, !is.na(log_se))

sex_model <- run_mr_brt(
  output_dir = mrbrt_dir,
  model_label = model_name,
  data = mrbrt_sex_dt,
  overwrite_previous = TRUE,
  remove_x_intercept = FALSE,
  mean_var = "log_ratio",
  se_var = "log_se",
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.10
)


sex_predictions <- predict_mr_brt(sex_model, newdata = data.table(midage = seq(42.5, 97.5, by = 5)), write_draws = T)

results <- sex_model
predicts <- sex_predictions

## SEX SPLIT DATA 
get_row <- function(n, dt){
  row <- copy(dt)[n]
  pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                          age_group_years_start >= row[, age_start] & age_group_years_end <= row[, age_end]])
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex")]
  row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
              both_N = agg[sex == "Both", pop_sum])]
  return(row)
}
sex_split_data <- get_split_data()

tosplit_dt <- copy(sex_split_data)
nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female") | (sex == "Both" & group_review == 0)]
tosplit_dt <- tosplit_dt[sex == "Both" & (group_review == 1 | is.na(group_review))]
tosplit_dt[, midyear := floor((year_start + year_end)/2)]
preds <- predict_mr_brt(sex_model, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T)
pred_draws <- as.data.table(preds$model_draws)
pred_draws[, c("X_intercept", "Z_intercept") := NULL]
pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)
pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                         year_ids = tosplit_dt[, unique(midyear)], location_ids = tosplit_dt[, unique(location_id)])

pops[age_group_years_end == 125, age_group_years_end := 99]

tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt), mc.cores = 12))
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
male_dt <- dplyr::select(male_dt, names(sex_plit_data))
female_dt <- copy(split_dt)

female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = NA, lower = NA, 
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                        ratio_se, ")"))]
female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
female_dt <- dplyr::select(female_dt, names(sex_plit_data))
total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt), fill=TRUE)

predict_sex <- list(final = total_dt, graph = split_dt)
graph_predictions <- function(dt){
  graph_dt <- copy(dt[measure == "prevalence", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
  graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
  graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"))
  graph_dt[, N := (mean*(1-mean)/error^2)]
  graph_dt$N[graph_dt$N=="NaN"] <-NA
  graph_dt <-subset(graph_dt, N!="")
  wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
  graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  graph_dt[, midage := (age_end + age_start)/2]
  ages <- c(60, 70, 80, 90)
  graph_dt[, age_group := cut2(midage, ages)]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) + 
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper)) +
    facet_wrap(~age_group) +
    labs(x = "Both Sex Mean", y = " Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Sex Split Means Compared to Both Sex Mean by Age") +
    scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "purple")) +
    theme_classic()
  return(gg_sex)
}

pdf(paste0(mrbrt_dir, cause_name, "_log_sex_split_onegraph.pdf"))
graph_predictions(predict_sex$graph)
dev.off()

write.csv(predict_sex$final, paste0(mrbrt_dir,  cause_path, "/", "log_", cause_name, "_final_sex_split.csv"), row.names = F)

