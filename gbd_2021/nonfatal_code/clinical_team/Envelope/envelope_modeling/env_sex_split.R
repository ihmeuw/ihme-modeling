# SEX-SPLITTING SCRIPT FOR THE INPATIENT UTILIZATION ENVELOPE


rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2, dplyr)
library(mortdb, lib = "FILEPATH")
library(msm, lib.loc = "FILEPATH")
library(Hmisc, lib.loc = "FILEPATH")
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS ---------------------------------------------------------------------------
# cause_path <- "FILEPATH"
# cause_name <- "envelope"
# covariate_name <- "sex"

#dem_dir <- paste0(j_root, "FILEPATH", cause_path)
functions_dir <- paste0("FILEPATH")
mrbrt_helper_dir <- paste0(j_root, "FILEPATH")
mrbrt_dir <- paste0(j_root, "FILEPATH/")
draws <- paste0("draw_", 0:999)

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_location_metadata.R"))
source(paste0(functions_dir, "get_age_metadata.R"))

functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
            "load_mr_brt_preds_function")

for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}

# GET METADATA ------------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 22, gbd_round_id = 7, decomp_step="iterative")

## PULL IN DATA TO DETERMINE SPLIT - this is original bundle version data
get_dem_data <- function(){
  dt <- fread("FILEPATH")
  #dt$seq <- 1:nrow(dt)
  dt <- dt[!is_outlier == 1 & measure %in% c("continuous")] # measure should be 'continuous'
  return(dt)
}

# Sex matches - within study ratios -----------------------------------------------------

find_sex_match <- function(dt){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("continuous")]
  match_vars <- c("nid", "age_start", "age_end", "location_id", "year_start", "year_end",  "measure", 
                  names(sex_dt)[grepl("^cv_", names(sex_dt)) & !names(sex_dt) %in% cv_drop])
  sex_dt[, match_n := .N, by = match_vars]
  sex_dt <- sex_dt[match_n == 2]
  keep_vars <- c(match_vars, "sex", "mean", "standard_error")
  sex_dt[, id := .GRP, by = match_vars]
  sex_dt <- dplyr::select(sex_dt, keep_vars)
  sex_dt <- data.table::dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"), fun.aggregate=mean)
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0]
  sex_dt[, id := .GRP, by = c("nid", "location_id")]
  return(sex_dt)
}

calc_sex_ratios <- function(dt){
  ratio_dt <- copy(dt)
  ratio_dt <- copy(dem_sex_matches)
  ratio_dt[, midage := (age_start + age_end)/2]
  ratio_dt[, `:=` (ratio = mean_Female/mean_Male,
                   ratio_se = sqrt((mean_Female^2/mean_Male^2)*(standard_error_Female^2/mean_Female^2+standard_error_Male^2/mean_Male^2)))]
  ratio_dt[, log_ratio := log(ratio)]
  ratio_dt$log_se <- sapply(1:nrow(ratio_dt), function(i){
    mean_i <- ratio_dt[i, "ratio"]
    se_i <- ratio_dt[i, "ratio_se"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  return(ratio_dt)
}

# MR-BRT SPLINE GRAPH
# graph_mrbrt_spline <- function(results, predicts){
#   data_dt <- as.data.table(results$train_data)
#   model_dt <- as.data.table(predicts$model_summaries)
#   shapes <- c(15, 16, 18)
#   data_dt[w == 0, excluded := 1][w > 0, excluded := 0]
#   gg <- ggplot() +
#     geom_point(data = data_dt, aes(x = midage, y = log_ratio, color = as.factor(excluded)), size = 3) +
#     scale_fill_manual(guide = F, values = c("midnightblue", "purple")) +
#     labs(x = "Age", y = "Log Ratio") +
#     ggtitle(paste0("Meta-Analysis Results")) +
#     theme_classic() +
#     theme(text = element_text(size = 15, color = "black")) +
#     geom_smooth(data = model_dt, aes(x = X_midage, y = Y_mean), color = "black") +
#     geom_ribbon(data = model_dt, aes(x = X_midage, ymin = Y_mean_lo, ymax = Y_mean_hi), alpha = 0.05) +
#     scale_color_manual(name = "", values = c("blue", "red"),
#                        labels = c("Included", "Trimmed"))
#   return(gg)
# }


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
  dt[is.na(standard_error) & measure == "continuous" & cases < 5, standard_error := (((5-mean*sample_size)/sample_size)+((mean*sample_size*(5/sample_size^2))/5))]
  dt[is.na(standard_error) & measure == "continuous" & cases >= 5, standard_error := sqrt(cases)/sample_size]
  return(dt)
}

calculate_cases_fromes <- function(raw_dt){
  dt <- copy(raw_dt)
  dt$sample_size <- as.numeric(dt$sample_size)
  dt[is.na(cases) & is.na(sample_size) & measure == "continuous", sample_size := mean*(1-mean)/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}

## PLOT MR-BRT FUNCTION
# plot_mr_brt <- function(model_object, continuous_vars = NULL, dose_vars = NULL, print_cmd = FALSE) {
#   dev <- FALSE
#   if (dev) {
#     model_object = readRDS("data/fit1_example.RDS")
#     continuous_vars = NULL
#     dose_vars = NULL
#     print_cmd = TRUE
#   }
#   wd <- model_object[["working_dir"]]
#   contvars <- paste(continuous_vars, collapse = " ")
#   dosevars <- paste(dose_vars, collapse = " ")
#   if (!file.exists(paste0(wd, "model_coefs.csv"))) {
#     stop(paste0("No model outputs found at '", wd, "'"))
#   }
#   contvars_string <- ifelse(
#     !is.null(continuous_vars), paste0("--continuous_variables ", contvars), "" )
#   dosevars_string <- ifelse(
#     !is.null(dose_vars), paste0("--dose_variable ", dosevars), "" )
#   cmd <- paste(
#     c("export PATH='FILEPATH'",
#       "source deactivate",
#       "source FILEPATH/activate mr_brt_env",
#       paste(
#         "python FILEPATH/mr_brt_visualize.py",
#         "--mr_dir", wd,
#         contvars_string,
#         dosevars_string
#       )
#     ), collapse = " && "
#   )
#   cat("To generate plots, run the following command in a qlogin session:")
#   cat("\n", cmd, "\n\n")
#   cat("Outputs will be available in:", wd)
# }



# RUN SEX SPLIT -----------------------------------------------------------

# Get both dataframes
dem_sex_dt <- get_dem_data()

# Prep data for split
dem_sex_dt <- get_cases_sample_size(dem_sex_dt)
dem_sex_dt <- get_se(dem_sex_dt)
dem_sex_matches <- find_sex_match(dem_sex_dt)

# Remove NaNs and infs
dem_sex_matches <- subset(dem_sex_matches, standard_error_Male > 0)

# RUN MR-BRT
mrbrt_sex_dt <- calc_sex_ratios(dem_sex_matches)
 
model_name <- paste0(cause_name, "_sexsplit_", date)

sex_model <- run_mr_brt(
  output_dir = mrbrt_dir,
# The below is for including age as a covariate
  #list(cov_info("midage", "X", degree = 3,
  #i_knots = paste(mrbrt_sex_dt[, quantile(midage, prob = seq(0, 1, by = 0.2))][2:5], collapse = ", "),
  #r_linear = T, l_linear = T)),
  model_label = model_name,
  data = mrbrt_sex_dt,
  overwrite = TRUE,
  mean_var = "log_ratio",
  se_var = "log_se",
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.1
)
# 
# 
# plot_mr_brt(sex_model)

# Make predictions
#sex_predictions <- predict_mr_brt(sex_model, newdata = data.table(midage = seq(0, 110, by = 5)), write_draws = T)

get_row <- function(n, dt){
  row <- copy(dt)[n]
  pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                          age_group_years_start >= row[, age_start] & age_group_years_end <= row[, age_end]])
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex")]
  row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
              both_N = agg[sex == "Both", pop_sum])]
  return(row)
}


dt <- as.data.table(dem_sex_dt)

dt <- dt[, midage := (age_start + age_end)/2]
dt$midage <- round(dt$midage/5)*5

#Only use this line line if you ran MR-BRT model in this script
#model <- sex_model

# SPLIT THE DATA
tosplit_dt <- as.data.table(copy(dt))
nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female")] 
tosplit_dt <- tosplit_dt[sex == "Both"] 
tosplit_dt[, midyear := floor((year_start + year_end)/2)]

#Pick first and second line if ran MR-BRT model in script and no age covariate
#Run third line if pulling in pre-run MR-BRT model
#preds <- predict_mr_brt(sex_model, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T)
#pred_draws <- as.data.table(preds$model_draws)
pred_draws <- as.data.table(read.csv("FILEPATH"))

pred_draws[, c("X_intercept", "Z_intercept") := NULL]
pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)
pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                         year_ids = tosplit_dt[, unique(midyear)], location_ids = tosplit_dt[, unique(location_id)])
pops[age_group_years_end == 125, age_group_years_end := 99]
tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt), mc.cores = 9))
tosplit_dt <- tosplit_dt[!is.na(both_N)] ## GET RID OF DATA THAT COULDN'T FIND POPS 
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
male_dt[is.na(cases), cases := mean * sample_size]
male_dt <- dplyr::select(male_dt, names(dt))
female_dt <- copy(split_dt)
female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = NA, lower = NA,
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                        ratio_se, ")"))]
female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
female_dt[is.na(cases), cases := mean * sample_size]
female_dt <- dplyr::select(female_dt, names(dt))

# add crosswalk parent seq
nosplit_dt$crosswalk_parent_seq <- NA
female_dt$crosswalk_parent_seq <- female_dt$seq
male_dt$crosswalk_parent_seq <- male_dt$seq
total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))

# predict_sex <- list(final = total_dt, graph = split_dt)
# #predict_sex <- split_data(dem_sex_dt, sex_model)
# 
# 
# graph_predictions <- function(dt){
#   #Set measure to incidence, prevalence, or proportion depending on model - 'continuous' here
#   graph_dt <- copy(dt[measure == "continuous", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])
#   graph_dt <- copy(dt)
#   graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
#   graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
#   graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
#   graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
#   setnames(graph_dt_error, "value", "error")
#   graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"), allow.cartesian = TRUE)
#   graph_dt[, N := (mean*(1-mean)/error^2)]
#   graph_dt <- subset(graph_dt, value > 0)
#   wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
#   graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
#   graph_dt[, midage := (age_end + age_start)/2]
#   ages <- c(60, 70, 80, 90)
#   graph_dt[, age_group := cut2(midage, ages)]
#   gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
#     geom_point() +
#     geom_errorbar(aes(ymin = lower, ymax = upper)) +
#     # Only need this if using age as a covariate
#     #facet_wrap(~midage) + 
#     labs(x = "Both Sex Mean", y = " Sex Split Means") +
#     geom_abline(slope = 1, intercept = 0) +
#     ggtitle("Sex Split Means Compared to Both Sex Mean") + 
#     scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "purple")) +
#     theme_classic()
#   return(gg_sex)
# }
# 
# pdf(paste0(dem_dir, cause_name, "_sex_split_plot.pdf"))
# graph_predictions(predict_sex$graph)
# dev.off()

# Check for duplicates
total_dt <- total_dt %>% distinct(nid, location_id, mean, sex, year_start, age_start, age_end, .keep_all = TRUE)

#write.csv(total_dt, "FILEPATH")


