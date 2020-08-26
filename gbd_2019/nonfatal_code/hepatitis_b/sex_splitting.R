##########################################################################
### Purpose: MRBRT SEX SPLITTING GBD2019
##########################################################################
rm(list=ls())


pacman::p_load(data.table, openxlsx, ggplot2)
library(msm)
library(Hmisc)
library(mortdb, lib = "FILEPATH")

# SET OBJECTS -------------------------------------------------------------

b_id <- OBJECT
a_cause <- OBJECT
version <- OBJECT


mrbrt_dir <- DIRECTORY

mrbrt_helper_dir <- "FILEPATH"
cv_drop <- OBJECT
draws <- paste0("draw_", 0:999)

# SOURCE FUNCTIONS --------------------------------------------------------

functions_dir <- "FILEPATH"
source(paste0(functions_dir, "get_location_metadata.R"))
source(paste0(functions_dir, "get_age_metadata.R"))
source("FILEPATH/get_ids.R")
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_crosswalk_version.R")

source(paste0(functions_dir, "get_population.R"))

functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
            "load_mr_brt_preds_function", "plot_mr_brt_function")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}


# GET METADATA ------------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 22)
age_dt <- FILE_PATH
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]

# DATA PROCESSING FUNCTIONS -----------------------------------------------

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

find_sex_match <- function(dt){
  sex_dt <- copy(dt)
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("prevalence")]
  match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end",
                  names(sex_dt)[grepl("^cv_", names(sex_dt)) & !names(sex_dt) %in% cv_drop])
  sex_dt[, match_n := .N, by = match_vars]
  sex_dt <- sex_dt[match_n == 2]
  keep_vars <- c(match_vars, "sex", "mean", "standard_error")
  sex_dt[, id := .GRP, by = match_vars]
  sex_dt <- dplyr::select(sex_dt, keep_vars)
  sex_dt <- dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"), fun.aggregate=mean)
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


# RUN SEX SPLIT -----------------------------------------------------------

original_hep <- get_bundle_version(bv_id) 
hep_sex_dt <- copy(original_hep)
hep_sex_dt <- hep_sex_dt[!is_outlier == 1 & measure %in% c("prevalence")]
hep_sex_dt <- hep_sex_dt[cases != 0 & sample_size != 0, ]
hep_sex_dt <- get_cases_sample_size(hep_sex_dt)
hep_sex_dt <- get_se(hep_sex_dt)
hep_sex_dt <- calculate_cases_fromse(hep_sex_dt)
hep_sex_matches <- find_sex_match(hep_sex_dt)
mrbrt_sex_dt <- calc_sex_ratios(hep_sex_matches)

model_name <- MODEL_NAME

if (file.exists(FILE_PATH){
  sex_model <- readr::read_rds(FILE_PATH)
} else {
     sex_model <- run_mr_brt(
     output_dir = mrbrt_dir,
     model_label = model_name,
     data = mrbrt_sex_dt,
     mean_var = "log_ratio",
     se_var = "log_se",
     study_id = "id",
     method = "trim_maxL",
     trim_pct = 0.1,
     overwrite_previous = F
    )
   readr::write_rds(sex_model, FILE_PATH)
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
  tosplit_dt$note_modeler <- NA
  nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female")]
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
  tosplit_dt <- tosplit_dt[!is.na(both_N)] ## GET RID OF DATA THAT COULDN'T FIND POPS - RIGHT NOW THIS IS HAPPENING FOR 100+ DATA POINTS
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

predict_sex <- split_data(hep_sex_dt, sex_model)

fix_subnationals <- function(sex_dt) {
  dt <- copy(sex_dt)
  dt$changed_loc <- NA
  loc_id <- dt$location_id
  model_locs <- get_location_metadata(location_set_id=35)
  england_locs <- model_locs[grepl("GBR", ihme_loc_id) & (level == 5 | level == 3), location_id]
  decomp4 <- dt[location_id %in% england_locs, ]
  dt <- dt[!(location_id %in% england_locs), ]
  return(list(final_dt = dt, dropped = decomp4))
}


sex_fixed <- fix_subnationals(predict_sex$final)
sex_final <- sex_fixed$final_dt[mean > 1, `:=` (mean = 1, note_modeler = paste0(note_modeler, " | mean estimate > 1 after sex splitting, capped at 1 ") )]
