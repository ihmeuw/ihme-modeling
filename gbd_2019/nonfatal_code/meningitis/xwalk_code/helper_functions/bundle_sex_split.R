#' @author 
#' @date 2019/04/17
#' @description Sex-split GBD 2019 decomp2
#' @function run_sex_split
#' @param dt bundle data used to be sex split
#' @param drop_large_se boolean if ratios with a standard error greater than the 80th percentile should be dropped
#' @param model_name name of the directory in the mrbrt_dir where MR-BRT outputs are stored
#' @return predict_sex$final : Sex-split dataset
#' @return predict_sex$graph : Plot of both-sex mean against sex-split means saved in mrbrt_dir
#' @note Make sure to run the output of plot_mr_brt(sex_model) in a qlogin session

# For mclapply
if(Sys.info()[1] != 'Windows') { RhpcBLASctl::blas_set_num_threads(1) }

pacman::p_load(data.table, parallel)

# SOURCE FUNCTIONS --------------------------------------------------------
k <- # filepath
source(paste0(k, "current/r/get_population.R"))
source(paste0(k, "current/r/get_age_metadata.R"))

repo_dir <- # filepath
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "plot_mr_brt_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))

# FUNCTIONS ---------------------------------------------------------------

## FIND MATCHES ON NID, AGE, LOCATION, MEASURE, AND YEAR
find_sex_match <- function(dt) {
  sex_dt <- copy(dt)
  # Don't use data that is split elsewhere in the bundle (i.e, input_type == parent)
  sex_dt <- sex_dt[input_type != "parent"]
  # Don't use data that was outliered
  sex_dt <- sex_dt[is_outlier != 1]
  sex_dt <- sex_dt[sex %in% c("Male", "Female") & measure %in% c("prevalence", "incidence")]
  # Find rows that have corresponding male and female rows
  match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end")
  sex_dt[, match_n := .N, by = match_vars]
  sex_dt <- sex_dt[match_n == 2]
  keep_vars <- c(match_vars, "sex", "mean", "standard_error")
  sex_dt[, id := .GRP, by = match_vars]
  sex_dt <- dplyr::select(sex_dt, keep_vars)
  sex_dt <- dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"))
  sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0]
  sex_dt[, id := .GRP, by = c("nid")]
  return(sex_dt)
}

## Calculate ratios and standard errors and convert to log space
calc_sex_ratios <- function(dt) {
  ratio_dt <- copy(dt)
  ratio_dt[, `:=` (ratio = mean_Female/mean_Male,
                   ratio_se = sqrt((mean_Female^2 / mean_Male^2) * (standard_error_Female^2/mean_Female^2 + standard_error_Male^2/mean_Male^2)))]
  ratio_dt[, log_ratio := log(ratio)]
  # approximate log-transformed standard error using delta method
  ratio_dt[, log_se := sqrt((1 / ratio^2) * ratio_se^2)]
  # ratio_dt$log_se <- sapply(1:nrow(ratio_dt), function(i) {
  #   mean_i <- ratio_dt[i, "ratio"]
  #   se_i <- ratio_dt[i, "ratio_se"]
  #   deltamethod(~log(x1), mean_i, se_i^2)
  # })
  return(ratio_dt)
}

## Get male, female, and both sex location-age specific population for each row 
get_row <- function(n, dt, pops) {
  row <- copy(dt)[n]
  # round age start and age end to closest gbd age group values
  row_age_start <- row[, age_start]
  row_age_end <- row[, age_end]
  gbd_age_start <- unique(pops$age_group_years_start)
  gbd_age_end <- unique(pops$age_group_years_end)
  row_gbd_age_start <- gbd_age_start[which.min(abs(row_age_start - gbd_age_start))]
  row_gbd_age_end <- gbd_age_end[which.min(abs(row_age_end - gbd_age_end))]
  if (row_gbd_age_start  == row_gbd_age_end) {
    pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                            age_group_years_start == row_gbd_age_start])
  } else {
    pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                            age_group_years_start >= row_gbd_age_start & age_group_years_end <= row_gbd_age_end])
  }
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex_id")]
  row[, `:=` (male_N = agg[sex_id == 1, pop_sum], female_N = agg[sex_id == 2, pop_sum],
              both_N = agg[sex_id == 3, pop_sum])]
  return(row)
}

## Apply sex ratio to Both Sex data
split_data <- function(dt, model) {
  # Create required columns for xwalk version
  if (!"crosswalk_parent_seq" %in% colnames(dt)) dt[, "crosswalk_parent_seq":= integer()]
  if (!"group" %in% colnames(dt)) dt[, "group":= integer()]
  if (!"specificity" %in% colnames(dt)) dt[, "specificity":= character()]
  if (!"group_review" %in% colnames(dt)) dt[, "group_review":= integer()]
  # Split both sex data that are not group review == 0
  tosplit_dt <- copy(dt)
  nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female") | (sex == "Both" & group_review == 0)
                           | measure != "incidence"]
  tosplit_dt <- tosplit_dt[sex == "Both" & (group_review == 1 | is.na(group_review))
                           & measure == "incidence"]
  tosplit_dt[, midyear := floor((year_start + year_end)/2)]
  # Create draws of log(female : male) ratio incorporating between study heterogeneity
  preds <- predict_mr_brt(model, newdata = data.table(X_intercept = 1, Z_intercept = 1), write_draws = T)
  pred_draws <- as.data.table(preds$model_draws)
  pred_draws[, c("X_intercept", "Z_intercept") := NULL]
  draws <- paste0("draw_", 0:999)
  pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
  ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
  ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)
  # Get age-location specific population to split denominator
  pops <- get_population(age_group_id = c(2:20, 30:32, 235), location_id = tosplit_dt[, unique(location_id)], 
                         year_id = tosplit_dt[, unique(midyear)], sex_id = c(1,2,3), gbd_round_id = 6, decomp_step = 'step2')
  age_meta <- get_age_metadata(12, gbd_round_id = 6)
  pops <- merge(pops, age_meta[, .(age_group_id, age_group_years_start, age_group_years_end)], 
                by = "age_group_id")
  pops[age_group_years_end == 125, age_group_years_end := 99]
  tosplit_list <- mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt, pops), mc.cores = 9)
  tosplit_dt <- rbindlist(tosplit_list)
  if (nrow(tosplit_dt[is.na(both_N)]) > 0) stop("There are some rows with no custom populations pulled")
  # Create male and female rows from both sex row
  tosplit_dt[, merge := 1]
  pred_draws[, merge := 1]
  split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
  # The following equations for sex splitting are derived from:
  # Let ratio = mean_female / mean_male. Then
  # mean_male * pop_male + mean_female * pop_female = mean_both * pop_both
  # mean_male * pop_male + (ratio * mean_male) * pop_female = mean_both * pop_both
  # mean_male = mean_both * (pop_both / (pop_male + ratio * pop_female))
  # mean_female = ratio * mean_male
  split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
  split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_sample_size := sample_size * (male_N / both_N)]
  split_dt[, female_sample_size := sample_size * (female_N / both_N)]
  split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
  male_dt <- copy(split_dt)
  options(warn=-1)
  male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = "", lower = "", 
                  cases = "", sample_size = male_sample_size, uncertainty_type_value = "", sex = "Male",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                        ratio_se, ")"))]
  male_dt[, specificity := "sex"][, group_review:= 1][, group:= 1]
  male_dt[, crosswalk_parent_seq := seq]
  male_dt[, seq := NA]
  male_dt <- dplyr::select(male_dt, names(dt))
  female_dt <- copy(split_dt)
  female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = "", lower = "", 
                    cases = "", sample_size = female_sample_size, uncertainty_type_value = "", sex = "Female",
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (", 
                                          ratio_se, ")"))]
  female_dt[, specificity := "sex"][, group_review:= 1][, group:= 1]
  female_dt[, crosswalk_parent_seq := seq]
  female_dt[, seq := NA]
  female_dt <- dplyr::select(female_dt, names(dt))
  options(warn=1)
  # Check that male and female data tables have the same crosswalk parent seqs
  if (!all.equal(unique(male_dt$crosswalk_parent_seq), unique(female_dt$crosswalk_parent_seq))) {
    stop("Male and Female split data tables have different crosswalk parent seq values")
  }
  total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))
  return(list(final = total_dt, ratio = ratio_mean, se = ratio_se))
}

## COPIES BOTH SEX CFR AND EMR DATA AS MALE AND FEMALE REMISSION FOR XWALK VERSION
get_sex_cfr <- function(sexsplit_dt) {
  dt <- copy(sexsplit_dt)
  not_cfr_dt <- dt[!measure %in% c("cfr", "mtexcess")]
  male_dt <- copy(dt[measure %in% c("cfr", "mtexcess") & (group_review == 1 | is.na(group_review))])
  male_dt[, `:=`(sex = "Male", specificity = "sex", group_review = 1, group = 1)]
  male_dt[, crosswalk_parent_seq := seq]
  male_dt[, seq := NA]
  female_dt <- copy(dt[measure %in% c("cfr", "mtexcess") & (group_review == 1 | is.na(group_review))])
  female_dt[, `:=`(sex = "Female", specificity = "sex", group_review = 1, group = 1)]
  female_dt[, crosswalk_parent_seq := seq]
  female_dt[, seq := NA]
  dt <- rbindlist(list(male_dt, female_dt, not_cfr_dt))
  return(dt)
}

# RUN SEX SPLIT -----------------------------------------------------------
run_sex_split <- function(dt, model_name) {
  dem_sex_dt <- data.table::copy(dt)
  print("Finding sex matches")
  dem_sex_matches <- find_sex_match(dem_sex_dt)
  print("Calculating sex ratios")
  mrbrt_sex_dt <- calc_sex_ratios(dem_sex_matches)
  print("Running MR-BRT")
  sex_model <- run_mr_brt(
    output_dir = mrbrt_dir,
    model_label = model_name,
    data = mrbrt_sex_dt,
    mean_var = "log_ratio",
    se_var = "log_se",
    study_id = "id",
    trim_pct = 0.1,
    method = 'trim_maxL',
    max_iter = 400,
    overwrite_previous = T
  )
  saveRDS(sex_model, file = paste0(mrbrt_dir, model_name, "_model_obj.RDS"))
  print(plot_mr_brt(sex_model))
  print("Splitting data")
  predict_sex <- split_data(dem_sex_dt, sex_model)
  print(paste("Female to male ratio", predict_sex$ratio, " (", predict_sex$se, ")"))
  print("Duplicating sex-specific CFR and EMR rows from both sex rows")
  predict_sex_final_dt <- get_sex_cfr(predict_sex$final)
  fwrite(predict_sex_final_dt, file.path(dem_dir, paste0(model_name, ".csv")))
  return(list(data = predict_sex_final_dt, model = sex_model, ratio = predict_sex$ratio))
}