#' @author 
#' @date 2020/08/18
#' @description Main script for GBD 2020 meningitis crosswalking,
#'              for MARKETSCAN AND MARKETSCAN 2000 ONLY.
#'              Creates model object for MS and applies it.
#'              Sex-split: See bundle_sex_split
#'              Crosswalk:
#'              Reference: inpatient data with correction factor 2
#'              Alternatives: inpatient only marketscan from 2000
#'                            all other inpatient only claims data
#'              MR-BRT model used logit difference (logit(alt) - logit(ref)) and
#'              used Delta method to calculate standard error in logit space
#'              Age-split: Used clinical data global age pattern to 
#'                         split numerator and population to split denominator 
#'                         for rows with age ranges greater than 25 years
#'               

rm(list=ls())

pacman::p_load(openxlsx, pbapply, ggplot2, data.table, boot, msm, metafor)
library(reticulate)

# SOURCE FUNCTIONS --------------------------------------------------------
library(crosswalk, lib.loc = "/filepath/")

# Source all GBD shared functions at once
shared.dir <- "/filepath/"
files.sources <- list.files(shared.dir)
files.sources <- paste0(shared.dir, files.sources) # writes the filepath to each function
invisible(sapply(files.sources, source)) # "invisible suppresses the sapply output

# Source helper functions
helper_dir <- "filepath"
source(paste0(helper_dir, "prep_to_adjust.R"))
source(paste0(helper_dir, "sex_split_group_review.R" ))
source(paste0(helper_dir, "rm_zeros.R" ))
source(paste0(helper_dir, "bundle_sex_split.R" ))
source(paste0(helper_dir, "graph_xwalk.R" ))
source(paste0(helper_dir, "graph_sex_split.R" ))
source(paste0(helper_dir, "fix_zero_se.R" ))
source(paste0(helper_dir, "clean_mr_brt.R" ))
source(paste0(helper_dir, "get_study_cov.R" ))
source(paste0(helper_dir, "bundle_crosswalk_collapse.R" ))
source(paste0(helper_dir, "bundle_age_split.R" ))
source(paste0(helper_dir, "get_cases_sample_size.R" ))
source(paste0(helper_dir, "get_closest_age.R" ))
source(paste0(helper_dir, "get_gbd_age_group_id.R" ))
source(paste0(helper_dir, "split_numerator.R" ))
source(paste0(helper_dir, "split_denominator.R" ))
source(paste0(helper_dir, "get_dismod_age_pattern.R" ))
source(paste0(helper_dir, "bool_check_age_splits.R" ))
source(paste0(helper_dir, "outlier_clinical_data.R" ))
source(paste0(helper_dir, "save_crosswalk_RDS.R" ))
source(paste0(helper_dir, "find_closest.R" ))

# SET OBJECTS -------------------------------------------------------------
bundle <- 28
cause <- 332
name_short <- "meningitis"
acause <- "meningitis"
ds <- 'iterative'
gbd_round_id <- 7
xw_measure <- "incidence"
read_sexsplit <- F
read_matches <- T

# get bundle version id from bv tracking sheet
bundle_version_dir <- "filepath"
bv_tracker <- fread(paste0(bundle_version_dir, 'bundle_version_tracking.csv'))
bv_row <- bv_tracker[bundle_id == bundle & current_best == 1]

bv_id <- bv_row$bundle_version
print(paste("Sex splitting on bundle version", bv_id, "BV description", bv_row$description))

# Specify model names to uses a prefixes for the MR-BRT output directories
date <- gsub("-", "_", Sys.Date())

# Specify where to save xwalk versions and MR-BRT outputs
out_dir <- "filepath"
dir.create(out_dir, recursive = T)

# get bundle version
data <- get_bundle_version(bv_id, transform = T, fetch = "all")

## fix rows with a zero SE
data <- fix_zero_se(data)

# get sex split data
if (read_sexsplit){
  sex_model_filepath <- paste0(h, "/filepath.xlsx")
  dem_sex_final_dt <- as.data.table(read.xlsx(sex_model_filepath))
}

# get matches
if (read_matches){
  matches_filepath <- paste0(h, "/filepath.xlsx")
  matches <- list()
  matches[["cv_marketscan_data"]] <- as.data.table(read.xlsx(matches_filepath, sheet = 1))
  matches[["cv_marketscan_inp_2000"]] <- as.data.table(read.xlsx(matches_filepath, sheet = 2))
} else matches_filepath <- NA

# outlier clinical data equal to zero
# data <- outlier_clinical_data(data)

# SEX SPLIT ---------------------------------------------------------------
## Sex split arguments
plot <- T
fix_ones <- F # since we are modeling in log space
offset <- F # apply an offset to data that is 0 or 1?

if (offset == T){
  type <- "offset" 
  drop_zeros <- F
} else if (offset == F){
  type <- "drop_zeros"
  drop_zeros <- T
}

if(!read_sexsplit){
  sex_model_name   <- paste0("_", type, "_sex_split_on_bv_id_", bv_id)
  sex_split_filepath <- NA
  sex_model_filepath <- NA
  
  ## Sex split "group review data"
  group_review_split <- sex_split_group_review(data, out_dir, bv_id, plot = T)
  
  dem_sex_dt <- copy(group_review_split)
  
  # NOTE - sex split can ONLY be done in log space
  # algebra in bundle_sex_split does not work for logit
  # because log(a) - log(b) = log(a/b). But logit(a) - logit(b) does NOT equal logit(a/b)
  
  # sex split data and save
  
  dem_sex_final_list <- pred_sex_split(run_model = T,
                                       dem_sex_dt = dem_sex_dt,
                                       original_data = data,
                                       cause = cause,
                                       bv_id = bv_id,
                                       gbd_round_id = gbd_round_id,
                                       decomp_step = ds,
                                       name_short = name_short,
                                       out_dir = out_dir,
                                       model_name = sex_model_name,
                                       offset = offset,
                                       drop_zeros = drop_zeros,
                                       fix_ones = fix_ones,
                                       plot = plot)
  dem_sex_final_dt <- dem_sex_final_list$data
  
  ## Write out dem_sex_final_list as an RDS with necessary sex-splitting info
  saveRDS(dem_sex_final_list, paste0(out_dir, sex_model_name, "model_info_and_results.rds"))
  
  ## Save sex-split data as a CSV
  fwrite(dem_sex_final_dt, paste0(out_dir, sex_model_name, ".csv"))
}

# CROSSWALK ---------------------------------------------------------------
# Specify the column names for the reference and alternative study covariates
inputs <- list(reference_def  = c("cv_inpatient"),
               alternate_defs = c("cv_marketscan_data",
                                  "cv_marketscan_inp_2000"),
               spline = T, # add an age spline?
               logit_transform = T, # logit or log?
               remove_x_intercept = F, 
               trim_pct = 0.0, 
               sex_split_dropped_zeros = drop_zeros,
               sex_model_filepath = sex_model_filepath,
               matches_filepath = matches_filepath,
               max_iter_val = NULL,
               outer_max_iter_val = NULL,
               study_id = "ihme_loc_abv",
               offset = F)

list2env(inputs, envir = .GlobalEnv)

if (spline == T){
  fit_info <- list(spline_covs = list("age_mid"),
                   degree = 3, # cubic spline
                   r_linear = F,
                   l_linear = F,
                   spline_monotonicity = NULL,
                   spline_convexity = NULL,
                   knot_placement_procedure ="frequency", 
                   addl_x_covs = list(NULL)
  )
} else if (spline == F){
  fit_info <- list(addl_x_covs = list("cv_haqi"))
  # fit_info <- list(addl_x_covs = NULL)
}

model_inputs <- c(inputs, fit_info)
list2env(model_inputs, envir = .GlobalEnv)
xwalk_model_name <- paste0(date, "_non_network_trim_pct_", trim_pct, "_study_id_", study_id, "_age_spline")

saveRDS(model_inputs, paste0(out_dir, "inputs_for_", xwalk_model_name, ".RDS"))

## Create other inputs from initial arguments

if (offset == T){
  type <- "offset" 
  drop_zeros <- F
} else if (offset == F){
  type <- "drop_zeros"
  drop_zeros <- T
}


if(logit_transform) {
  response <- "logit_diff"
  data_se <- "logit_diff_se"
  mrbrt_response <- "diff_logit"
  fix_ones <- T
} else {
  response <- "ratio_log"
  data_se <- "ratio_se_log"
  mrbrt_response <- "diff_log"
  fix_ones <- F
}

# Get HAQ values as a covariate for MR-BRT
haqi <- get_covariate_estimates(covariate_id = 1099,
                                gbd_round_id = gbd_round_id, 
                                decomp_step = "step3", 
                                status = "best",
                                location_id = "all", 
                                year_id = "all"
)
haqi <- haqi[, c("location_id", "year_id", "mean_value")]
setnames(haqi, c("mean_value", "year_id"), c("cv_haqi", "year_mid"))

# get data
data <- copy(as.data.table(dem_sex_final_dt))

# Properly mark each row as reference or alternative with relevant columns
data <- get_study_cov(data)

## fix rows with a zero SE
data <- fix_zero_se(data)

## Pull out original data. Later we will apply the XW to this dataset.
data[, age_mid := (age_start + age_end)/2]
data[, year_mid := (year_start + year_end)/2]
data[, age_floor := floor(age_mid)]
data[, year_floor := floor(year_mid)]

# drop probable and suspected cases from the data this round 
data <- copy(data[cv_confirmed == 1 | is.na(cv_probable)])
original_data_to_xw <- copy(data[measure==xw_measure & get(reference_def) == 0,])
original_data_no_xw <- copy(data[measure==xw_measure & get(reference_def) == 1 | measure != xw_measure,])

# do not match non-incidence
data_match <- copy(data[measure==xw_measure])
# do not include anything with dummy sample size
data_match <- copy(data_match[!note_modeler %like% "dummy"])

# do not include group_review 0 data, suspected/probable, or non-incidence
data_match <- copy(data[cv_confirmed == 1 | is.na(cv_confirmed)])
data_match <- copy(data_match[group_review == 1| is.na(group_review)])
data_match <- copy(data_match[measure==xw_measure])

if (!read_matches){
  matches <- list()
  age_cut <- c(0,1,5,20,40,60,80,100)
  
  for (def in alternate_defs){
    if (def == "cv_marketscan_inp_2000"){
      year_cut <- c(1980, 1985, 1990, 1995, 2005, 2010, 2015, 2020) # allow to match with 2005 data
    } else {year_cut <- seq(1980,2020,5)}
    matches[[def]] <- bundle_crosswalk_collapse(df = data_match, 
                                                gbd_round_id = gbd_round_id,
                                                decomp_step = ds,
                                                covariate_name = def,
                                                reference_name = reference_def,
                                                age_cut = age_cut,
                                                year_cut = year_cut, 
                                                merge_type = "between", 
                                                location_match = "exact", 
                                                include_logit = T)
    ## Generate study_id variable
    matches[[def]][, study_id := paste0(n_nid, nid)]
    matches[[def]][, year_mid := floor((year_start + year_end)/2)]
    matches[[def]][, age_mid := floor((age_start + age_end)/2)]
    ## Merge with HAQi
    matches[[def]] <- merge(matches[[def]], haqi, by = c("location_id", "year_mid"))
  }
  
  wb <- createWorkbook()
  for (def in alternate_defs){
    addWorksheet(wb=wb, sheetName = def)
    writeData(wb, def, matches[[def]])
  }
  saveWorkbook(wb, paste0(out_dir, "matches_", xwalk_model_name, ".xlsx"), overwrite = TRUE)
  
}

## Prep the original data for predicting. 

## Generate knots based on ranges
age_knots = quantile(rbindlist(matches)$age_mid, probs = seq(0, 1, length.out = 5))

# to_adjust <- copy(original_data_to_xw)
to_adjust <- original_data_to_xw[cv_marketscan_data == 1 | cv_marketscan_inp_2000 == 1]
to_adjust <- prep_to_adjust(to_adjust,
                            alternate_defs = alternate_defs,
                            reference_def = reference_def,
                            gbd_round_id = gbd_round_id,
                            ds = ds, 
                            logit_transform = logit_transform)

## Add HAQ values
to_adjust <- merge(to_adjust, haqi, by.x = c("location_id", "year_floor"), by.y = c("location_id", "year_mid"), all.x = T)


#######################################
##### Launch the MR-BRT model #########
#######################################

message("Launching MR-BRT...")

## If there are no matches, remove from list of alternate definitions and get rid of them
for (cov in alternate_defs) {
  if (!(cov %in% names(matches))) {
    message(paste(cov, " has no matches. We cannot estimate a beta for it. It will be dropped from the list of x-covariates."))
    
    to_adjust <- to_adjust[get(cov) == 0,]
    
    alternate_defs <- setdiff(alternate_defs, paste0(cov))
    #message(paste0("Your new x-covariates are: ", print(x_covs)))
  }
}

short_cov_list <- list("age_mid")

## Format data for MR-BRT
formatted_data <- list()
for (def in alternate_defs){
  formatted_data[[def]] <- CWData(
    df = matches[[def]],
    obs = response,
    obs_se = data_se,
    alt_dorms = "alt_dorms",
    ref_dorms = "ref_dorms",
    covs = short_cov_list,
    study_id = "ihme_loc_abv",
    add_intercept = T
  )
}


## Run MR-BRT

covariate_list <- list(
  CovModel(cov_name = "intercept"),
  CovModel(cov_name = "age_mid", spline = XSpline(knots = age_knots, degree = 3L, l_linear = T, r_linear = T)))

results <- list()
for (def in alternate_defs){
  results[[def]] <- CWModel(
    cwdata = formatted_data[[def]],
    obs_type = mrbrt_response,
    cov_models = covariate_list,
    use_random_intercept = T,
    gold_dorm = reference_def,
    inlier_pct = (1 - trim_pct)
  )
  
  # save the results
  model <- save_crosswalk_RDS(results[[def]], out_dir, paste0(xwalk_model_name, def))
  ## Save as a .pkl
  saveRDS(results[[def]], paste0(out_dir, xwalk_model_name, "_", def, "_pyobj.RDS"))
  py_save_object(object = results[[def]], filename = paste0(out_dir, xwalk_model_name, "_", def, ".pkl"), pickle = "dill")
  ## Save results dataframe
  result_df <- results[[def]]$create_result_df()
  write.csv(result_df, paste0(out_dir, xwalk_model_name, "_", def, "results.csv"))
}


## Plot crosswalk results with the functions in xwalk package
# ... then type 'exit' to get back to the R interpreter
repl_python()

plots <- import("crosswalk.plots")

for (def in alternate_defs){
  plots$funnel_plot(
    cwmodel = results[[def]], 
    cwdata = formatted_data[[def]],
    continuous_variables = list("age_mid"),
    obs_method = def,
    plot_note = paste(def, "funnel"), 
    plots_dir = out_dir, 
    file_name = paste0(xwalk_model_name, "_", def, "_funnel"),
    write_file = TRUE
  )
  
  plots$dose_response_curve(
    dose_variable = unlist(short_cov_list),
    obs_method = def, 
    continuous_variables=short_cov_list, 
    cwmodel = results[[def]], 
    cwdata = formatted_data[[def]],
    plot_note=paste(def, "dose response"), 
    plots_dir = out_dir, 
    file_name = paste0(xwalk_model_name, "_", def, "_dose_response"),
    write_file=TRUE)
  
}

## Assigning global variables
results <<- results
to_adjust <<- to_adjust


## Use the predictions to adjust the original data.
## Predict onto original data with AND without random effects
loops <- c(T, F)
orig_study_id <- copy(study_id)
orig_out_dir <- copy(out_dir)

for (condition in loops){
  predict_on_random_effects <- condition
  plot <- T
  if (predict_on_random_effects){
    out_dir <- paste0(orig_out_dir, "/predict_on_random_effects/")
    dir.create(out_dir)
    study_id <- orig_study_id
  } else {
    out_dir <- paste0(orig_out_dir, "/predict_without_random_effects/")
    dir.create(out_dir)
    study_id <- NULL
  }
  
  ## Predict onto original data 
  new_preds_list <- list()
  
  for (def in alternate_defs){
    new_preds_list[[def]] <- adjust_orig_vals(
      fit_object = results[[def]], # object returned by `CWModel()`
      df = to_adjust[definition == def],
      orig_dorms = "definition",
      orig_vals_mean = "mean",
      orig_vals_se = "standard_error",
      study_id = study_id, 
      data_id = "data_id"
    )
  }
  
  # combine predictions for all the definitions
  new_preds <- rbindlist(new_preds_list)
  new_predictions <- data.table(new_preds)
  
  ## Use the predictions to adjust the original data.
  adjusted_data <- merge(new_predictions, to_adjust, by="data_id")
  
  ## The below is consolidated - just use output from adjust_orig_vals
  setnames(adjusted_data, c("ref_vals_mean", "ref_vals_sd"), c("mean_adj", "se_adjusted"))
  # calculate the actual adjustment in mean: it is not necessarily equal to pred_diff_mean if you are using random effects in prediction
  if(logit_transform) {
    adjusted_data[, c("mean_logit_adjusted", "se_logit_adjusted") := data.table(delta_transform(mean = mean_adj, sd = se_adjusted, transformation = "linear_to_logit"))]
    adjusted_data[, logit_diff_mean := logit_mean - mean_logit_adjusted]
    adjusted_data[, c("mean_adj_normal", "pred_se_normal") := data.table(delta_transform(mean = pred_diff_mean, sd = sqrt(pred_diff_sd^2 + as.vector(results[["cv_marketscan_data"]]$gamma)), transformation = "logit_to_linear"))]
    plot_var <- "logit_diff_mean"
  } else {
    adjusted_data[, c("mean_log_adjusted", "se_log_adjusted") := data.table(delta_transform(mean = mean_adj, sd = se_adjusted, transformation = "linear_to_log"))]
    adjusted_data[, log_diff_mean := log_mean - mean_log_adjusted]
    adjusted_data[, c("mean_adj_normal", "pred_se_normal") := data.table(delta_transform(mean = pred_diff_mean, sd = sqrt(pred_diff_sd^2 + as.vector(results[["cv_marketscan_data"]]$gamma)), transformation = "log_to_linear"))]
    plot_var <- "log_diff_mean"
  }
  
  ## However, if mean is 0, we need to adjust the uncertainty in normal space, not log-space
  adjusted_data[original_zero_mean==1, se_adjusted := ifelse(get(reference_def) == 1, se_adjusted, sqrt(standard_error^2 + pred_se_normal^2))]
  adjusted_data[original_zero_mean==1, mean_adj := 0]
  adjusted_data[original_zero_mean==1, `:=` (upper=mean_adj + 1.96*se_adjusted, lower=mean_adj - 1.96*se_adjusted)]
  
  data_for_comparison <- copy(adjusted_data)
  fwrite(data_for_comparison, paste0(out_dir, xwalk_model_name, "_adjusted_data.csv"))
  
  ## Plot, if desired, before collapsing back to preferred data structure
  if (plot){
    graph_xwalk(dt = data_for_comparison,
                out_dir = out_dir,
                model_name = xwalk_model_name)
    message("Plot saved to ", out_dir)
  }
  
  # try my own plot 
  pdf(paste0(out_dir, xwalk_model_name, "_dose_response_imitation.pdf"), width = 20, height = 7)
  # plot the matches in points
  for (def in unique(adjusted_data$definition)){
    matches2 <- data.table(cbind(results[[def]]$cwdata$df, data.frame(w = results[[def]]$w)))
    matches2[, trimmed := ifelse(w == 1, "Included", "Trimmed")]
    matches2 <- matches2[ref_dorms == "cv_inpatient" & alt_dorms %like% def,]
    mean_var <- ifelse(logit_transform, "logit_diff", "ratio_log")
    data_compare <- data_for_comparison[definition == def & original_zero_mean == 0] # leave out points with mean zero
    plot <- ggplot() +
      geom_point(data = matches2, aes(x = age_mid, y = get(mean_var), alpha = 0.3, color = as.factor(super_region_name), shape = as.factor(trimmed))) +
      labs(x = unlist(spline_covs), y = ifelse(logit_transform, "Logit difference", "Log difference")) +
      ggtitle(paste0("Meta-Analysis Results, ", def)) +
      theme_bw() +
      theme(text = element_text(size = 12, color = "black"), legend.position = "bottom") +
      # geom_smooth(data = data_compare, aes(x = cv_haqi, y = pred_diff_mean, color = as.factor(definition), se = F, fullrange = T)) +
      geom_point(data = data_compare, aes(x = age_mid, y = get(plot_var), color = as.factor(super_region_name), size = 2, se = F, fullrange = T)) +
      scale_shape_manual(name= "", values=c(19,4), labels=c("Included", "Trimmed"))
    print(plot)
  }
  dev.off()
  
  ## Collapse back to our preferred data structure
  full_dt <- copy(adjusted_data)
  fwrite(full_dt, paste0(out_dir, xwalk_model_name, "_compare_adjusted_data.csv"))
  
  full_dt[, `:=` (mean = mean_adj, standard_error = se_adjusted, upper = NA, lower = NA,
                  cases = "", sample_size = "", uncertainty_type_value = "",
                  note_modeler = paste0(note_modeler, " | crosswalked with ", response, ": ", round(pred_diff_mean, 2), " (",
                                        round(pred_diff_sd), ")"))]
  
  extra_cols <- setdiff(names(full_dt), names(original_data_no_xw))
  full_dt[, c(extra_cols) := NULL]
  
  # fill definition for unadjusted data
  original_data_no_xw[measure==xw_measure, definition := reference_def]
  
  final_dt <- rbind(original_data_no_xw, full_dt, fill=T)
  fwrite(final_dt, paste0(out_dir, date, xwalk_model_name, "_marketscan_xwalked_data_WITH_reference.csv"))
  fwrite(full_dt, paste0(out_dir, date, xwalk_model_name, "_marketscan_xwalked_data_NO_reference.csv"))
}


if (nrow(dem_sex_final_dt) == nrow(final_dt)) {
  message("Done crosswalking.")
} else {
  stop ("Rows were dropped, trace back where this happened")
}

## Combine this with the surveillance, and run age split