#' @author 
#' @date 2020/08/18
#' @description Main script for GBD 2020 meningitis crosswalking,
#'              for surveillance ONLY.
#'              Creates model object for surveillance and applies it.
#'              Sex-split: See bundle_sex_split
#'              Crosswalk:
#'              Reference: inpatient data with correction factor 2
#'              Alternatives: Surveillance data 
#'                            Surveillance data including viral
#'              MR-BRT model used logit difference (logit(alt) - logit(ref)) and
#'              used Delta method to calculate standard error in logit space
#'              Age-split: Used clinical data global age pattern to 
#'                         split numerator and population to split denominator 
#'                         for rows with age ranges greater than 25 years


rm(list=ls())

pacman::p_load(openxlsx, pbapply, ggplot2, data.table, boot, stringr, msm, metafor)
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
source(paste0(helper_dir, "bundle_crosswalk_collapse_network.R" ))
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
  sex_model_filepath <- paste0("/filepath.xlsx")
  dem_sex_final_dt <- as.data.table(read.xlsx(sex_model_filepath))
} else sex_model_filepath <- NULL

# get matches
if (read_matches){
  matches_filepath <- paste0("/filepath.xlsx")
  matches <- as.data.table(read.xlsx(matches_filepath))
} else matches_filepath <- NULL

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
  sex_split_filepath <- paste0(h, "/filepath.pkl")

  ## Sex split "group review data"
  group_review_split <- sex_split_group_review(data, out_dir, bv_id, plot = T)

  dem_sex_dt <- copy(group_review_split)

  # NOTE - sex split can ONLY be done in log space
  # algebra in bundle_sex_split does not work for logit
  # because log(a) - log(b) = log(a/b). But logit(a) - logit(b) does NOT equal logit(a/b)

  # sex split data and save

  dem_sex_final_list <- pred_sex_split(run_model = T,
                                       sex_model_filepath = sex_split_filepath,
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
# direct comparisons for marketscan, network comparision for surveillance

inputs <- list(reference_def  = c("cv_inpatient"),
               alternate_defs = c("cv_surveillance",
                                   "broadly_defined"),
               spline = F, # add an age spline?
               logit_transform = T, # logit or log?
               remove_x_intercept = F, 
               trim_pct = 0, 
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
                   addl_x_covs = list("cv_haqi")
  )
} else if (spline == F){
  fit_info <- list(addl_x_covs = list("cv_haqi"))
  # fit_info <- list(addl_x_covs = NULL)
}

model_inputs <- c(inputs, fit_info)
list2env(model_inputs, envir = .GlobalEnv)
xwalk_model_name <- paste0(date, "_surveillance_subdefinitions_network_trim_pct_", trim_pct, "_study_id_", study_id, "_haq_linear")
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
data <- get_study_cov_surv(data)

## fix rows with a zero SE
data <- fix_zero_se(data)

## Pull out original data. Later we will apply the XW to this dataset.
data[, age_mid := (age_start + age_end)/2]
data[, year_mid := (year_start + year_end)/2]
data[, age_floor := floor(age_mid)]
data[, year_floor := floor(year_mid)]

# drop probable or suspected cases from the data this round 
data <- copy(data[cv_confirmed == 1  | is.na(cv_confirmed)])
original_data_to_xw <- copy(data[measure==xw_measure & get(reference_def) == 0,])
original_data_no_xw <- copy(data[measure==xw_measure & get(reference_def) == 1 | measure != xw_measure,])

# do not match non-incidence
data_match <- copy(data[measure==xw_measure])
# do not include anything with dummy sample size
data_match <- copy(data_match[!note_modeler %like% "dummy"])

age_cut <- c(0,1,5,20,40,60,80,100)
year_cut <- seq(1980,2020,5)

if (!read_matches){
  # Run for surveillance only - we can read in old coefficients for marketscan
  matches <- bundle_crosswalk_collapse_network(df = data_match, 
                                               gbd_round_id = gbd_round_id,
                                               decomp_step = ds,
                                               covariate_names = alternate_defs,
                                               reference_name = reference_def,
                                               age_cut = age_cut,
                                               year_cut = year_cut, 
                                               merge_type = "between", 
                                               location_match = "exact", 
                                               include_logit = T)
  
  ## Generate study_id variable
  matches[, study_id := paste0(nid, n_nid)]
  matches[, year_mid := floor((year_start + year_end)/2)]
  matches[, age_mid := floor((age_start + age_end)/2)]
  
  ## Merge with HAQi
  matches <- merge(matches, haqi, by = c("location_id", "year_mid"))
  
  ## Save
  wb <- createWorkbook()
  addWorksheet(wb=wb, sheetName = "surv_network_matches")
  writeData(wb, "surv_network_matches", matches)
  saveWorkbook(wb, paste0(out_dir, "matches_", xwalk_model_name, ".xlsx"), overwrite = TRUE)
  
}

## Generate knots based on ranges
age_knots = quantile(matches$age_mid, probs = seq(0, 1, length.out = 5))

## Prep the original data for predicting. 

# to_adjust <- copy(original_data_to_xw)
to_adjust <- original_data_to_xw[get("cv_surveillance") == 1]
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

## Format data for MR-BRT

if (spline == T){
  format_covs <- c(addl_x_covs, spline_covs)
} else format_covs <- addl_x_covs

# xwalk_model_name <- paste0(xwalk_model_name, "_drop_USA")
# df <- matches[ihme_loc_abv != "USA"]
df <- copy(matches)

formatted_data <- CWData(
    df = df,
    obs = response,
    obs_se = data_se,
    dorm_separator = "AND",
    alt_dorms = "alt_dorms",
    ref_dorms = "ref_dorms",
    covs = format_covs,
    study_id = study_id,
    add_intercept = T 
  )

# save the formatted data
py_save_object(object = formatted_data, filename = paste0(out_dir, xwalk_model_name, "_formatted_data.pkl"), pickle = "dill")

## Run MR-BRT

covariate_list <- list(
 CovModel(cov_name = "intercept"))
if (!is.null(addl_x_covs)){
  covariate_list <- c(covariate_list, list(CovModel(cov_name = unlist(addl_x_covs))))
}
if (spline == T){
  covariate_list <- c(covariate_list, list(CovModel(cov_name = "age_mid", spline = XSpline(knots = age_knots, degree = 3L, l_linear = T, r_linear = T))))
}

results <- CWModel(
  cwdata = formatted_data,
  obs_type = mrbrt_response,
  cov_models = covariate_list,
  use_random_intercept = !is.null(study_id),
  gold_dorm = reference_def,
  inlier_pct = (1 - trim_pct),
  # max_iter = max_iter_val, 
  # outer_max_iter = outer_max_iter_val
)
  
# save the results
model <- save_crosswalk_RDS(results, out_dir, paste0(xwalk_model_name))
## Save as a .pkl
saveRDS(results, paste0(out_dir, xwalk_model_name,  "_pyobj.RDS"))
py_save_object(object = results, filename = paste0(out_dir, xwalk_model_name, ".pkl"), pickle = "dill")
## Save results dataframe
result_df <- results$create_result_df()
write.csv(result_df, paste0(out_dir, xwalk_model_name, "_results.csv"))
## Plot crosswalk results with the functions in xwalk package
# ... then type 'exit' to get back to the R interpreter
repl_python()

plots <- import("crosswalk.plots")

for (def in alternate_defs){
  plots$funnel_plot(
    cwmodel = results,
    cwdata = formatted_data,
    continuous_variables = format_covs,
    obs_method = def,
    plot_note = paste(def, "funnel"),
    plots_dir = out_dir,
    file_name = paste0(xwalk_model_name, "_", def, "_funnel"),
    write_file = TRUE
  )
  
  plots$dose_response_curve(
    dose_variable = "cv_haqi",
    obs_method = def,
    continuous_variables=list(),
    cwmodel = results,
    cwdata = formatted_data,
    plot_note=paste(def, "dose response"),
    plots_dir = out_dir,
    file_name = paste0(xwalk_model_name, "_", def, "_dose_response"),
    write_file=TRUE)
  
  if(spline==T){
    plots$dose_response_curve(
      dose_variable = "age_mid",
      obs_method = def,
      continuous_variables=list("cv_haqi"),
      cwmodel = results,
      cwdata = formatted_data,
      plot_note=paste(def, "dose response"),
      plots_dir = out_dir,
      file_name = paste0(xwalk_model_name, "_", def, "_age_dose_response"),
      write_file=TRUE)
  }
}

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
  
  new_preds <- adjust_orig_vals(
    fit_object = results, # object returned by `CWModel()`
    df = to_adjust,
    orig_dorms = "definition",
    orig_vals_mean = "mean",
    orig_vals_se = "standard_error",
    study_id = study_id,
    data_id = "data_id"
  )

  ## Use the predictions to adjust the original data.
  new_predictions <- data.table(new_preds)
  adjusted_data <- merge(new_predictions, to_adjust, by="data_id")
  
  ## The below is consolidated - just use output from adjust_orig_vals
  setnames(adjusted_data, c("ref_vals_mean", "ref_vals_sd"), c("mean_adj", "se_adjusted"))
  # calculate the actual adjustment in mean: it is not necessarily equal to pred_diff_mean if you are using random effects in prediction
  ## include beta and gamma uncertainty in ratio uncertainty
  if(logit_transform) {
    adjusted_data[, c("mean_logit_adjusted", "se_logit_adjusted") := data.table(delta_transform(mean = mean_adj, sd = se_adjusted, transformation = "linear_to_logit"))]
    adjusted_data[, logit_diff_mean := logit_mean - mean_logit_adjusted]
    adjusted_data[, c("mean_adj_normal", "pred_se_normal") := data.table(delta_transform(mean = pred_diff_mean, sd = sqrt(pred_diff_sd^2 + as.vector(results$gamma)), transformation = "logit_to_linear"))]
    plot_var <- "logit_diff_mean"
  } else {
    adjusted_data[, c("mean_log_adjusted", "se_log_adjusted") := data.table(delta_transform(mean = mean_adj, sd = se_adjusted, transformation = "linear_to_log"))]
    adjusted_data[, log_diff_mean := log_mean - mean_log_adjusted]
    adjusted_data[, c("mean_adj_normal", "pred_se_normal") := data.table(delta_transform(mean = pred_diff_mean, sd = sqrt(pred_diff_sd^2 + as.vector(results$gamma)), transformation = "log_to_linear"))]
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
  pdf(paste0(out_dir, xwalk_model_name, "_dose_response_imitationV2.pdf"), width = 11, height = 8.5)
  # plot the matches in points
  for (def in unique(adjusted_data$definition)){
    to_adjust <- expand.grid(definition = def, mean = seq(0.01,.99,0.01), standard_error = 0.1, cv_haqi = seq(1,100,1))
    y_pred <- adjust_orig_vals(
      fit_object = results, # object returned by `CWModel()`
      df = to_adjust,
      orig_dorms = "definition",
      orig_vals_mean = "mean",
      orig_vals_se = "standard_error"
    )
    y_pred <- cbind(y_pred, to_adjust)
    y_mean <- y_pred$pred_diff_mean
    y_sd_fixed <- y_pred$pred_diff_sd 
    y_sd <- sqrt(y_sd_fixed^2 + as.vector(results$gamma))
    # lower/upper bound with fixed effect and heterogeneity
    y_lo <- y_mean - 1.96*y_sd; y_hi <- y_mean + 1.96*y_sd
    # lower/upper bound with only fixed effect
    y_lo_fe <- y_mean - 1.96*y_sd_fixed; y_hi_fe <- y_mean + 1.96*y_sd_fixed
    data_df <- data.table(y = formatted_data$df$logit_diff,
                          se = formatted_data$df$logit_diff_se,
                          w = results$w,
                          cv_haqi = formatted_data$df$cv_haqi,
                          obs_method = unlist(formatted_data$alt_dorms),
                          dorm_alt = formatted_data$df$alt_dorms,
                          dorm_ref = formatted_data$df$ref_dorms,
                          mean = 0.1,
                          standard_error = 0.1)
    data_pred = adjust_orig_vals(   
      fit_object = results,
      df=data_df,            
      orig_dorms = "obs_method", 
      orig_vals_mean = "mean",  
      orig_vals_se = "standard_error"
    )
    data_df$pred = data_pred$pred_diff_mean
    # determine points inside/outside funnel
    data_df$position <- 'inside funnel'
    data_df[y < (pred - (se * 1.96)) | y > (pred + (se * 1.96)) , position := 'outside_funnel']
    mean_var <- ifelse(logit_transform, "logit_diff", "ratio_log")
    plot <- ggplot() +
      geom_point(data = data_df[dorm_alt != def], aes(x = cv_haqi, y = y, shape = as.factor(w), size = 1/se), color = "gray", fill = "gray") +
      geom_point(data = data_df[dorm_alt == def], aes(x = cv_haqi, y = y, shape = as.factor(w), size = 1/se, fill = position, color = position)) +
      labs(x = "Exposure", y = "Effect Size") + 
      scale_shape_manual(name= "", values=c(21,4), labels=c("Included", "Trimmed")) + scale_fill_manual(values=c("seagreen","coral")) + scale_color_manual(values=c("darkgreen","firebrick")) +
      ggtitle(paste0(def, "dose response")) +
      theme_bw() +
      theme(text = element_text(size = 12, color = "black"), legend.position = "bottom") +
      geom_line(data = y_pred, aes(x = cv_haqi, y = pred_diff_mean)) +
      geom_ribbon(data = y_pred, aes(x = cv_haqi, ymin = y_lo, ymax = y_hi), alpha = 0.15) +
      geom_ribbon(data = y_pred, aes(x = cv_haqi, ymin = y_lo_fe, ymax = y_hi_fe), alpha = 0.35)
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
  fwrite(final_dt, paste0(out_dir, date, xwalk_model_name, "_all_xwalked_data_NO_Marketscan.csv"))
  
}


if (nrow(dem_sex_final_dt) == nrow(final_dt)) {
  message("Done crosswalking.")
} else {
  stop ("Rows were dropped, trace back where this happened")
}

# then COMBINE this with marketscan and age split