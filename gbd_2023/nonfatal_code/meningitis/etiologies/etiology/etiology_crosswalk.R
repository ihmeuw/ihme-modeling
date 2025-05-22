rm(list=ls())
library(ggplot2)
library(RColorBrewer)
library(plotly)
library(data.table)
library(argparse)
library(openxlsx)
library(matrixStats)
# SOURCE FUNCTIONS --------------------------------------------------------
library(crosswalk, lib.loc = "/filepath/")

# Source all GBD shared functions at once
shared.dir <- "/filepath/"
files.sources <- list.files(shared.dir)
files.sources <- paste0(shared.dir, files.sources) # writes the filepath to each function
invisible(sapply(files.sources, source)) # "invisible suppresses the sapply output

# Source helper functions
helper_dir <- "filepath"
source(paste0(helper_dir, "bundle_age_split_etiology.R")
source(paste0(helper_dir, "carry_outliers_cv_to_cv.R")
source(paste0(helper_dir, "sex_split_group_review.R" ))
source(paste0(helper_dir, "bundle_sex_split.R" ))
source(paste0(helper_dir, "rm_zeros.R" ))
source(paste0(helper_dir, "graph_xwalk.R" ))
source(paste0(helper_dir, "clean_mr_brt.R" ))
source(paste0(helper_dir, "graph_sex_split.R" ))
source(paste0(helper_dir, "etio_age_split_helpers.R" ))
source(paste0(helper_dir, "find_nondismod_locs.R" ))
source(paste0(helper_dir, "get_cases_sample_size.R" ))
source(paste0(helper_dir, "save_crosswalk_RDS.R" ))

one_hot_encode <- function(df, col) {
  stopifnot(is.data.table(df))
  stopifnot(class(df[, get(col)]) == 'factor')
  
  # Create a copy of the df to work with
  df_copy <- copy(df)
  df_copy[, uniq_row := seq(1, .N)]
  one_hot <- as.data.table(dcast(
    df_copy, formula(paste0("uniq_row ~ ", col)),
    length
  ))
  stopifnot(ncol(one_hot) == length(unique(df[, get(col)])) + 1)
  stopifnot(unique(
    one_hot[, rowSums(.SD),
            .SDcols = names(one_hot)[names(one_hot) != "uniq_row"]]
  ) == 1)
  
  reference_level <- names(one_hot)[names(one_hot) != "uniq_row"][1]
  one_hot[, c("uniq_row", reference_level) := NULL]
  names(one_hot) <- sapply(names(one_hot), function(x) {paste0(col, x)})
  df <- cbind(df, one_hot)
  return(list("df" = df, "cols" = names(one_hot)))
}

log_transform <- function(df, mean_col, se_col) {
  tf <- as.data.frame(
    delta_transform(
      df[, get(mean_col)], df[, get(se_col)], "linear_to_log")
  )
  names(tf) <- c(paste0("log_", mean_col), paste0("log_", se_col))
  return(cbind(df, tf))
}

logit_transform <- function(df, mean_col, se_col) {
  tf <- as.data.frame(
    delta_transform(
      df[, get(mean_col)], df[, get(se_col)], "linear_to_logit")
  )
  names(tf) <- c(paste0("logit_", mean_col), paste0("logit_", se_col))
  return(cbind(df, tf))
}

convert_to_factor <- function(df, factor_cols) {
  # Convert relevant columns to factors
  factor_cols <- factor_cols[factor_cols %in% names(df)]
  df[, (factor_cols) := lapply(.SD, factor), .SDcols = factor_cols]
  return(df)
}

get_data <- function(working_dir, matches_file, match_cols, value_col, error_col,
                     factor_cols = NULL) {
  # Get the data
  df <- fread(paste0(working_dir, matches_file))
  stopifnot(!any(is.na(df)))
  if(!is.null(factor_cols)) df <- convert_to_factor(df, factor_cols)
  
  # Transform from linear rates to logit rates using the delta method
  if (method == "logit"){
    df <- logit_transform(df, paste0(value_col, ".x"), paste0(error_col, ".x"))
    df <- logit_transform(df, paste0(value_col, ".y"), paste0(error_col, ".y"))
  } else if (method == "log"){
    df <- log_transform(df, paste0(value_col, ".x"), paste0(error_col, ".x"))
    df <- log_transform(df, paste0(value_col, ".y"), paste0(error_col, ".y"))
  }
  
  # Calculate differences in log space
  diff <- calculate_diff(
    df, alt_mean = paste0(method, "_", value_col, ".y"), alt_sd = paste0(method, "_", error_col, ".y"),
    ref_mean = paste0(method, "_", value_col, ".x"), ref_sd = paste0(method, "_", error_col, ".x")
  )
  names(diff) <- c(paste0(method, "_diff_", value_col), paste0(method, "_diff_se"))
  df <- cbind(df, diff)
  
  # Lastly, create a variable for the different matching groups
  df[, group_id := .GRP, by = match_cols]
  return(df)
}

check_collinearity <- function(df, covs) {
  # pairwise correlations
  # Get just the continuous variables
  corr_mat <- abs(cor(df[, ..covs]))
  diag(corr_mat) <- 0  # Just to get rid of the self-correlations
  print("Checking collinearity... correlation matrix")
  print(round(corr_mat, digits = 2))
  # If this breaks, evaluate the above matrix, edit the covariates
  # selected in the config, and try again!
  stopifnot(all(corr_mat < 0.80))
}

run_crosswalk <- function(df, dorm_col, linear_covs, spline_covs, gold_dorm, stage1_betas = NULL) {
  # Have to generate these column names outside the CWData call for some reason
  alt_dorms <- paste0(dorm_col, ".y")
  ref_dorms <- paste0(dorm_col, ".x")
  covs <- c(linear_covs, spline_covs)
  df.cw <- CWData(
    df = df,
    obs = paste0(method, "_diff_mean"),
    obs_se = paste0(method, "_diff_se"),
    alt_dorms = alt_dorms,
    ref_dorms = ref_dorms,
    covs = covs,
    study_id = "group_id"
  )
  # age_knots <- quantile(df$age_mid, probs = seq(0, 1, length.out = 5))
  # print(age_knots)
  linear_cov_models <- lapply(linear_covs, function(x) {CovModel(cov_name = x)})
  spline_cov_models <- lapply(spline_covs, function(x, age_knots) {
    CovModel(cov_name = x, spline = XSpline(knots = c(0, 0.005479452, 0.7083333, 3, 12, 100), 
                                            degree = 3L, l_linear = TRUE, r_linear = TRUE))
    }, age_knots = age_knots)
  cov_models <- c(linear_cov_models, spline_cov_models)
  
  cov_models[[length(cov_models) + 1]] <- CovModel(cov_name = "intercept")
  fit.cw <- CWModel(
    cwdata = df.cw,
    obs_type = paste0("diff_", method),
    cov_models = cov_models,
    gold_dorm = gold_dorm,
    max_iter = 10000L
  )
  return(fit.cw)
}

save_betas <- function(fit.cw, model_dir, stage1_betas = NULL) {
  # The first 4 columns are the dorm (pathogen), covariate, beta and beta sd
  betas <- as.data.table(fit.cw$create_result_df()[, 1:4])
  # Degrees of freedom calculation
  degf = fit.cw$cwdata$num_obs - nrow(betas[dorms != betas$dorms[1]])
  betas[, p := 2 * pt(-1 * abs(beta / beta_sd), df = degf)]
  betas[, signf := p <= 0.05]
  if (is.null(stage1_betas)) {
    print("Returning stage 1 betas for setting priors...")
    return(betas)
  } else {
    print("Comparing stage 1 and 2 models...")
    compare_betas <- merge(
      betas, stage1_betas, by = c("dorms", "cov_names"), suffixes = c("_new", "_old")
    )
    write.csv(compare_betas, paste0(model_dir, "compare_betas.csv"), row.names=FALSE)
    return(compare_betas)
  }
}

make_predictions <- function(predict_template, gold_dorm, fit.cw, model_dir, encode_cols = NULL) {
  preds_template <- copy(predict_template)
  value_col <- "mean"
  error_col <- "standard_error"
  if (method == "logit") preds_template <- logit_transform(preds_template, paste0(value_col), paste0(error_col))
  if (method == "log") preds_template <- log_transform(preds_template, paste0(value_col), paste0(error_col))
  
  if (!is.null(encode_cols)){
    preds_template <- convert_to_factor(preds_template, encode_cols)
    for (encode_col in encode_cols) {
      preds_template <- one_hot_encode(preds_template, encode_col)$df
    }
  }
  
  # subset to only rows that need to be adjusted
  # if a row does not include an etiology, we want to know what the mean would be for that etiology
  # What we want here is the predicted ratio for every reference:X combination where that is necessary
  # so we need to expand the current data frame X4 "changing" the definition for each row
  # make sure that the cv_reference is always 1, so this will only rbind 4 data frames.
  preds_template[, eval(paste0("cv_", gold_dorm)) := 1]
  preds_temp_new <- rbind(preds_template[cv_meningitis_meningo == 0 & cv_obj_all_meningitis == 0][, bundle_name := "meningitis_meningo"],
                          preds_template[cv_meningitis_hib == 0 & cv_obj_all_meningitis == 0][, bundle_name := "meningitis_hib"],
                          preds_template[cv_meningitis_other == 0 & cv_obj_all_meningitis == 0][, bundle_name := "meningitis_other"],
                          preds_template[cv_meningitis_gbs == 0 & cv_obj_all_meningitis == 0][, bundle_name := "meningitis_gbs"],
                          preds_template[cv_meningitis_pneumo == 0 & cv_obj_all_meningitis == 0][, bundle_name := "meningitis_pneumo"])
  oldnames <- names(preds_temp_new)
  preds_temp_new$data_id <- 1:nrow(preds_temp_new)
  
  unadjusted <- preds_template[(cv_meningitis_meningo + cv_meningitis_hib + cv_meningitis_other + cv_meningitis_gbs + cv_meningitis_pneumo == 5 )
                               | cv_obj_all_meningitis == 1]
  unadjusted[, mean_predicted := mean]
  
  # What is the expected mean of X given mean of reference, and the ratio? 

  preds <- adjust_orig_vals(
    fit_object = fit.cw,
    df = preds_temp_new,
    orig_dorms = "bundle_name",
    orig_vals_mean = "mean",
    orig_vals_se = "standard_error",
    study_id = NULL
  )
  
  # Write a copy for vetting purposes
  write.csv(preds, paste0(model_dir, gold_dorm, "_raw_predictions.csv"), row.names = FALSE)
  
  ## To return later
  # ratio_mean <- preds$pred_diff_mean
  # ratio_se <- sqrt(preds$pred_diff_sd^2 + as.vector(fit.cw$gamma))
  # ratio_se <- data.table(data_id = preds$data_id, se = ratio_se)
  
  # MANUALLY make adjustments 
  preds_temp_new <- merge(preds_temp_new, preds[, c("pred_diff_mean", "pred_diff_sd", "data_id")],by = "data_id")
  
  preds_temp_new[, ref_vals_mean := inv.logit(pred_diff_mean + get(paste0(method, "_mean")))]

  # Plot the adjusted mean vs the continuous covariates, before reshaping
  # plot the predictions as lines and the original points as points
  # original_data <- df_matched[bundle_name.x == gold_dorm | bundle_name.y == gold_dorm]
  # need to deal with the x & y situation here
  
  # create an expand.grid to get continuous hypothetical lines
  x_pred_dt <- expand.grid(pcv = seq(0, 100, 20),
                           age_mid = c(seq(0, 10, 0.1), seq(12,100,2)),
                           hib = seq(0, 100, 20),
                           mean = 0.5, 
                           standard_error = 0.1, 
                           haq  = seq(0, 100, 20),
                           bundle_name = unique(preds_temp_new$bundle_name))
  setDT(x_pred_dt)
  
  preds_plot <- adjust_orig_vals(
    fit_object = fit.cw,
    df = x_pred_dt,
    orig_dorms = "bundle_name",
    orig_vals_mean = "mean",
    orig_vals_se = "standard_error",
    study_id = NULL
  )
  
  preds_plot <- cbind(x_pred_dt, preds_plot[, c("pred_diff_mean", "pred_diff_sd", "data_id")])

  p <- ggplot() + geom_point(data=preds_plot[], aes(x=age_mid, y=pred_diff_mean, color=as.factor(haq))) + facet_grid(rows = vars(pcv), cols = vars(bundle_name)) + theme_bw() + xlab("age_mid") + ylab("pred_diff_mean") + ggtitle("facet by pcv coverage")
  ggsave(paste0(model_dir, gold_dorm, "_age_spline_plot.pdf"), plot = p, width = 12, height = 8)
  p <- ggplot() + geom_point(data=preds_plot[age_mid<=20], aes(x=age_mid, y=pred_diff_mean, color=as.factor(haq))) + facet_grid(rows = vars(pcv), cols = vars(bundle_name)) + theme_bw() + xlab("age_mid") + ylab("pred_diff_mean") + ggtitle("facet by pcv coverage")
  ggsave(paste0(model_dir, gold_dorm, "_young_age_spline_plot.pdf"), plot = p, width = 12, height = 8)
  # Now ref_vals_mean is the expected proportion of etiology X given etiology reference
  # reshape to wide according to bundle_name so all etiologies are added at once
  preds_temp_new <- dcast(preds_temp_new, ...~bundle_name, value.var = c("ref_vals_mean", "pred_diff_mean", "pred_diff_sd", "data_id" ))
  preds_temp_new <- as.data.table(preds_temp_new)
  
  # Sum all missing etiologies
  preds_temp_new[, ref_vals_mean_all := rowSums(.SD, na.rm = T), .SDcols = names(preds_temp_new)[names(preds_temp_new) %like% "ref_vals_mean"]]
  
  # SE of the adjusted data point IN TRANSFORMED SPACE calculated as sqrt(a^2 + b^2 + c^2), where
  # a is the (log or logit) standard error of the original data point,
  # b is the standard error of the predicted adjustment
  # c is the standard deviation of between-group heterogeneity, a.k.a. sqrt(gamma)
  
  # object a 
  # preds_template$log_standard_error
  a <- preds_temp_new[,get(paste0(method, "_standard_error"))]^2
  
  # object b
  # sqrt(sd_all)
  df_tmp_sd <- preds_temp_new[,names(preds_temp_new)[names(preds_temp_new) %like% "pred_diff_sd"], with = F]
  # square all the SDs
  sd_add <- sapply(1:ncol(df_tmp_sd), function(i, df_tmp){df_tmp[,..i]^2}, df_tmp=df_tmp_sd)
  # # add all the squared SDs
  sd_all <- rowSums(do.call(cbind, sd_add), na.rm = TRUE)
  b <- sqrt(sd_all)
  
  # object c
  # sqrt(gamma)
  # c <- sqrt(as.vector(fit.cw$gamma))
  c <- 0
  
  sd_all <- sqrt(a + b + c)
  preds_temp_new$se_predicted_transform <- sd_all
  
  # Note how many missing etiologies were added 
  preds_temp_new$n_added_etios <- apply(!is.na(preds_temp_new[,names(preds_temp_new) %like% "ref_vals_mean", with = F]), 1, sum)
  
  # adjust mean by "adding" new etiologies to the denominator
  preds_temp_new[, mean_predicted := mean/(1+ref_vals_mean_all)]
  
  # bring standard error back into linear space   
  # inv logit/log with delta transform
  test <- delta_transform(mean = logit(preds_temp_new$mean_predicted), 
                          sd = preds_temp_new$se_predicted_transform, 
                          transformation = paste0(method, "_to_linear"))
  # already have mean, only need SE
  preds_temp_new$se_predicted <- test[,2]
  
  #the SE values are extreme, so I set caps
  upper_bound <- quantile(preds_temp_new$se_predicted, 0.75)
  preds_temp_new[se_predicted > upper_bound, se_predicted := upper_bound]
  
  preds_temp_new[, note_modeler := paste(note_modeler, "| denominator crosswalk applied, added", n_added_etios, "etiologies, original mean", round(mean,2))]
  
  # combine with unadjuststed rows 
  preds_temp_new <- rbind(preds_temp_new, unadjusted, fill = T)
  
  # unadjusted will have NAs in se_predicted and mean_predicted, fix this
  preds_temp_new[is.na(se_predicted), se_predicted := standard_error]
  preds_temp_new[is.na(mean_predicted), mean_predicted := mean]
  
  # Write a copy for vetting purposes
  write.csv(preds_temp_new, paste0(model_dir, gold_dorm, "_predictions.csv"), row.names = FALSE)
  
  return(preds_temp_new)
}

compute_rmse <- function(obs, pred) {
  return(sqrt(mean((obs - pred)^2)))
}

make_plots <- function(data_orig, df_matched, predictions,
                       continuous_covs, compare_betas = NULL) {
  plots <- list()
  getPalette <- colorRampPalette(brewer.pal(9, "Set1"))
  set.seed(42)
  
  # Input data
  for (cov in continuous_covs) {
    plots[[cov]] <- ggplot(data_orig, aes_string(cov)) +
      geom_histogram() +
      scale_fill_manual(values = sample(getPalette(length(unique(data_orig$source)))))
  }
  
  # to the below plot, add aesthetics for how many etios are being "added"
  plots[['mean_vs_mean_adj']] <- ggplot(
    predictions, aes(x = mean, y = mean_predicted, color = n_added_etios)) +
    geom_point() + geom_abline(intercept = 0, slope = 1) 

  
  # plot the ratios as they vary with age
  for (cov in continuous_covs) {
    plots[[paste0('adj_ratio_vs_', cov)]] <- ggplot(
      predictions, aes(x = get(cov), y = mean/mean_predicted)) + facet_wrap(~n_added_etios) +
        geom_point()
  }
  return(plots)
}

save_plots <- function(model_dir, plots, bundle_name) {
  for (plot_name in names(plots)) {
    if (plot_name %in% c("resid_vs_pred", "pred_vs_obs")) {
      ggsave(
        paste0(plot_name, "_", bundle_name, ".pdf"),
        plot = plots[[plot_name]],
        device = "pdf",
        path = model_dir
      )
    } else if (plot_name == "resid_vs_pred_by_pathogen") {
      ggsave(
        paste0(plot_name, "_", bundle_name, ".pdf"),
        plot = plots[[plot_name]],
        device = "pdf",
        path = model_dir,
        height = 20,
        width = 20,
        units = "in"
      )
    } else {
      fig <- ggplotly(plots[[plot_name]])
      htmlwidgets::saveWidget(fig, paste0(model_dir, "/", plot_name, "_", bundle_name, ".html"))
    }
  }
}

main <- function(model_dir, infectious_syndrome=NULL, encode_cols = NULL,
                 linear_covs, spline_covs, ref_pathogen, read_cache = FALSE) {
  # 1) GET THE DATA
  df_matched <- get_data(
    model_dir,
    "crosswalk_in.csv",
    c("nid", "age_group_id", "sex", "year_id", "location_id"),
    "mean",
    "standard_error",
    encode_cols
  )
  add_cols <- c()
  if (!is.null(encode_cols)){  
    for (encode_col in encode_cols) {
    matched_encode <- one_hot_encode(df_matched, encode_col)
    df_matched <- matched_encode$df
    # If the encoded column is also a covariate,
    # make sure to update the covariates list with the new
    # column names
    if (encode_col %in% linear_covs) {
      linear_covs <- linear_covs[linear_covs != encode_col]
      linear_covs <- c(linear_covs, matched_encode$cols)
    }
    add_cols <- c(add_cols, matched_encode$cols)
  }}

  cov_cols <- c(linear_covs, spline_covs)
  # Check collinearity
  continuous_covs <- cov_cols[!(cov_cols %in% add_cols)]
  check_collinearity(df_matched, continuous_covs)
  
  if (!read_cache) {
    # 2) RUN CROSSWALK TO GET BETAS
    #stopifnot(ref_pathogen %in% df_matched$bundle_name.x)
    # fit.cw.betas <- run_crosswalk(
    #   df_matched, "bundle_name", as.list(linear_covs), as.list(spline_covs), ref_pathogen
    # )
    # stage1_betas <- save_betas(fit.cw.betas, model_dir)
    
    # 3) RERUN CROSSWALK WITH PRIORS
    fit.cw.preds <- run_crosswalk(
      df_matched, "bundle_name", as.list(linear_covs), as.list(spline_covs),
      ref_pathogen
       # , stage1_betas = stage1_betas
    )
    # compare_betas <- save_betas(fit.cw.preds, model_dir, stage1_betas = stage1_betas)
    # Save model object
    py_save_object(
      object = fit.cw.preds,
      filename = paste0(model_dir, "/fit.pkl"),
      pickle = "dill"
    )
    df_result <- fit.cw.preds$create_result_df()
    write.csv(df_result, paste0(model_dir,"result_crosswalk.csv"))
  } else {
    print("Skipping modelling, reading cache...")
    # compare_betas <- fread(paste0(model_dir, "/compare_betas.csv"))
    fit.cw.preds <- py_load_object(
      filename = paste0(model_dir, "/fit.pkl"),
      pickle = "dill"
    )
  }
  
  # 4) CREATE PREDICTIONS
  print("Making predictions...")
  # can predict ONLY on the reference 
  preds <- make_predictions(data_list[[ref_pathogen]], ref_pathogen, fit.cw = fit.cw.preds, model_dir = model_dir, encode_cols = encode_cols)

  # 5) SAVE PLOTS
  print("Saving plots...")
  data_orig <- fread(paste0(model_dir, "crosswalk_in.csv"))
  plots <- make_plots(
    data_orig, df_matched, preds, continuous_covs)
  save_plots(model_dir, plots, ref_pathogen)
  print("Finished network analysis successfully!")
  return(preds)
}


## START DATA PROCESSING

# get all bundle versions and then sex split them
date <- gsub("-", "_", Sys.Date())
ds <- 'iterative'
gbd_round_id = 7
method <- "logit" # log or logit 
model_dir <- paste0("/filepath/", date, "_", method, "_age_spline/")
dir.create(model_dir)
spline_covs <- c("age_mid")
linear_covs <- c("haq", "pcv")
encode_cols <- NULL

cov_cols <- c(linear_covs, spline_covs)
ref_pathogen <- "meningitis_pneumo"
read_cache <- F

bundle_map <- as.data.table(read.xlsx(paste0('filepath.xlsx')))

dir_2020 <- 'filepath'
bv_tracker <- fread(paste0(dir_2020, 'bundle_version_tracking.csv'))

data_dir <- 'filepath'
bundles <- c(29, 33, 37, 41, 7958)
bundle_map <- bundle_map[bundle_id %in% bundles]
bundle_map[bundle_id == 7958, bundle_name_short := "meningitis_gbs"]

age_meta <- get_age_metadata(age_group_set_id = 19, gbd_round_id = 7)
age_ids <- get_ids("age_group")

data_list <- list()
for (i in 1:nrow(bundle_map)) {
  id <- bundle_map$bundle_id[i] 
  name <- bundle_map$bundle_name_short[i]
  bv_id <- bv_tracker[bundle_id == id & current_best == 1]$bundle_version
  bv_df <- get_bundle_version(bundle_version_id = bv_id,
                              fetch = "all", 
                              transform = T, 
                              export = T)
  
  out_dir <- 'filepath'
  drop_threshold <- 10

  # drop any rows that have a sample size less than 10
  # fill in cases, sample size, mean - including getting SS from effective SS!
  bv_df <- get_cases_sample_size(bv_df)
  drop <- nrow(bv_df[sample_size < drop_threshold | effective_sample_size < drop_threshold])
  message(paste("dropping", drop, "rows with a sample size less than", drop_threshold))
  bv_df$drop <- 0
  bv_df[sample_size < drop_threshold | effective_sample_size < drop_threshold, drop := 1]
  
  # sex split the bundle data
  # Specify model names to uses a prefixes for the MR-BRT output directories
  sex_model_name   <- paste0(date, "_bundle_",id, "_logit_sexsplit_on_bv_", bv_id)
  plot_out_dir <- paste0(out_dir, date, "_bundle_",id)
  group_review_split <- sex_split_group_review(bv_df, plot_out_dir, bv_id, plot = F)
  
  dem_sex_dt <- copy(group_review_split)
  
  plot <- T
  offset <- F # apply an offset to data that is 0 or 1?
  drop_zeros <- T # drop any values that are 0 or 1? 
  fix_ones <- T # since we are modeling in log space
  xw_measure <- "proportion"
  
  dem_sex_final_list <- run_sex_split(dem_sex_dt = dem_sex_dt, # grp review split
                                      data = bv_df,# not grp review split, for matching
                                      out_dir = out_dir, 
                                      sex_model_name = sex_model_name,
                                      offset = offset, 
                                      drop_zeros = drop_zeros,
                                      fix_ones = fix_ones,
                                      plot = plot)
  dem_sex_final_dt <- dem_sex_final_list$data
  
  # subset to only non-drop rows
  dem_sex_final_dt <- dem_sex_final_dt[drop == 0]
  dem_sex_final_dt$bundle_name <- name
  
  # offset zeros in preparation to log/logit transform later
  dem_sex_final_dt <- rm_zeros(dem_sex_final_dt, offset = T)
  dem_sex_final_dt[, age_mid := age_start+age_end/2]
  dem_sex_final_dt[age_mid > 100, age_mid := 100]
  dem_sex_final_dt[, year_id := floor((year_start+year_end)/2)]
  
  # transform age_mid to age_group_id
  dem_sex_final_dt <- dem_sex_final_dt %>%  mutate(age_group_id = if_else(age_mid<=28/365, 42, 
                                                                    if_else(age_mid<5, 179,  
                                                                            if_else(age_mid < 50, 239, 
                                                                                    if_else(age_mid < 70, 25, 26)))))
  dem_sex_final_dt$age_group_id <- as.factor(dem_sex_final_dt$age_group_id)

  data_list[[name]] <- as.data.table(dem_sex_final_dt)
}

## Fix the cv_pneumo, cv_meningo, cv_hib, cv_meningitis_gbs, cv_meningitis_other
## If those columns were originally marked 1 leave as 1, OR
## If they were marked as zero but the source is present in the corresponding bundle, change to 1
for (i in 1:nrow(bundle_map)) {
  name <- bundle_map$bundle_name_short[i]
  df_tmp <- data_list[[name]]
  setnames(df_tmp, c("cv_meningo", "cv_pneumo", "cv_hib"), c("cv_meningitis_meningo", "cv_meningitis_pneumo", "cv_meningitis_hib"))
  # manual overwrite for CDC sources
  df_tmp[field_citation_value %like% "CDC", `:=` (cv_meningitis_pneumo = 1, cv_meningitis_meningo = 1, cv_meningitis_hib = 1)]
  for (nid_look in unique(df_tmp$nid)){ # for every nid
    # look in all the other bundles
    for (k in which(1:nrow(bundle_map) != i)){
      nid_present <- nid_look %in% unique(data_list[[k]]$nid)
      # if the nid is present in the bundle of a given etiology, mark that etiology as 1
      if(nid_present){
        df_tmp[nid==nid_look, paste0("cv_", unique(data_list[[k]]$bundle_name)) := 1]
      } # else df_tmp[nid==nid_look, paste0("cv_", unique(data_list[[k]]$bundle_name)) := 0]
    }
  }
  print(paste("checked for denominator completion in bundle", i, "of", length(data_list)))
  data_list[[name]] <- df_tmp
}

data_list_copy <- copy(data_list)
# merge with covariates of interest 
# get covariate estimates for HAQ, PCV, HIB
cov_list <- list()
cov_list[["haq"]] <- get_covariate_estimates(covariate_id = 1099, gbd_round_id = 7, decomp_step = "iterative",
                                             location_id = unique(all_pairs$location_id), year_id = "all",
                                             sex_id = c(1,2,3))
cov_list[["pcv"]] <- get_covariate_estimates(covariate_id = 2314, gbd_round_id = 7, decomp_step = "iterative",
                                             location_id = unique(all_pairs$location_id), year_id = "all",
                                             sex_id = c(1,2,3))
# cov_list[["hib"]] <- get_covariate_estimates(covariate_id = 2313, gbd_round_id = 7, decomp_step = "iterative",
#                                              location_id = unique(all_pairs$location_id), year_id = unique(all_pairs$year_id),
#                                              sex_id = c(1,2,3))

# merge on data_list by location-year, covs are not age-sex-specific
for (name in unique(bundle_map$bundle_name_short)){
  for (i in 1:length(cov_list)){
    cov_name <- as.character(names(cov_list))[i]
    setnames(cov_list[[i]], "mean_value", cov_name, skip_absent = T)
    data_list[[name]] <- merge(data_list[[name]], cov_list[[i]][,c(cov_name, "location_id", "year_id"), with = F], by = c("location_id", "year_id"))
  }
}

# merge all the data list bits
merge_cols <- names(rbindlist(data_list,fill = T))
merge_cols_cv <- merge_cols[merge_cols %like% "cv_"]
merge_cols <- c('nid', 'location_id', 'year_start', 'year_end', 'age_start', 'age_end', 'age_mid', 'age_group_id', 'sex', "year_id", 'group_review', cov_cols, merge_cols_cv)
merge_cols <- unique(merge_cols)
# test_merge <- merge(data_list[[1]], data_list[[2]], by=merge_cols[merge_cols %in% intersect(names(data_list[[1]]), names(data_list[[2]]))])
new_list <- list()
k <- 1
for(i in 1:length(data_list)){
  for(d in 1:length(data_list)){
    if(i != d){
      dt_id <- merge(data_list[[i]], data_list[[d]], by = merge_cols[merge_cols %in% intersect(names(data_list[[i]]), names(data_list[[d]]))])
      if (length(new_list)>0) {
        # proxy check for equality by checking number of rows
        duplicate <- sapply(1:length(new_list), function(x) nrow(new_list[[x]]) == nrow(dt_id))
        if (!any(duplicate) == T){ # If this data table does not already exist, add it
          new_list[[k]] <- dt_id
          k <- k+1
        }
      } else {
        new_list[[k]] <- dt_id
        k <- k+1
      }
    }
  }
}

# check for drops
if (nrow(combinations(length(data_list),2)) != length(new_list)) stop("not all combinations included")
all_pairs <- rbindlist(new_list, fill = T)
# drop improbable GBS data 
# if age_mid is > 90 days and meningitis_gbs is the larger of the pair, drop
# gbs meningitis is only in bundle_name.y
all_pairs <- all_pairs[bundle_name.y != "meningitis_gbs" 
                       | (bundle_name.y == "meningitis_gbs" & mean.x > mean.y)
                       | (bundle_name.y == "meningitis_gbs" & mean.x < mean.y & age_mid <=90/365)]
cols_keep <- c("bundle_name.x", "bundle_name.y","mean.x", "mean.y", "standard_error.x", "standard_error.y", merge_cols)
all_pairs <- all_pairs[, cols_keep, with = F]
# keep only group review data
all_pairs_copy <- copy(all_pairs)
all_pairs <- all_pairs[group_review == 1]
all_pairs$cv_suspected <- NULL
all_pairs$cv_confirmed <- NULL
all_pairs$cv_probable <- NULL

# save

fwrite(all_pairs, paste0(model_dir, "crosswalk_in.csv"))
for (name in unique(bundle_map$bundle_name_short)) write.csv(data_list[[name]], paste0(model_dir, "crosswalk_in_", name, ".csv"))

# all_pairs <- read.csv(paste0(model_dir, "crosswalk_in.csv"))
# for (i in 1:5) data_list[[i]] <- read.csv(paste0(model_dir, "crosswalk_in_list_", i, ".csv"))
preds_list <- list()
for (name in unique(bundle_map$bundle_name_short)){
  preds_list[[name]] <- main(model_dir = model_dir,
                             encode_cols = encode_cols,
                             linear_covs = linear_covs,
                             ref_pathogen = name,
                             spline_covs = spline_covs,
                             read_cache = F)
}

# save crosswalk versions
for (name in unique(bundle_map$bundle_name_short)){
  cv <- copy(preds_list[[name]])
  og_data <- copy(data_list[[name]])
  cv[, mean:= mean_predicted]
  # NOTE this SE is very high
  cv[, standard_error:= se_predicted]
  cv[standard_error>1, standard_error := 1]
  cv[!is.na(standard_error), `:=` (upper = NA, lower = NA, uncertainty_type_value = NA)] # remove UI when new SE present
  cv <- cv[,names(og_data),with=F]
  # do final neatening for upload 
  cv <- find_nondismod_locs(cv)
  cv <- cv[group_review == 1 | is.na(group_review)]
  setnames(cv, c("cv_meningitis_meningo", "cv_meningitis_pneumo", "cv_meningitis_hib"), c("cv_meningo", "cv_pneumo", "cv_hib"))
  # upload to the bundle 
  write.xlsx(cv, paste0(model_dir, name, "_for_upload.xlsx"), sheetName = "extraction")
  description <- paste('SE fix, implement denominator crosswalk, logit transform, age spline')
  id <- bundle_map[bundle_name_short==name]$bundle_id
  bv_id <- bv_tracker[bundle_id == id & current_best == 1]$bundle_version
  result <- save_crosswalk_version(bundle_version_id = bv_id,
                                   paste0(model_dir, name, "_for_upload.xlsx"), 
                                   description = description
  )
  print(sprintf('Request status: %s', result$request_status))
  print(sprintf('Request ID: %s', result$request_id))
  print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
  
  if (result$request_status == "Successful") {
    df_tmp <- data.table(bundle_id = id,
                         bundle_version_id = bv_id,
                         crosswalk_version = result$crosswalk_version_id, 
                         parent_crosswalk_version = NA,
                         is_bulk_outlier = 0,
                         filepath = paste0(out_dir, sex_model_name, "_for_upload.xlsx"),
                         current_best = 1,
                         date = date,
                         description = description)
    
    cv_tracker <- as.data.table(read.xlsx(paste0(dir_2020, 'crosswalk_version_tracking.xlsx')))
    cv_tracker[bundle_id == id, current_best := 0]
    cv_tracker <- rbind(cv_tracker, df_tmp, fill = T)
    write.xlsx(cv_tracker, paste0(dir_2020, 'crosswalk_version_tracking.xlsx'))
  }
}
 
# age split the crosswalked data 
for (name in unique(bundle_map$bundle_name_short)){
  cv <- copy(preds_list[[name]])
  og_data <- copy(data_list[[name]])
  cv[, mean:= mean_predicted]
  # NOTE this SE is very high
  cv[, standard_error:= se_predicted]
  cv[!is.na(standard_error), `:=` (upper = NA, lower = NA, uncertainty_type_value = NA)] # remove UI when new SE present
  cv <- cv[,names(og_data),with=F]
  # do final neatening for upload 
  cv <- find_nondismod_locs(cv)
  cv <- cv[group_review == 1 | is.na(group_review)]
  setnames(cv, c("cv_meningitis_meningo", "cv_meningitis_pneumo", "cv_meningitis_hib"), c("cv_meningo", "cv_pneumo", "cv_hib"))
  # age split
  id <- bundle_map[bundle_name_short==name]$bundle_id
  # if (id == 37){
  #   age_start_vector <- c(0, 5.0, 20.0, 40.0, 60.0)
  #   age_end_vector <- c(4.99, 19.99, 39.99, 59.99, 99.99)
  # } else {
  #   age_start_vector <- c(0, 1.0, 5.0, 20.0, 40.0, 60.0)
  #   age_end_vector <- c(0.99, 4.99, 19.99, 39.99, 59.99, 99.99)
  # }
  age_start_vector <- c(0, 28/365, 1.0, 5.0, 20.0, 40.0, 60.0)
  age_end_vector <- c(27/365, 0.99, 4.99, 19.99, 39.99, 59.99, 99.99)
  print(id)
  cv$age_group_id <- NULL
  age_split_dt <- age_split_data(xwalk_final_dt = cv, 
                                 gbd_round_id = gbd_round_id, 
                                 ds = ds, 
                                 bundle = id,
                                 age_start_vector,
                                 age_end_vector)

  # upload to the bundle 
  write.xlsx(age_split_dt, paste0(model_dir, name, "_age_split_for_upload.xlsx"), sheetName = "extraction")
  description <- paste('include neonates, age split with clin pattern on parent, SE fix denom xwalk, logit transform, age spline')
  bv_id <- bv_tracker[bundle_id == id & current_best == 1]$bundle_version
  result <- save_crosswalk_version(bundle_version_id = bv_id,
                                   paste0(model_dir, name, "_age_split_for_upload.xlsx"), 
                                   description = description
  )
  print(sprintf('Request status: %s', result$request_status))
  print(sprintf('Request ID: %s', result$request_id))
  print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
  
  if (result$request_status == "Successful") {
    df_tmp <- data.table(bundle_id = id,
                         bundle_version_id = bv_id,
                         crosswalk_version = result$crosswalk_version_id, 
                         parent_crosswalk_version = NA,
                         is_bulk_outlier = 0,
                         filepath = paste0(out_dir, sex_model_name, "_for_upload.xlsx"),
                         current_best = 1,
                         date = date,
                         description = description)
    
    cv_tracker <- as.data.table(read.xlsx(paste0(dir_2020, 'crosswalk_version_tracking.xlsx')))
    cv_tracker[bundle_id == id, current_best := 0]
    cv_tracker <- rbind(cv_tracker, df_tmp, fill = T)
    write.xlsx(cv_tracker, paste0(dir_2020, 'crosswalk_version_tracking.xlsx'))
  }
  
  # carry outliers 
  age_split_dt <- carry_outliers_cv_to_cv(cv_new = result$crosswalk_version_id, 
                                          acause = "meningitis", 
                                          date = date)
}
