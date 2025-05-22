##' ***************************************************************************
##' Title: 02_crosswalk.R
##' Purpose: Adjust cv_dx_genetic to reference cv_dx_chemical
##' ***************************************************************************

Sys.umask(mode = 002)



crosswalk_and_adjust <- function(bun_id, measure_name = 'prevalence', trim_percent, out_dir, offset = 1e-7){
  
  ############# These are the crosswalk packages. Make sure you are running the most updated R singularity image or this will fail to load
  library(reticulate)
  library(crosswalk002, lib.loc = "FILEPATH")
  library(dplyr)
  library(data.table)
  ###############################
  cv <- "cv_dx_chemical"
  df <- read.xlsx(paste0("FILEPATH")) %>% as.data.table
  df_nonprev <- df[measure != measure_name]
  df <- df[measure == measure_name]
  df$row_id <- paste0("row", 1:nrow(df))
  cols_to_drop <- c('location_id', 'sex', 'age_start', 'age_end', 'year_start', 'year_end', 'mean', 'standard_error', 'sample_size', 'cases','upper','lower', cv)
  df_other_cols <- df %>% select(-all_of(cols_to_drop)) # for merging back on at the end
  cols_to_keep <- c(cols_to_drop, 'row_id')
  df <- df[, ..cols_to_keep]
  df <- df[cv_dx_chemical == 1, obs_method := 'refvar']
  df <- df[cv_dx_chemical == 0, obs_method := 'altvar']
  df <- df[sex=='Male', sex_id := 1]
  df <- df[sex=='Female', sex_id := 2]
  df$sex <- NULL
  
  
  ref_data <- df[obs_method == 'refvar']
  nrow(ref_data)
  setnames(ref_data, c('mean', 'standard_error'), c('mean_ref', 'se_ref'))
  alt_data <- df[obs_method == 'altvar']
  nrow(alt_data)
  setnames(alt_data, c('mean', 'standard_error'), c('mean_alt', 'se_alt'))

  match_cols <- c('location_id', 'sex_id', 'age_start', 'age_end', 'year_start', 'year_end')
  df_matched <- as.data.table(left_join(ref_data, alt_data, by = match_cols)) 
  
  #Remove any rows with no matched alternative observation or where means are zero
  df_matched <- df_matched[!(is.na(mean_alt))]
  df_matched <- df_matched[!(mean_alt == 0 | mean_ref == 0)]
  print(paste0('df_matched has ', nrow(df_matched), ' rows'))
  
  df_matched <- setnames(df_matched, colnames(df_matched), gsub('.x', '_ref', colnames(df_matched), fixed = TRUE))
  df_matched <- setnames(df_matched, colnames(df_matched), gsub('.y', '_alt', colnames(df_matched), fixed = TRUE))
  
  dat_diff <- as.data.frame(cbind(delta_transform(mean = df_matched$mean_alt + offset, 
                                                  sd = df_matched$se_alt,
                                                  transformation = "linear_to_logit" ), 
                                  delta_transform(mean = df_matched$mean_ref + offset, 
                                                  sd = df_matched$se_ref, 
                                                  transformation = "linear_to_logit")))
  names(dat_diff) <- c("mean_alt", "se_alt", "mean_ref", "se_ref")
  df_matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(df = dat_diff, 
                                                                   alt_mean = "mean_alt", alt_sd = "se_alt",
                                                                   ref_mean = "mean_ref", ref_sd = "se_ref" )
  
  print("Your df_matched now has 2 new columns of the log difference between alt and ref observations. Note that mean and SE are not log transformed in this df.")
  

  df_matched <- df_matched[, group_id := location_id]
  # df_matched <<- df_matched # The CW functions can only access global variables so I'm assigning these to be global variables
  
  print("Creating CWData")
  
  df1 <- CWData(
    df = df_matched,          # dataset for metaregression
    obs = "logit_diff",       # column name for the observation mean
    obs_se = "logit_diff_se", # column name for the observation standard error
    alt_dorms = "obs_method_alt",     # column name of the variable indicating the alternative method
    ref_dorms = "obs_method_ref",     # column name of the variable indicating the reference method
    covs = list("sex_id"),     # names of columns to be used as covariates later
    study_id = "group_id",    # name of the column indicating group membership, usually the matching groups
    add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
  )
  py_save_object(object = df1, filename = paste0(out_dir, "df1", bun_id, ".pkl"), pickle = "dill")
  
  # df1 <<- df1 # The CW functions can only access global variables so I'm assigning these to be global variables
  
  fit1 <- CWModel(
    cwdata = df1,            # object returned by `CWData()`
    obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
    use_random_intercept = FALSE,
    cov_models = list(
      CovModel(cov_name = "intercept"),
      CovModel(cov_name = "sex_id")),
    gold_dorm = "refvar",   # the level of `alt_dorms` that indicates it's the gold standard
    inlier_pct = 1- trim_percent
  )
  py_save_object(object = fit1, filename = paste0(out_dir, "fit1", bun_id, ".pkl"), pickle = "dill")
  
  
  df_result <- fit1$create_result_df()
  
  print(paste0('saving betas ',  out_dir, 'betas_', bun_id,'.csv'))
  write.csv(df_result, paste0(out_dir, 'betas_', bun_id,'.csv'))
  
  
  print(paste0('Original df has ', nrow(df), ' and ', length(unique(df$nid)), ' unique NIDs'))
  adjust_nonzeros <- copy(df[mean != 0]) 
  adjust_zeros <- copy(df[mean == 0 & obs_method == 'altvar']) # this is for adjusting the standard error of non-reference mean zero rows.
  no_adjust <- copy(df[mean == 0 & obs_method == 'refvar'])
  
  
  
  adjust_nonzeros[, c("mean_adj", "se_adj", "pred_logit", "pred_se_logit", "data_id")] <- adjust_orig_vals(
    fit_object = fit1, # object returned by `CWModel()`
    df = adjust_nonzeros,
    orig_dorms = "obs_method",
    orig_vals_mean = "mean",
    orig_vals_se = "standard_error",
    data_id = "row_id"   # optional argument to add a user-defined ID to the predictions
  )
  
  
  
  adjust_zeros <- adjust_zeros[,mean := offset]
  adjust_zeros[, c("mean_adj", "se_adj", "pred_logit", "pred_se_logit", "data_id")] <- adjust_orig_vals(
    fit_object = fit1, # object returned by `CWModel()`
    df = adjust_zeros,
    orig_dorms = "obs_method",
    orig_vals_mean = "mean",
    orig_vals_se = "standard_error",
    data_id = "row_id"   # optional argument to add a user-defined ID to the predictions
  )
  adjust_zeros <- adjust_zeros[, mean_adj := 0]
  
  no_adjust[, mean_adj := mean]
  no_adjust[, se_adj := standard_error]
  adjusted_df <- rbind(adjust_nonzeros, adjust_zeros, no_adjust, fill = T, use.names =T)
  print(paste0('Crosswalked df has ', nrow(adjusted_df), ' and ', length(unique(adjusted_df$nid)), ' unique NIDs'))
  
  loc_dt <- get_location_metadata(35, gbd_round_id = 7)
  
  
  plots <- import("crosswalk.plots")
  
  plots$funnel_plot(
    cwmodel = fit1, 
    cwdata = df1,
    binary_variables = list(sex_id = 1),
    obs_method = 'altvar',
    plot_note = paste0('G6PD crosswalk (ref: chemical test; alt: non chemical test; trim ', trim_percent, '%; male'), 
    plots_dir = out_dir,
    file_name = paste0('funnel_plot_', bun_id, '_male'),
    write_file = TRUE
  )
  
  plots$funnel_plot(
    cwmodel = fit1, 
    cwdata = df1,
    binary_variables = list(sex_id = 2),
    obs_method = 'altvar',
    plot_note = paste0('G6PD crosswalk (ref: chemical test; alt: non chemical test; trim ', trim_percent, '%; female'), 
    plots_dir = out_dir,
    file_name = paste0('funnel_plot_', bun_id, '_female'),
    write_file = TRUE
  )
  
  
  ## Rename mean_adj and se_adj to mean and standard_error
  print(paste0('Pre crosswalk cv_dx_chemical mean summary: '))
  print(summary(adjusted_df[cv_dx_chemical == 0]$mean))
  print(paste0('Post crosswalk cv_dx_chemical mean summary: '))
  print(summary(adjusted_df[cv_dx_chemical == 0]$mean_adj))
  
  print(paste0('Pre crosswalk cv_dx_chemical, mean zero, SE summary: '))
  print(summary(adjusted_df[cv_dx_chemical == 0 & mean_adj ==0]$standard_error))
  print(paste0('Post crosswalk cv_dx_chemical, mean zero, SE summary: '))
  print(summary(adjusted_df[cv_dx_chemical == 0 & mean_adj ==0]$se_adj))
  
  
  adjusted_df[, c('mean', 'standard_error') := NULL]
  adjusted_df[,lower := mean_adj - 1.96 * se_adj]
  adjusted_df[,upper := mean_adj + 1.96 * se_adj] 
  setnames(adjusted_df, c('mean_adj', 'se_adj'), c('mean', 'standard_error'))
  adjusted_df[, sex := ifelse(sex_id == 1, 'Male', 'Female')]
  
  ## Bring back the non-prevalence data and all the original columns
  df_other_cols$sex_id <- NULL
  full <- merge(adjusted_df, df_other_cols, by = 'row_id')
  full <- rbind(full, df_nonprev, fill = T)
  write.xlsx(full,
             file = paste0("FILEPATH"),
             row.names = FALSE, sheetName = "extraction")
  print('Crosswalked file written out to filepath')
  
}




