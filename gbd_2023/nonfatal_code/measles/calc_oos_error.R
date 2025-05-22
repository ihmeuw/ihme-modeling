#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:  REDACTED
# Date:    July 2020
# Purpose: Calculate OOS fit statistics for measles incidence model
#***********************************************************************************************************************

calculate_oos_error <- function(preds.dir, input.data.dir, output.dir, folds){

  draw_summaries <- function(x, new_col, cols_sum, se = F) {
    
    x[, (paste0(new_col, "_lower")) := apply(.SD, 1, quantile, probs = .025, na.rm =T), .SDcols = (cols_sum)]
    x[, (paste0(new_col, "_mean"))  := rowMeans(.SD), .SDcols = (cols_sum)]
    x[, (paste0(new_col, "_upper")) := apply(.SD, 1, quantile, probs = .975, na.rm =T), .SDcols = (cols_sum)]
    if (se == T) x[, paste0(new_col, "_se") := (get(paste0(new_col, "_upper"))-get(paste0(new_col, "_lower")))/3.92]
    if (se == T) x[get(paste0(new_col, "_se")) == 0, paste0(new_col, "_se") := apply(.SD, 1, sd, na.rm=T), .SDcols = (cols_sum)]
    x <- x[, !cols_sum, with = F]
    return(x)
  }
  
  ## read in input data, IS data, OOS data
  input_data <- fread(file.path(input.data.dir, "case_regression_input.csv"))
  
  ## read in in sample predictions. note this will have additional rows because we are predicting out to all location years, not just those in the input data
  in_sample <- fread(file.path(preds.dir, "01_case_predictions_from_model.csv"))
  
  out_of_sample <- lapply(1:n_folds, function(i) {
    fread(file.path(preds.dir, paste0("01_case_predictions_from_model_", i, ".csv")))
  }) %>% rbindlist
  
  # Note that there is one out of sample prediction per row of original input data
  # The rbinded out_of_sample data frame contains OOS predictions for each row, since each i file was predicted using a model fit without that whole fold
  nrow(input_data) == nrow(out_of_sample)
  
  #ID draw cols to drop because only want to look and IS and OOS point prediction generated using the REs rather than the IS draws generated using the std RE
  draw_cols <- subset(colnames(in_sample), colnames(in_sample) %like% "_draw_")
  
  #remove draw cols (only present in IS file)
  in_sample_summ <- in_sample[, !draw_cols, with = FALSE]
  
  #now merge the OS predictions on to the input data for comparison
  df_input_oos <- merge(input_data, out_of_sample[, c("location_id", "year_id", "oos_mean")], by = c("location_id", "year_id"))
  df_input_oos_is <- merge(df_input_oos, in_sample_summ, by=c("location_id", "year_id"), all.x = T, all.y = FALSE)
  
  #calc rmse, mae, mean error both IS and OOS
  rmse <- function(predicted, actual, remove_na){
    sqrt(mean((predicted - actual)^2, na.rm = remove_na))
  }
  mae <- function(predicted, actual, remove_na){
    mean(abs(predicted-actual), na.rm = remove_na)
  }
  mean_err <- function(predicted, actual, remove_na=F){
    mean(predicted-actual, na.rm = remove_na)
  }
  
  rmse_oos <- rmse(predicted=df_input_oos_is$oos_mean, actual=df_input_oos_is$cases, remove_na=F)
  mae_oos <- mae(predicted=df_input_oos_is$oos_mean, actual=df_input_oos_is$cases, remove_na = F)
  mean_err_oos <- mean_err(predicted = df_input_oos_is$oos_mean, actual=df_input_oos_is$cases)
  
  rmse_is <- rmse(predicted=df_input_oos_is$is_mean, actual=df_input_oos_is$cases, remove_na = F)
  mae_is <- mae(predicted=df_input_oos_is$is_mean, actual=df_input_oos_is$cases, remove_na=F)
  mean_err_is <- mean_err(predicted = df_input_oos_is$is_mean, actual=df_input_oos_is$cases, remove_na=F)
  
  fit_stats <- data.table(measure=c("rmse", "mae", "mean_err"), 
                          in_sample=c(rmse_is, mae_is, mean_err_is), 
                          oos = c(rmse_oos, mae_oos, mean_err_oos))
  fwrite(fit_stats, file.path(output.dir, "inc_model_fit_stats.csv"))
}

