
# source libraries --------------------------------------------------------

library(data.table)
library(stringr)

# format bundles for st-gpr -----------------------------------------------

format_stgpr_bundle <- function(input_df, release_id, hot_fix = FALSE){
  invisible(sapply(
    list.files(
      path = file.path(getwd(), "model_prep/src_bundle/format_helper_functions/"),
      pattern = "*\\.R$",
      full.names = TRUE
    ),
    source
  ))
  
  df <- copy(input_df)
  
  validate_columns(colnames(df))
  df$nid <- update_nid(u_nid = df$underlying_nid, nid = df$nid)
  df <- add_sex_id(input_df = df)
  df$measure <- update_measure(hb_variable_col = df$var)
  df$source_type <- update_source_type(source_col = df$source_type)
  df <- get_location_id(input_df = df, gbd_rel_id = release_id)
  df <- update_gbd2023_location_ids(input_df = df)
  df <- split_ethiopia_subnats(input_df = df)
  df <- reassign_gbd_locations(input_df = df, release_id = release_id)
  df <- assign_year_id(input_df = df)
  df$elevation_year <- assign_elevation_year(df$year_id)
  df <- assign_age_group_ids(input_df = df)
  df$cv_trimester <- update_trimester(trimester_col = df$cv_trimester)
  df$is_outlier <- 0 
  if(hot_fix){
    df <- mean_hot_fix(input_df = df)
  }
  df$val <- assign_mean(mean_vec = df$mean, measure_vec = df$measure)
  df <- impute_missing_statistics(input_df = df, se_hot_fix = hot_fix)
  df <- subset_columns(input_df = df)
  df <- validate_column_size(input_df = df)
  
  return(df)
}
