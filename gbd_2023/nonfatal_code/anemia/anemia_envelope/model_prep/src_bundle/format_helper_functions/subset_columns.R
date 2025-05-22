# subset columns in bundle df ---------------------------------------------

subset_columns <- function(input_df){
  df <- copy(input_df)
  
  bun_cols <- fread(file.path(getwd(), "model_prep/param_maps/bundle_col_names.csv"))
  bun_cols <- as.character(as.vector(bun_cols[keep == 1, df_cols]))
  
  df <- df[, bun_cols, with = FALSE]
  
  return(df)
}