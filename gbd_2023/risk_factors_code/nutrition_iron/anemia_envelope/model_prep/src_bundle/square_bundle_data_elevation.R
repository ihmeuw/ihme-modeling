
# source libraries --------------------------------------------------------

library(data.table)

# main square function ----------------------------------------------------

square_bundle_data_elevation_adj <- function(input_df, measure_adj_df, var_map){
  df <- copy(input_df)

  #create a data table with all of the potential combos
  id_cols <- c(
    'nid', 'year_id', 'sex_id', 'orig_age_start', 'orig_age_end', 'location_id',
    'cv_pregnant', 'site_memo'
  )
  
  df <- expand_by_elevation_adj(
    input_df = df,
    measure_adj_df = measure_adj_df,
    id_cols = id_cols
  )
  
  df <- assign_reference_elevation_adj_value(
    input_df = df, 
    id_cols = id_cols,
    var_map = var_map
  )
  
  return(df)
}

# create all potential combos of elevation adj/measure combos -------------

expand_by_elevation_adj <- function(input_df, measure_adj_df, id_cols){
  df <- copy(input_df)
  
  unique_elevation_df <- df |>
    dplyr::select(tidyselect::all_of(id_cols)) |>
    setDT()
  
  square_elevation_df <- data.table()
  for(c in colnames(measure_adj_df)){
    for(measure_val in measure_adj_df[[c]]){
      temp_df <- copy(unique_elevation_df)
      temp_df$var <- measure_val
      square_elevation_df <- rbindlist(
        list(square_elevation_df, temp_df),
        use.names = TRUE,
        fill = TRUE
      )
    }
  }
  square_elevation_df <- unique(square_elevation_df)
  
  square_elevation_df <- merge.data.table(
    x = df,
    y = square_elevation_df,
    by = c(id_cols, 'var'),
    all = TRUE
  )
  
  return(square_elevation_df)
}

# assign the row_id that will help impute this value ----------------------

assign_reference_elevation_adj_value <- function(input_df, id_cols, var_map){
  df <- copy(input_df)
  
  i_vec <- which(is.na(df$row_id))
  inverse_i_vec <- setdiff(seq_len(nrow(df)), i_vec)
  
  to_impute_df <- df |> 
    dplyr::slice(i_vec) |>
    dplyr::mutate(row_id = NULL) |>
    dplyr::mutate(measure_row_id = NULL) |>
    dplyr::mutate(imputed_measure_row = NULL)
  
  df <- df |>
    dplyr::slice(inverse_i_vec) |>
    setDT()
  
  matched_df <- data.table()
  
  value_cols <- c(
    'var', 'val', 'standard_error', 'variance', 'upper', 'lower'
  )
  
  measure_value_cols <- paste0(value_cols, ".reference_measure")
  
  keep_df_cols <- c(
    id_cols, value_cols, measure_value_cols, 'row_id', 'measure_row_id',
    'imputed_measure_row'
  )
  fill_cols <- setdiff(colnames(df), keep_df_cols)
  
  for(i in names(var_map)){
    subset_impute_df <- to_impute_df |>
      dplyr::slice(which(to_impute_df$var == i)) |>
      setDT()
    
    for(v in var_map[[i]]){
      if(nrow(subset_impute_df) > 0){
        subset_df <- df |>
          dplyr::slice(which(df$var == v)) |>
          dplyr::select(dplyr::all_of(keep_df_cols)) |>
          setDT()
        
        merge_df <- merge.data.table(
          x = subset_impute_df,
          y = subset_df,
          by = id_cols,
          suffixes = c("", ".reference_elevation_adj"),
          all.x = TRUE
        )
        i_vec <- which(!(is.na(merge_df$val.reference_elevation_adj)) |
                         merge_df$imputed_measure_row == 1)
        inverse_i_vec <- setdiff(seq_len(nrow(merge_df)), i_vec)
        matched_df <- rbindlist(
          list(matched_df, merge_df[i_vec, ]),
          use.names = TRUE, 
          fill = TRUE
        )
        subset_df <- merge_df[inverse_i_vec, ]
      }
    }
  }
  matched_df$imputed_elevation_adj_row <- 1
  setnames(
    x = matched_df,
    old = 'row_id', 
    new = 'elevation_adj_row_id'
  )
  matched_df$temp_row_id <- seq_len(nrow(matched_df))
  matched_df$row_id <- paste(
    matched_df$elevation_adj_row_id, 
    matched_df$temp_row_id, 
    sep = "_"
  )
  matched_df$temp_row_id <- NULL
  
  df <- rbindlist(
    list(df, matched_df),
    use.names = TRUE,
    fill = TRUE
  )
  
  df <- df |>
    dplyr::group_by_at(id_cols) |>
    tidyr::fill(fill_cols, .direction = "down") |>
    setDT()
  
  drop_cols <- which(
    grepl("\\.reference_measure\\.reference_elevation_adj", colnames(df))
  )
  drop_cols <- colnames(df)[drop_cols]
  for(c in drop_cols){
    df[[c]] <- NULL
  }
  
  return(df)
}
