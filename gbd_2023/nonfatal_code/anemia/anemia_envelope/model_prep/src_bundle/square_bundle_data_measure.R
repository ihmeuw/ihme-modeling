
# source libraries --------------------------------------------------------

library(data.table)

# main square function ----------------------------------------------------

square_bundle_data_measure <- function(input_df, measure_adj_df, var_map){
  df <- copy(input_df)
  
  i_vec <- which(is.na(df$site_memo) | df$site_memo == "")
  df$site_memo[i_vec] <- df$location_name[i_vec]
  
  df$row_id <- seq_len(nrow(df))
  
  df$elevation_adj_type <- NA_character_
  for(c in colnames(measure_adj_df)){
    i_vec <- which(grepl(c, df$var))
    df$elevation_adj_type[i_vec] <- c
  }
  
  #create a data table with all of the potential combos
  id_cols <- c(
    'nid', 'year_id', 'sex_id', 'orig_age_start', 'orig_age_end', 'location_id',
    'cv_pregnant', 'site_memo', 'elevation_adj_type'
  )
  
  df <- split_by_measure(
    input_df = df, 
    measure_adj_df = measure_adj_df,
    id_cols = id_cols
  )
  
  df <- assign_reference_meausre_value(
    input_df = df, 
    id_cols = id_cols,
    var_map = var_map
  )
  
  return(df)
}

# split data by measure, where each NID will have all measures ------------

split_by_measure <- function(input_df, measure_adj_df, id_cols){
  df <- copy(input_df)
  
  unique_measure_df <- df[, id_cols, with = F]
  
  square_measure_df <- data.table()
  for(c in colnames(measure_adj_df)){
    temp_df <- unique_measure_df[elevation_adj_type == c]
    for(measure_val in measure_adj_df[[c]]){
      temp_df$var <- measure_val
      square_measure_df <- rbindlist(
        list(square_measure_df, temp_df),
        use.names = TRUE,
        fill = TRUE
      )
    }
  }
  square_measure_df <- unique(square_measure_df)
  square_measure_df <- merge.data.table(
    x = df,
    y = square_measure_df,
    by = c(id_cols, 'var'),
    all = TRUE
  )
  
  return(square_measure_df)
}

# assign how missing adjustment types get imputed -------------------------

assign_reference_meausre_value <- function(input_df, id_cols, var_map){
  df <- copy(input_df)
  
  i_vec <- which(is.na(df$row_id))
  inverse_i_vec <- setdiff(seq_len(nrow(df)), i_vec)
  
  to_impute_df <- df |> 
    dplyr::slice(i_vec) |>
    dplyr::mutate(row_id = NULL)
  
  df <- df |>
    dplyr::slice(inverse_i_vec) |>
    setDT()
  
  matched_df <- data.table()
  
  keep_df_cols <- c(
    id_cols, 'var', 'row_id', 'val', 'standard_error', 'variance', 
    'upper', 'lower'
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
          suffixes = c("", ".reference_measure"),
          all.x = TRUE
        )
        matched_df <- rbindlist(
          list(matched_df, merge_df[which(!(is.na(merge_df$val.reference_measure))), ]),
          use.names = TRUE, 
          fill = TRUE
        )
        subset_df <- merge_df[which(is.na(merge_df$val.reference_measure)), ]
      }
    }
  }
  matched_df$imputed_measure_row <- 1
  setnames(
    x = matched_df,
    old = 'row_id', 
    new = 'measure_row_id'
  )
  matched_df$temp_row_id <- seq_len(nrow(matched_df))
  matched_df$row_id <- paste(
    matched_df$measure_row_id,
    matched_df$temp_row_id, 
    sep = "_"
  )
  matched_df$temp_row_id <- NULL
  
  df$row_id <- as.character(df$row_id)
  df <- rbindlist(
    list(df, matched_df),
    use.names = TRUE,
    fill = TRUE
  )
  
  df <- df |>
    dplyr::group_by_at(id_cols) |>
    tidyr::fill(fill_cols, .direction = "down") |>
    setDT()
  
  return(df)
}
