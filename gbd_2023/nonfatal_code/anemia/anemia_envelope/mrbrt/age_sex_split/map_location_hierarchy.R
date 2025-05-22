
# create map for each row from global to location_id ----------------------

#' @export
map_location_hierarchy <- function(input_df, loc_df, hierarchy_col_names){
  df <- data.table::copy(input_df)
  
  df <- df |>
    dplyr::mutate(
      ihme_loc_id = stringr::str_remove(ihme_loc_id, '\\n')
    )
  
  df <- data.table::merge.data.table(
    x = df,
    y = loc_df[,.(ihme_loc_id, path_to_top_parent)],
    by = 'ihme_loc_id'
  )
  
  for(i in hierarchy_col_names) df[[i]] <- NA_integer_
  
  for(r in seq_len(nrow(df))){
    parent_vec <- as.integer(
      stringr::str_split(
        string = df$path_to_top_parent[r],
        pattern = ",",
        simplify = TRUE
      )
    )
    
    for(i in seq_len(length(parent_vec))){
      df[[hierarchy_col_names[i]]][r] <- parent_vec[i]
    }
  }
  
  return(df)
}

# split input data by most granular region level --------------------------

#' @export
split_input_location_levels <- function(input_df, cascade_dir, hierarchy_col_names){
  df <- data.table::copy(input_df)
  df$pkl_file <- NA_character_
  
  for(r in seq_len(nrow(df))){
    for(c in rev(hierarchy_col_names)){
      if(!(is.na(df[[c]][r]))){
        cascade_file <- file.path(
          cascade_dir,
          'pickles',
          paste0(c, "__", df[[c]][r], ".pkl")
        )
        if(file.exists(cascade_file)){
          df$pkl_file[r] <- cascade_file
          break
        }
      }
    }
  }
  
  return(df)
}
