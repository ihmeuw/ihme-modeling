
# reassign location IDs that aren't in GBD 2023 ---------------------------

reassign_gbd_locations <- function(input_df, release_id){
  df <- copy(input_df)
  
  loc_df <- get_location_metadata(location_set_id = 35, release_id = release_id)
  
  i_vec <- which(is.na(df$location_id))
  df$ihme_loc_id[i_vec] <- get_ihme_loc_id(df$file_path[i_vec])
  update_list_vals <- list(
    location_name = unique(df$location_name[i_vec]),
    ihme_loc_id = unique(df$ihme_loc_id[i_vec])
  )
  
  for(i in names(update_list_vals)){
    temp_vec <- update_list_vals[[i]]
    for(j in temp_vec){
      if(j %in% loc_df[[i]]){
        j_index <- match(j, loc_df[[i]])
        j_vec <- which(df[[i]] == j & is.na(df$location_id))
        df$location_id[j_vec] <- loc_df$location_id[j_index]
      }
    }
  }
  
  if(any(is.na(df$location_id))){
    assign("na_loc_df", df[is.na(location_id)], envir = .GlobalEnv)
    stop("Not all locaitons are defined. Please check and rerun.")
  }
  
  return(df)
}