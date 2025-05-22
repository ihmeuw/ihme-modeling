
# get location IDs --------------------------------------------------------

get_location_id <- function(input_df, gbd_rel_id){
  df <- copy(input_df)
  
  loc_df <- get_location_metadata(
    location_set_id = 35,
    release_id = gbd_rel_id
  )
  
  loc_df <- loc_df[,.(location_id, ihme_loc_id)]
  
  df$ihme_loc_id <- clean_ihme_loc_id(df$ihme_loc_id)
  
  i_vec <- which(is.na(df$location_id) & !(is.na(df$ihme_loc_id)))
  inverse_i_vec <- setdiff(seq_len(nrow(df)), i_vec)
  
  to_update_df <- df[i_vec, ]
  df <- df[inverse_i_vec, ]
  to_update_df$location_id <- NULL
  
  to_update_df <- merge.data.table(
    x = to_update_df,
    y = loc_df,
    by = "ihme_loc_id",
    all.x = TRUE
  )
  
  df <- rbindlist(
    list(df, to_update_df),
    use.names = TRUE, 
    fill = TRUE
  )
  
  df <- reassign_gbd_locations(df, gbd_rel_id)
  
  return(df)
}

# get missing ihme loc id -------------------------------------------------

update_missing_ihme_loc_id <- function(input_df, loc_df){
  df <- copy(input_df)
  
  i_vec <- which(is.na(df$location_id) & (is.na(df$ihme_loc_id) | df$ihme_loc_id == "")
                 & (!(is.na(df$file_path)) | df$file_path != ""))
  df$ihme_loc_id[i_vec] <- unlist(lapply(df$file_path[i_vec], get_ihme_loc_id))
  
  return(df)
}

# get ihme loc id from file name ------------------------------------------

get_ihme_loc_id <- function(x){
  file_vec <- unlist(str_split(x, "/"))
  file_name <- file_vec[length(file_vec)]
  return(substr(file_name, 1, 3))
}

# remove spaces/tabs from winnower generate ihme loc id -------------------


clean_ihme_loc_id <- function(vec){
  vec <- str_remove_all(string = vec, pattern = "\\n")
  vec <- str_remove_all(string = vec, pattern = "\\r")
  vec <- str_remove_all(string = vec, pattern = "\\t")
  return(vec)
}

# update missing subnats --------------------------------------------------

update_missing_subnats <- function(input_df){
  df <- copy(input_df)
  
  i_vec <- which(is.na(df$location_id) &
                   !(is.na(df$ihme_loc_id)) &
                   grepl("_", df$ihme_loc_id))
  
  df$location_id[i_vec] <- unlist(lapply(i_vec, function(x){
    return(as.integer(unlist(str_split(df$ihme_loc_id[x], "_"))[2]))
  }))
  
  return(df)
}