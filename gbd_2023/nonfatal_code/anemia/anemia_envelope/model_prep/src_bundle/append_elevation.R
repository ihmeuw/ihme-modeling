
# source libraries --------------------------------------------------------

library(data.table)

# append on elevation data ------------------------------------------------

append_elevation <- function(input_df, gbd_rel_id){
  df <- copy(input_df)
  
  i_vec <- which(is.na(df$cluster_altitude))
  inverse_i_vec <- setdiff(seq_len(nrow(df)), i_vec)
  
  to_update_df <- df[i_vec, ]
  df <- df[inverse_i_vec, ]
  
  if(nrow(to_update_df) > 0){
    
    to_update_df$cluster_altitude <- NULL
    
    elevation_df <- get_elevation_values(
      location_id_vec = unique(to_update_df$location_id),
      year_end_vec = unique(to_update_df$elevation_year)
    )
    
    to_update_df <- merge.data.table(
      x = to_update_df,
      y = elevation_df,
      by = c('location_id', 'elevation_year'),
      all.x = TRUE
    )
    
    setnames(to_update_df, 'weighted_mean_elevation', 'cluster_altitude')
    
    i_vec <- which(!(is.na(to_update_df$cluster_altitude)))
    to_update_df$cluster_altitude_unit[i_vec] <- "m"
  }
  
  df <- rbindlist(
    list(df, to_update_df),
    use.names = TRUE,
    fill = TRUE
  )
  
  return(df)
}

get_elevation_values <- function(location_id_vec, year_end_vec){
  elevation_dirs <- c(
    "FILEPATH/ihme_subnat_elevation_means_",
    "FILEPATH/admin0_elevation_means_"
  )
  
  loc_column_name <- c('loc_id', 'location_id')
  
  elevation_df <- data.table()
  
  for(yr in year_end_vec){
    for(i in 1:length(elevation_dirs)){
      file_name <- paste0(
        elevation_dirs[i],
        yr,
        ".csv"
      )
      temp_df <- fread(file_name)
      temp_df$elevation_year <- yr
      
      keep_cols <- c(loc_column_name[i], 'weighted_mean_elevation', 'elevation_year')
      temp_df <- temp_df[, keep_cols, with = FALSE]
      
      if('loc_id' %in% colnames(temp_df)) setnames(temp_df, 'loc_id', 'location_id')
      
      i_vec <- which(temp_df$location_id %in% location_id_vec)
      temp_df <- temp_df[i_vec, ]
      
      elevation_df <- rbindlist(list(elevation_df, temp_df), use.names = T, fill = T)
    }
  }
  return(elevation_df)
}
