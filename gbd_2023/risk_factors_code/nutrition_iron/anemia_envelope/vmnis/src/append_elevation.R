
# source libraries --------------------------------------------------------

library(data.table)

# append on elevation data ------------------------------------------------

append_elevation <- function(input_df, gbd_rel_id){
  df <- copy(input_df)
  
  elevation_dirs <- c(
    "FILEPATH/ihme_subnat_elevation_means_",
    "FILEPATH/admin0_elevation_means_"
  )
  
  loc_column_name <- c('loc_id', 'location_id')
  
  elevation_df <- data.table()
    
  for(yr in unique(df$year_end)){
    for(i in 1:length(elevation_dirs)){
      file_name <- paste0(
        elevation_dirs[i],
        yr,
        ".csv"
      )
      temp_df <- fread(file_name)
      temp_df$year_end <- yr
      
      keep_cols <- c(loc_column_name[i], 'weighted_mean_elevation', 'year_end')
      temp_df <- temp_df[, keep_cols, with = FALSE]
      
      if('loc_id' %in% colnames(temp_df)) setnames(temp_df, 'loc_id', 'location_id')
      
      elevation_df <- rbindlist(list(elevation_df, temp_df), use.names = T, fill = T)
    }
  }
  
  
  df <- merge.data.table(
    x = df,
    y = elevation_df,
    by.x = c('location_id', 'year_end'),
    by.y = c(loc_column_name[i], 'year_end'),
    all.x = TRUE
  )
  
  setnames(df, 'weighted_mean_elevation', 'cluster_altitude')
  
  return(df)
}
