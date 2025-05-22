library(data.table)

# update for GBD 2023 location IDs ----------------------------------------

update_gbd2023_location_ids <- function(input_df){
  df <- copy(input_df)
  
  df$orig_location_id <- df$location_id
  df$location_id <- nch::convert_2021_locations_to_2023(df$location_id)
  df$location_name <- nch::name_for('location', id = df$location_id)
  
  return(df)
}

# split ethiopia subnats --------------------------------------------------

split_ethiopia_subnats <- function(input_df){
  df <- copy(input_df)
  
  i_vec <- which(df$location_id == 44858)
  inverse_i_vec <- setdiff(seq_len(nrow(df)), i_vec)
  
  eth_df <- df[i_vec, ]
  df <- df[inverse_i_vec, ]
  
  subnat_id_vec <- c(60908, 95069, 94364)
  
  pop_df <- get_population(
    year_id = 2021,
    sex_id = 3,
    age_group_id = 22,
    location_id = subnat_id_vec,
    release_id = 16
  )
  
  pop_sum <- sum(pop_df$population)
  pop_df$ratio <- pop_df$population / pop_sum
  
  for(i in subnat_id_vec){
    temp_df <- copy(eth_df)
    temp_df$location_id <- i
    pop_ratio <- as.numeric(pop_df[location_id == i, ratio])
    temp_df$sample_size <- temp_df$sample_size * pop_ratio
    temp_df$cases <- temp_df$cases * pop_ratio
    df <- rbindlist(
      list(df, temp_df),
      use.names = TRUE, 
      fill = TRUE
    )
  }
  return(df)
}