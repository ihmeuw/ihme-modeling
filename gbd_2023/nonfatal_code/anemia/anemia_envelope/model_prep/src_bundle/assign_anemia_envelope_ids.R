
# source libraries --------------------------------------------------------

library(data.table)
library(stringr)

# load in parameter maps --------------------------------------------------

add_df_columns <- function(input_df, release_id){
  df <- copy(input_df)
  cols <- colnames(df)
  
  elevation_map <- fread(file.path(getwd(), "model_prep/param_maps/elevation_category_map.csv"))
  anemia_map <- fread(file.path(getwd(), "model_prep/param_maps/anemia_map.csv"))
  raw_prev_map <- fread(file.path(getwd(), "model_prep/param_maps/raw_prev_map.csv"))
  
  age_df <- get_age_metadata(release_id = release_id)
  
  req_cols <- c("cluster_altitude", "cv_pregnant")
  if(all(req_cols %in% cols)){
    df <- df[!(is.na(cluster_altitude)) & cluster_altitude < max(elevation_map$end_elevation)]
    for(r in 1:nrow(elevation_map)){
      elevation_id <- as.character(elevation_map[r,cluster_cat])
      low_val <- as.numeric(elevation_map[r,start_elevation])
      high_val <- as.numeric(elevation_map[r,end_elevation])
      
      df <- df[cluster_altitude >= low_val & cluster_altitude < high_val, cluster_elevation_id := elevation_id]
    }
    for(a in 1:nrow(age_df)){
      age_id <- as.numeric(age_df[a,age_group_id])
      low_age <- as.numeric(age_df[a,age_group_years_start])
      high_age <- as.numeric(age_df[a,age_group_years_end])
      
      df <- df[age_year >= low_age & age_year < high_age, age_group_id := age_id]
    }
    for (r in seq_len(nrow(raw_prev_map))) {
      prev_id <- raw_prev_map$raw_prev_category[r]
      low_val <- raw_prev_map$lower_prev[r]
      high_val <- raw_prev_map$upper_prev[r]
      
      df <- df[var %like% "anemia" & mean >= low_val & mean < high_val, raw_prev_id := prev_id]
    }
    
    df <- merge.data.table(df,anemia_map,by = c("age_group_id","sex_id","cv_pregnant"))
  }
  return(df)
}