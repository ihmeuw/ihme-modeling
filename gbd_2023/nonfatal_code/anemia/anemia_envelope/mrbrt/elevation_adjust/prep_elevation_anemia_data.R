library(data.table)
library(haven)
library(stringr)

winnower_dir <- "FILEPATH"
dta_file_vec <- list.files(winnower_dir,full.names = T)
elevation_map <- fread(file.path(getwd(), "mrbrt/elevation_adjust/elevation_category_map.csv"))
anemia_map <- fread(file.path(getwd(), "mrbrt/elevation_adjust/anemia_map.csv"))

age_df <- ihme::get_age_metadata(release_id = 9)

main_function <- function(file_vec){
  diagnostic_df <- data.table()
  for(i in file_vec){
    print(i)
    df <- as.data.table(read_dta(i))
    df <- add_df_columns(df)
    write_dta(data = df, path = i)
    
    if("cluster_elevation_id" %in% colnames(df)){
      temp_df <- df[,.(file_name = i,num_rows = .N),
                    .(cluster_elevation_id, anemia_category)]
      
      diagnostic_df <- rbindlist(list(diagnostic_df,temp_df),use.names = T,fill = T)
    }
  }
  return(diagnostic_df)
}

add_df_columns <- function(input_df){
  df <- copy(input_df)
  cols <- colnames(df)
  req_cols <- c("cluster_altitude", "cv_pregnant", "anemia_anemic_brinda", "anemia_anemic_raw", "anemia_anemic_adj", "anemia_anemic_who")
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
    df <- merge.data.table(df,anemia_map,by = c("age_group_id","sex_id","cv_pregnant"))
  }
  return(df)
}

diagnostic_df <- main_function(dta_file_vec)
write.csv(diagnostic_df,file.path(getwd(), "mrbrt/elevation_adjust/diagnostic_counts.csv"),row.names = F)