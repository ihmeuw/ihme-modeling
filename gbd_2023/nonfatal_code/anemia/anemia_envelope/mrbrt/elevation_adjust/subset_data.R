subset_elevation_data <- function(input_df,
                                  elevation_df,
                                  elevation_cat_threshold = 3, 
                                  anemia_vec,
                                  anemia_cat_threshold = 3){
  e_df <- copy(elevation_df)
  e_df$num_surveys <- 0
  
  a_df <- data.table(anemia_category = anemia_vec,
                     num_surveys = 0)
  
  df <- copy(input_df)
  df <- df[order(-sample_size.ref,elevation_number_cat)]
  
  final_df <- data.table()
  
  nid_vec <- unique(df$nid)
  for(i in nid_vec){
    temp_df <- df[nid==i]
    current_elevation_cats <- unique(temp_df$elevation_number_cat)
    current_anemia_cats <- unique(temp_df$anemia_category)
    
    e_vec <- as.vector(e_df[elevation_number_cat %in% current_elevation_cats, num_surveys])
    logic_e_vec <- e_vec >= elevation_cat_threshold
    
    a_vec <- as.vector(a_df[anemia_category %in% current_anemia_cats, num_surveys])
    logic_a_vec <- a_vec >= anemia_cat_threshold
    
    if(!(all(logic_e_vec)) || !(all(logic_a_vec))){
      final_df <- rbindlist(list(final_df,temp_df),use.names = T,fill = T)
      e_df <- e_df[elevation_number_cat %in% current_elevation_cats, num_surveys := num_surveys+1]
      a_df <- a_df[anemia_category %in% current_anemia_cats, num_surveys := num_surveys+1]
    }
  }
  return(final_df)
}
