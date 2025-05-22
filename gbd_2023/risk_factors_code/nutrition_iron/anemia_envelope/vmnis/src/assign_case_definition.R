
# source libraries --------------------------------------------------------

library(data.table)

# assign case definition --------------------------------------------------

assign_case_definition <- function(input_df){
  df <- copy(input_df)
  
  me_id_list <- list(
    mean_adj_hb = c("mean hgb"), 
    total_anemia_prev = c("total anaemia"),
    mild_anemia_prev = c("mild anaemia"),
    mod_anemia_prev = c("moderate anaemia"), 
    sev_anemia_prev = c("severe anaemia")
  ) 
  
  df$me_type <- NA_character_
  for(i in names(me_id_list)){
    i_vec <- grepl(me_id_list[[i]], df$case_name, ignore.case = TRUE)
    df$me_type[i_vec] <- i
  }
  
  return(df)
}
