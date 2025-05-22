
# source libraries --------------------------------------------------------

library(data.table)

# assign sex ids based on free text ---------------------------------------

assign_sex_id <- function(input_df){
  df <- copy(input_df)
  
  df$sex_id <- NA_integer_
  
  sex_id_list <- list(
    Male = 1,
    Female = 2,
    Both = 3
  )
  
  for(i in names(sex_id_list)){
    i_vec <- df$sex == i
    df$sex_id[i_vec] <- sex_id_list[[i]]
  }
  
  return(df)
}
