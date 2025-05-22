
# source libraries --------------------------------------------------------

library(data.table)

# put in placeholder values for rows that will be imputed -----------------

create_placeholders_for_imputed_rows <- function(input_df){
  df <- copy(input_df)
  
  i_vec <- which(
    df$imputed_measure_row == 1 |
      df$imputed_elevation_adj_row == 1
  )
  
  update_cols <- c(
    'val', 'upper', 'lower', 'standard_error', 'variance'
  )
  
  for(c in update_cols){
    df[[c]][i_vec] <- 0
  }
  
  return(df)
}
