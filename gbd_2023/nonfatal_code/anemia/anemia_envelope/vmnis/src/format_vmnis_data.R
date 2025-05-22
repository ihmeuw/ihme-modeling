
# source libraries --------------------------------------------------------

library(data.table)

# format final vmnis data set ---------------------------------------------

format_vmnis_data <- function(input_df){
  df <- copy(input_df)
  
  setnames(df, "underlying_nid", "nid")
  df$underlying_nid <- NA_character_
  
  if(!("seq" %in% colnames(input_df))){
    df$seq <- seq_len(nrow(df))
  }
  
  return(df)
}
