
# source libraries --------------------------------------------------------

library(data.table)

# validate to ensure no columns are over the max limit --------------------

validate_column_size <- function(input_df){
  df <- copy(input_df)
  
  MAX_VAL <- 4294967295
  
  for(c in colnames(df)){
    col_class <- class(df[[c]])
    if(col_class %in% c('numeric', 'integer') && !(all(is.na(df[[c]])))){
      i_vec <- which(!(is.na(df[[c]])))
      if(any(df[[c]][i_vec] > MAX_VAL)){
        warning(paste(
          "Column", c, "has a value greater than the allowed maximum value.",
          "Truncating values over", MAX_VAL,"to the maximum value allowed."
        ))
        
        i_vec <- which(!(is.na(df[[c]])) & df[[c]] > MAX_VAL)
        df[[c]][i_vec] <- MAX_VAL - 1
      }
    }
  }
  
  return(df)
}
