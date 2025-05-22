
# source libraries --------------------------------------------------------

library(data.table)

# helper functions --------------------------------------------------------

assign_anemia_measure <- function(input_df){
  df <- copy(input_df)
  
  adjust_map <- fread(file.path(getwd(), "vmnis/src/adjust_map.csv"))
  
  df <- merge.data.table(
    x = df,
    y = adjust_map,
    by = c("me_type", "adj_type"),
    all.x = TRUE
  )
  
  return(df)
}
