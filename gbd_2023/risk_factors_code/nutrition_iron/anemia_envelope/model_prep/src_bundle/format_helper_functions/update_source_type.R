
# update source type ------------------------------------------------------

update_source_type <- function(source_col){
  i_vec <- which(is.na(source_col) | source_col == "")
  source_col[i_vec] <- "Survey - other/unknown"
  return(source_col)
}