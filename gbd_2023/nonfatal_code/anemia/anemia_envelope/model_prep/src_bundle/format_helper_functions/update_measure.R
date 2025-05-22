
# update measure value ----------------------------------------------------

update_measure <- function(hb_variable_col){
  vec <- rep(NA_character_, length(hb_variable_col))
  
  prop_index <- which(grepl("anemia", hb_variable_col))
  vec[prop_index] <- 'proportion'
  
  cont_index <- which(grepl("hemog", hb_variable_col))
  vec[cont_index] <- 'continuous'
  
  if(any(is.na(vec))){
    stop("Not all measures defined. Please check and rerun.")
  }
  
  return(vec)
}