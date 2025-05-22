# validate columns --------------------------------------------------------

validate_columns <- function(cols){
  req_cols <- c(
    "nid", "underlying_nid", "location_id", "sex", "sex_id", "measure",
    "year_start", "year_end", "age_start", "age_end", 
    "mean", "standard_error", "upper", "lower"
  )
  
  if(!(all(req_cols %in% cols))){
    i_vec <- which(!(req_cols %in% cols))
    col_string <- paste(req_cols[i_vec], collapse = ", ")
    stop("The follow required columns are not in your df: ", col_string)
  }
}