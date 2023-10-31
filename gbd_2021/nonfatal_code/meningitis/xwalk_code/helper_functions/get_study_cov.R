## ASSIGN STUDY COVARIATES FOR INPATIENT AND CLAIMS
get_study_cov <- function(raw_dt) {
  #' get study covariate
  #' 
  #' @description fills in study covariate indicator (1 or 0) for 
  #' alternative and reference definition
  #'              
  #' 
  #' @param raw_dt data.table. data table to fill in study covariate columns
  #' @return dt data.table. data table with study covariate columns marked
  #' @details 
  #' @note 
  #' 
  #' @examples get_study_cov(sex_split_final_dt)
  #' 
  dt <- copy(raw_dt)
  
  # set study covariate column classes to be numeric
  cols <- which(colnames(dt) %like% "cv")
  cols <- colnames(dt)[cols]
  dt <- dt[,(cols) := lapply(.SD, as.numeric), .SDcols = cols]

  # look for any NAs in columns of interest
  if(any(is.na(unique(dt$cv_obj_all_meningitis)))){
    stop("there are NAs in the columns needed to determine reference vs. alternative. please go back and reextract")
  }
  
  # convert cv_obj_all_meningitis to proper formatting
  # set reference covariate
  dt[cv_obj_all_meningitis == 1, full_denominator := 1]
  dt[cv_obj_all_meningitis == 0, full_denominator := 0]
  
  # set alternative covariate
  dt[cv_obj_all_meningitis == 1, partial_denominator := 0]
  dt[cv_obj_all_meningitis == 0, partial_denominator := 1]
  
  # check that all data have only one of the used study covariates marked
  if (nrow(dt[full_denominator + partial_denominator!= 1]) > 0) {
    stop("Not all rows are uniquely defined as reference or alternative case definition")
  }
  return(dt)
}
