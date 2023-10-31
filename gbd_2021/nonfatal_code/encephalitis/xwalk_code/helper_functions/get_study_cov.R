## ASSIGN STUDY COVARIATES FOR INPATIENT AND CLAIMS
get_study_cov <- function(raw_dt) {
  #' get study covariate
  #' 
  #' @description fills in study covariate indicator (1 or 0) for 
  #'              cv_marketscan_inp_2000,
  #'              cv_marketscan_data,
  #'              cv_inpatient,
  #'              cv_population_surveillance, and
  #'              cv_sentinel_surveillance
  #'              
  #' 
  #' @param raw_dt data.table. data table to fill in study covariate columns
  #' @return dt data.table. data table with study covariate columns marked
  #' @details cv_inpatient is used as the gold standard for GBD 2019 and all
  #'          other study types are crosswalked up to the gold standard
  #' @note We want to treat marketscan 2000 data separately due to data 
  #'       quality concerns. Poland claims GBD 2019 are treated same as 
  #'       marketscan claims
  #' 
  #' @examples get_study_cov(sex_split_final_dt)
  #' 
  dt <- copy(raw_dt)
  
  # set study covariate column classes to be numeric
  cols <- which(colnames(dt) %like% "cv")
  cols <- colnames(dt)[cols]
  dt <- dt[,(cols) := lapply(.SD, as.numeric), .SDcols = cols]

  # set MS 2000 Covariate
  dt[field_citation_value %like% "Claims", cv_marketscan_data:= 1]
  dt[field_citation_value %like% "Truven" & year_start == 2000, `:=`
     (cv_marketscan_inp_2000 = 1, cv_marketscan_data = 0)]
  
  # exception for Phillipine Health Insurance
  dt[field_citation_value %like% "Philippine Health Insurance", cv_marketscan_data := 0]
  
  dt[clinical_data_type == "inpatient", cv_inpatient:= 1]
  
    # fix NAs
  dt[is.na(cv_marketscan_data), cv_marketscan_data := 0]
  dt[is.na(cv_marketscan_inp_2000), cv_marketscan_inp_2000 := 0]
  dt[is.na(cv_surveillance), cv_surveillance := 0]
  dt[is.na(cv_inpatient), cv_inpatient := 0]
  
  # recode cv_hospital as cv_inpatient
  dt[cv_marketscan_data == 0 & cv_marketscan_inp_2000 == 0 
     & cv_surveillance == 0 & cv_hospital == 1, cv_inpatient := 1]
  # check that all data have only one of the used study covariates marked
  # check only incidence measure since we do not xwalk mtexcess or cfr
  dt_check <- dt[measure == "incidence"]
  # if (nrow(dt_check[cv_inpatient + cv_marketscan_inp_2000 + cv_marketscan_data + 
  #             cv_surveillance != 1]) > 0) {
  #   stop("Not all rows are uniquely defined as reference or alternative case definition")
  # }
  return(dt)
}