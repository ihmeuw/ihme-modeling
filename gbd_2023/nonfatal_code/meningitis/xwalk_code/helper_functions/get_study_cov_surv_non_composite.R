## ASSIGN STUDY COVARIATES FOR INPATIENT AND CLAIMS
get_study_cov_surv_non_composite <- function(raw_dt) {
  #' get study covariate
  #' 
  #' @description fills in study covariate indicator (1 or 0) for 
  #'              cv_marketscan_inp_2000,
  #'              cv_marketscan_data,
  #'              cv_inpatient,
  #'              cv_surveillance_confirmed_bacterial, and
  #'              cv_surveillance_suspected
  #'              
  #' 
  #' @param raw_dt data.table. data table to fill in study covariate columns
  #' @return dt data.table. data table with study covariate columns marked
  #' @details cv_inpatient is used as the gold standard and all
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
  dt[field_citation_value %like% "Claims" & year_start == 2000, cv_marketscan_inp_2000:= 1]
  dt[field_citation_value %like% "Claims" & year_start != 2000, cv_marketscan_data:= 1]
  
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
  
  # read in additional meningitis surveillance info
  surv <- as.data.table(read.xlsx("FILEPATH", sheet = 1))
  classes <- c("all_bacterial", "partial_bacterial", "bacterial_viral", "incl_suspected", "undefined")
  surv <- surv[, c("nid", classes), with = F]
  surv <- surv[2:nrow(surv),] # drop first row
  surv <- surv[,(classes) := lapply(.SD, as.numeric), .SDcols = classes]
  surv[,nid := as.character(nid)]
  
  # merge surveillance in with the surveillance data  
  dt <- merge(dt, surv, all.x = T, by = "nid")
  
  # handle surveillance that is suspected
  dt[cv_surveillance == 1 & cv_suspected == 1 & cv_probable == 0 & cv_confirmed == 0, `:=` (incl_suspected = 1)]
  
  # handle NIDs where both suspected and confirmed cases were available
  dt[cv_surveillance == 1 & cv_suspected == 1 & cv_probable == 0 & cv_confirmed == 0, `:=` (all_bacterial = 0, partial_bacterial = 0)]

  dt[cv_surveillance == 1 & field_citation_value %like% "Brazil Information System", bacterial_viral := 1]
  dt[cv_surveillance == 1, unique_surv_def := all_bacterial + partial_bacterial + bacterial_viral + incl_suspected + undefined]
  if (any(na.omit(unique(dt$unique_surv_def))!=1)) print ("all survs must have 1 def marked")
  
  # check that all data have only one of the used study covariates marked
  # check only incidence measure since we do not xwalk mtexcess or cfr
  dt_check <- dt[measure == "incidence"]

  return(dt)
}