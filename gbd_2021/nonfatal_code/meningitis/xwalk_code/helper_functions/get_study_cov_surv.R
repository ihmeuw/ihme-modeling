## ASSIGN STUDY COVARIATES FOR INPATIENT AND CLAIMS
get_study_cov_surv <- function(raw_dt) {
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

  # set MS & MS 2000 Covariate
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
  
  # do not allow any suspected or probable cases to be marked as inpatient reference
  dt[(cv_suspected == 1 | cv_probable == 1), cv_inpatient := 0]
  
  # read in additional meningitis surveillance info
  surv <- as.data.table(read.xlsx("/filepath/meningitis_inc_surv_excl_brazil_tracker_Jul2020.xlsx", sheet = 1))
  classes <- c("all_bacterial", "partial_bacterial", "bacterial_viral", "incl_suspected", "undefined")
  surv <- surv[, c("nid", classes), with = F]
  surv <- surv[2:nrow(surv),] # drop first row
  surv <- surv[,(classes) := lapply(.SD, as.numeric), .SDcols = classes]
  # surv[,nid := as.character(nid)]
  
  # merge partial_bacterial into all_bacterial
  surv[partial_bacterial == 1, all_bacterial := 1]
  
  # merge surveillance in with the surveillance data  
  dt <- merge(dt, surv, all.x = T, by = "nid")
  
  # merge incl_suspected and undefined both into cv_suspected
  dt[cv_surveillance == 1 & (incl_suspected == 1 | undefined == 1), `:=` (cv_suspected = 1)]
  
  # deal with NIDs where both suspected and confirmed cases were available
  dt[cv_surveillance == 1 & cv_suspected == 1 & cv_probable == 0 & cv_confirmed == 0, `:=` (all_bacterial = 0, partial_bacterial = 0)]

  # make sure that suspected or probable cases are not marked as the reference
  dt[cv_suspected == 1 | cv_probable == 1, cv_inpatient := 0 ]
  
  # deal with Brazil
  dt[cv_surveillance == 1 & field_citation_value %like% "Brazil Information System", bacterial_viral := 1]
  dt[cv_surveillance == 1, unique_surv_def := all_bacterial + bacterial_viral + cv_suspected]
  if (any(na.omit(unique(dt$unique_surv_def))>1)) print ("all survs must have 1 or fewer def marked")
  
  # set compound definitions
  dt$cv_surveillanceANDcv_suspected <- 0
  dt$cv_surveillanceANDbacterial_viral <- 0
  dt$cv_surveillanceANDbroadly_defined <- 0
  dt$broadly_defined <- 0
  dt[(cv_surveillance == 1 & (bacterial_viral == 1)), `:=`
     (cv_surveillanceANDbroadly_defined = 1, broadly_defined = 1)]

  # check that all data have only one of the used study covariates marked
  # check only incidence measure since we do not xwalk mtexcess or cfr
  # ALSO do not check suspected or probable rows since we aren't using those
  dt_check <- dt[measure == "incidence" & (cv_confirmed == 1 | is.na(cv_confirmed))]
  if (nrow(dt_check[cv_inpatient + cv_marketscan_inp_2000 + cv_marketscan_data +
              cv_surveillance != 1]) > 0) {
    stop("Not all rows are uniquely defined as reference or alternative case definition")
  }
  return(dt)
}