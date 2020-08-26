# Get a bundle version into an R data table, either a new version or calling an old one
get_raw_data <- function(bundle_id, decomp_step, clin_inf = TRUE, bundle_version = 0){
  
 if (bundle_version==0) {
  version <- save_bundle_version(bundle_id=bundle_id, decomp_step=decomp_step, include_clinical=clin_inf)
  print(sprintf('Bundle version ID: %s', version$bundle_version_id))
  version_id <- version$bundle_version_id 
 } else {
   version_id <- bundle_version
 }
  
  df <- get_bundle_version(version_id)
  dt <- as.data.table(df)
  
  if (clin_inf==TRUE) {
  
    ## Label the clinical informatics with single cv for everything except marketscan, one for marketscan, one each for 2000 and not 2000
    dt[grep("MarketScan", field_citation_value), cv_marketscan_data:=1]
    dt[is.na(cv_marketscan_data), cv_marketscan_data:=0]
    dt[clinical_data_type!="" & cv_marketscan_data!=1, cv_admin:=1]
    dt[clinical_data_type==""| cv_marketscan_data==1, cv_admin:=0]
    dt <- dt[cv_marketscan_data==1 & year_start==2000, cv_marketscan_2000 := 1]
    dt <- dt[is.na(cv_marketscan_2000), cv_marketscan_2000:=0]
    dt <- dt[cv_marketscan_data==1 & year_start!=2000, cv_marketscan_other := 1]
    dt <- dt[is.na(cv_marketscan_other), cv_marketscan_other:=0]
  
  }
  
  return(dt)
  
}

market_scan_cv_labels <- function(dt) {
  
  ## Label the clinical informatics with single cv for everything except marketscan, one for marketscan, one each for 2000 and not 2000
  dt[grep("MarketScan", field_citation_value), cv_marketscan_data:=1]
  dt[is.na(cv_marketscan_data), cv_marketscan_data:=0]
  dt[clinical_data_type!="" & cv_marketscan_data!=1, cv_admin:=1]
  dt[clinical_data_type==""| cv_marketscan_data==1, cv_admin:=0]
  dt <- dt[cv_marketscan_data==1 & year_start==2000, cv_marketscan_2000 := 1]
  dt <- dt[is.na(cv_marketscan_2000), cv_marketscan_2000:=0]
  dt <- dt[cv_marketscan_data==1 & year_start!=2000, cv_marketscan_other := 1]
  dt <- dt[is.na(cv_marketscan_other), cv_marketscan_other:=0]
  
}

## FILL OUT MEAN/CASES/SAMPLE SIZE
get_cases_sample_size <- function(raw_dt){
  
  dt <- copy(raw_dt)
  
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean] #if cases and crude mean are present, this would give true samp size, so if se later derived from this, would not account for complex sampling (if present); if cases and some adjusted mean are presented, would give a faux sample size
  dt[measure == "prevalence" & is.na(sample_size) & is.na(cases), sample_size := mean*(1-mean)/standard_error^2]  #this might be a faux samp size if standard error reported (or upper/lower reported and converted to se by Uploader) had adjustment for complex sampling
  dt[measure == "incidence" & is.na(sample_size) & is.na(cases), sample_size := mean/standard_error^2]   #this might be a faux samp size if standard error reported (or upper/lower reported and converted to se by Uploader) had adjustment for complex sampling
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  
  return(dt)
  
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}
