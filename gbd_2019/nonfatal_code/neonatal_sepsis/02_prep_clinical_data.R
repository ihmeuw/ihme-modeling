#############################################################################################################
### Project: Neonatal Sepsis and Other Neonatal Infections
### Purpose: Process and prep clinical data, including age restrictions and changing measure to incidence
### Inputs: df: dataframe of bundle data pulled from bundle version that includes clinical data 
###         prevalence (T/F): whether to use the reported prevalence as incidence or run incidence conversion
#############################################################################################################

prep_clinical_data <- function(df, prevalence=FALSE) {
  
  measure <- ifelse(prevalence,"prevalence","incidence")
  bundle <- copy(df)
  bundle$cv_marketscan <- 0
  bundle[clinical_data_type=="claims, inpatient only"]$cv_marketscan <- 1
  bundle$cv_hospital <- 0
  bundle[clinical_data_type=="inpatient"]$cv_hospital <- 1
  bundle[clinical_data_type %in% c("inpatient","claims, inpatient only")]$cv_literature <- 0
  non_clinical <- bundle[clinical_data_type==""]
  
  clinical <- bundle[clinical_data_type=="inpatient"|clinical_data_type=="claims, inpatient only"]
  clinical <- clinical[age_end < 1]
  clinical[, crosswalk_parent_seq := seq]
  clinical <- clinical[!(cv_marketscan==1 & year_start==2000 & location_name!="Singapore")]
  
  if (prevalence==TRUE) {
    clinical[, measure := "incidence"]
    clinical[, unit_type := "Person*year"][, unit_value_as_published := 0]
  } else {
    clinical[, unadjusted_cases := cases]
    clinical[, unadjusted_sample_size := sample_size]
    clinical[, unadjusted_effective_sample_size := effective_sample_size]
    clinical[, unadjusted_mean := mean]
    clinical[, unadjusted_upper := upper]
    clinical[, unadjusted_lower := lower]
    clinical[, unadjusted_standard_error := standard_error]

    # # --------------------------
    # # OPTION 1: If there are cases and sample size, change sample size to person-years, instead of persons. Clear out mean, lower, upper, and stderr if existing
    #
    # clinical[!is.na(cases) & !is.na(sample_size), sample_size := sample_size * (28/365)]
    # clinical[!is.na(cases) & !is.na(sample_size), effective_sample_size := ""]
    # clinical[!is.na(cases) & !is.na(sample_size), mean := ""]
    # clinical[!is.na(cases) & !is.na(sample_size), upper := ""]
    # clinical[!is.na(cases) & !is.na(sample_size), lower := ""]
    # clinical[!is.na(cases) & !is.na(sample_size), standard_error := ""]
    # clinical[!is.na(cases) & !is.na(sample_size), uncertainty_type_value := ""]
    # clinical[!is.na(cases) & !is.na(sample_size), note_modeler := "cases and sample size - converted sample size to person-years using sample_size = sample_size * (28/365). Deleted mean,lower,upper,standard_error."]
    # # # --------------------------
    # # OPTION 2: If no cases/sample size, and only mean/lower/upper, use assumption of 14 days duration to convert to incidence
    #
    clinical[#(is.na(cases) & is.na(sample_size)) &
                (!is.na(mean) & !is.na(lower) & !is.na(upper)), mean := mean / (14/365) ]
    clinical[#(is.na(cases) & is.na(sample_size)) &
                (!is.na(mean) & !is.na(lower) & !is.na(upper)), lower := lower / (14/365) ]
    clinical[#(is.na(cases) & is.na(sample_size)) &
                (!is.na(mean) & !is.na(lower) & !is.na(upper)), upper := upper / (14/365) ]
    clinical[#(is.na(cases) & is.na(sample_size)) &
                (!is.na(mean) & !is.na(lower) & !is.na(upper)), effective_sample_size := ""]
    clinical[#(is.na(cases) & is.na(sample_size)) &
                (!is.na(mean) & !is.na(lower) & !is.na(upper)), standard_error := (upper - lower)/3.92]
    clinical[#(is.na(cases) & is.na(sample_size)) &
                (!is.na(mean) & !is.na(lower) & !is.na(upper)), note_modeler := "mean, lower, upper only - converted to incidence using incidence = prevalence / average duration (14 days). Deleted effective sample size & standard error"]
    clinical[#(is.na(cases) & is.na(sample_size)) &
                (!is.na(mean) & !is.na(lower) & !is.na(upper)), uncertainty_type_value := 95]
    clinical[, unit_type := "Person*year"][, unit_value_as_published := 0]
    clinical[, measure := "incidence"]
  }

  bundle <- rbind(non_clinical,clinical,fill=T)
  message(paste0("Clinical data prepped! Writing out to FILEPATH/bundle_92_",format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%Y-%m-%d"),".csv"))
  write.csv(bundle,paste0("FILEPATH/bundle_92_",measure,"_",format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%Y-%m-%d"),".csv"),row.names=F)
  return(bundle)
}