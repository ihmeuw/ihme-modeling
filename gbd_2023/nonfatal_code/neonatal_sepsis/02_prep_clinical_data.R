#############################################################################################################
### Project: Neonatal Sepsis and Other Neonatal Infections
### Purpose: Process and prep clinical data, including age restrictions and changing measure to incidence
#############################################################################################################

prep_clinical_data <- function(df, aggregate_under1=FALSE) {
  
  bundle <- copy(df)
  bundle$cv_marketscan <- 0
  bundle[clinical_data_type=="claims, inpatient only"]$cv_marketscan <- 1
  bundle$cv_hospital <- 0
  bundle[clinical_data_type=="inpatient"]$cv_hospital <- 1
  bundle[clinical_data_type %in% c("inpatient","claims, inpatient only")]$cv_literature <- 0
  non_clinical <- bundle[clinical_data_type==""]
  
  if (aggregate_under1==FALSE){
  
    clinical <- bundle[clinical_data_type=="inpatient"|clinical_data_type=="claims, inpatient only"]
    clinical <- clinical[!(age_start==0 & age_end==0)] # remove birth prevalence
    clinical <- clinical[!(age_start > 0.02)] # remove non-neonatal
    clinical[cv_marketscan==1 & age_end==0.999, age_end:=0.07671233] # change marketscan age end to only neonatal for age-splitting purposes
    clinical[, crosswalk_parent_seq := seq]
    clinical <- clinical[!(cv_marketscan==1 & year_start==2000 & location_name!="Singapore")]
  
    # Adjust clinical data and convert to incidence
    clinical[,`:=` (unadjusted_cases = cases,
                    unadjusted_sample_size = sample_size,
                    unadjusted_effective_sample_size = effective_sample_size,
                    unadjusted_mean = mean,
                    unadjusted_lower = lower,
                    unadjusted_upper = upper,
                    unadjusted_standard_error = standard_error)]

    # --------------------------
    # First convert rows where we only have mean/lower/upper to implied cases/sample_size
    clinical[(is.na(cases) & is.na(sample_size)) &
              (!is.na(mean) & !is.na(lower) & !is.na(upper)), sample_size := (mean*(1-mean))/(((upper - lower)/3.92)^2)] 
    clinical[(is.na(cases) & is.na(sample_size)) &
              (!is.na(mean) & !is.na(lower) & !is.na(upper)), cases := mean*sample_size] 
  
    # Change sample size to person-years, instead of persons. Clear out mean, lower, upper, and stderr if existing
    # age-specific hospital data  
    clinical[!is.na(cases) & !is.na(sample_size) & cv_hospital==1 & age_start==0, sample_size := sample_size * (7/365)]
    clinical[!is.na(cases) & !is.na(sample_size) & cv_hospital==1 & age_start==0.01917808, sample_size := sample_size * (21/365)]
    # marketscan data (treat as all neonatal sample size)
    clinical[!is.na(cases) & !is.na(sample_size) & cv_marketscan==1, sample_size := sample_size * (28/365)]
   
  }
  
  if (aggregate_under1){
    clinical <- bundle[clinical_data_type=="inpatient"|clinical_data_type=="claims, inpatient only"]
    clinical <- clinical[!(age_start==0 & age_end==0)] # remove birth prevalence
    clinical <- clinical[age_start < 1] # only want under 1
    
    # Calculate cases and sample size when only have mean/lower/upper
    clinical[(is.na(cases) & is.na(sample_size)) &
               (!is.na(mean) & !is.na(lower) & !is.na(upper)), sample_size := (mean*(1-mean))/(((upper - lower)/3.92)^2)] 
    clinical[(is.na(cases)) &
               (!is.na(mean) & !is.na(lower) & !is.na(upper)), cases := mean*sample_size] 
    
    # Separate hospital (needs aggregation) and claims (doesn't need aggregation)
    hospital <- clinical[cv_hospital==1]
    claims <- clinical[cv_marketscan==1]
    
    # Process hospital data
    hosp_reshape <- hospital[,.(nid,location_id,sex,year_start,year_end,age_start,cases,sample_size)]
    hosp_reshape[age_start %in% c(0.5,0.07671233), sample_size:=0] # only want to aggregate neonatal sample_size
    
    # Generate total sample size for each source/year/sex
    hosp_reshape[, sample_size := sum(sample_size), by=c("nid","location_id","sex","year_start","year_end")]
    
    # Cast wide to sum cases 
    hosp_reshape <- dcast(hosp_reshape, nid + location_id + sex + year_start + year_end + 
                            sample_size ~ age_start, value.var="cases")
    hosp_reshape[,cases:=`0` + `0.01917808` + `0.07671233` + `0.5`]
    hosp_reshape[,`:=` (`0`=NULL, `0.01917808`=NULL, `0.07671233`=NULL,`0.5`=NULL,
                        age_start=0, age_end=0.999)]
    
    # Merge on metadata
    hospital <- hospital[,-c("age_start","age_end","cases","sample_size","mean","lower","upper","standard_error")]
    hospital <- hospital[!duplicated(hospital[,c("nid","location_id","sex","year_start","year_end")])]
    hospital <- merge(hosp_reshape,hospital,by=c("nid","location_id","sex","year_start","year_end"),all=TRUE)
    
    # Add hospital and claims back together
    clinical <- rbind(hospital,claims,fill=TRUE)
    
    # Save original values
    clinical[,`:=` (unadjusted_cases = cases,
                    unadjusted_sample_size = sample_size,
                    unadjusted_effective_sample_size = effective_sample_size,
                    unadjusted_mean = mean,
                    unadjusted_lower = lower,
                    unadjusted_upper = upper,
                    unadjusted_standard_error = standard_error)]
    
    clinical[, age_end:=0.07671233] # change age end to only neonatal for age-splitting purposes
    clinical[, crosswalk_parent_seq := seq]
    clinical <- clinical[!(cv_marketscan==1 & year_start==2000 & location_name!="Singapore")]
    
    
    # Change sample size to person-years, instead of persons. 
    clinical[, sample_size := sample_size * (28/365)]
  }
  
   # universal processing
   clinical[!is.na(cases) & !is.na(sample_size), effective_sample_size := ""]
   clinical[!is.na(cases) & !is.na(sample_size), mean := ""]
   clinical[!is.na(cases) & !is.na(sample_size), upper := ""]
   clinical[!is.na(cases) & !is.na(sample_size), lower := ""]
   clinical[!is.na(cases) & !is.na(sample_size), standard_error := ""]
   clinical[!is.na(cases) & !is.na(sample_size), uncertainty_type_value := ""]
   clinical[!is.na(cases) & !is.na(sample_size), note_modeler := "cases and sample size - converted sample size to person-years using sample_size = sample_size * (28/365). Deleted mean,lower,upper,standard_error."]
   clinical[, unit_type := "Person*year"][, unit_value_as_published := 0]
   clinical[, measure := "incidence"]

  # Add non-clinical data and write-out
  bundle <- rbind(non_clinical,clinical,fill=T)
  message(paste0("Clinical data prepped! Writing out to FILEPATH.csv"))
  write.csv(bundle,paste0("FILEPATH.csv"),row.names=F)
  return(bundle)
}
