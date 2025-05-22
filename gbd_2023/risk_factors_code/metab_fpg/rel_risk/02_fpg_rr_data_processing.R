################################################################################
# FPG RISK CURVE
# Description: processing bundle to save as crosswalk version before running BOP
#             1.blood glucose unit conversion and glucose test type conversion
#             2.calculate log transformed SE
#             3.filling in missing lower/upper cut offs for reference and alternative


# Set up------------------------------------------------------------------------
    invisible(sapply(list.files('FILEPATH', full.names = T), source))
    date <- gsub("-", "_", Sys.Date())

    library(readxl)
    library(data.table)
    library(openxlsx)
    library(ggplot2)
    library(dplyr)
    library(msm)
    
    cause_ids <- get_ids("cause")
    loc <- get_location_metadata(location_set_id = 35, release_id = 10)
    loc2 <- subset(loc, select = c(location_id, region_name))
    
    #bundle version you want to process
    bv<-41532 
    
# Loading in bundle_version-----------------------------------------------------
   df<-get_bundle_version(bundle_version_id = bv, fetch = 'all')

  # filtering for data to include in GBD 2021 curves
      df2 <- df[gbd2020_use == 1, ]
  
  # subsetting to outcomes
      cvd_ihd <- df2[outcome == "Ischemic heart disease", ]
      cvd_stroke_isch <- df2[outcome == "Ischemic stroke"]
      cvd_stroke_cerhem <- df2[outcome %in% c("Intracerebral hemorrhage", "Hemmorhagic stroke"), ]
      cvd_pvd <- df2[outcome == "Peripheral vascular disease",]
  
  # reading in csv with fpg imputation values
      fpg <- read.csv('FILEPATH')
      fpg$cut <- round(fpg$cut, digits = 1)


# Processing for MR-BRT risk curve -------------------------------------------------------------------------
  acauselist<- list("cvd_ihd"=cvd_ihd , "cvd_stroke_isch"=cvd_stroke_isch, "cvd_stroke_cerhem"=cvd_stroke_cerhem, "cvd_pvd"=cvd_pvd) # input acause you want to process
  upload<-data.frame()
      
for (i in 1:length(acauselist)) {
  temp<-acauselist[[i]]
  temp<- as.data.table(temp)
  
    ## 1. unit/glucose test conversion
        cols <- c("a_0", "a_1", "b_0", "b_1") 
        temp[, (cols) := lapply(.SD, as.numeric), .SDcols = cols] 
        
        temp <- temp[cohort_exp_unit_rr == "mg/dL", b_0 :=b_0/18]
        temp <- temp[cohort_exp_unit_rr == "mg/dL", b_1 := b_1/18]
        temp <- temp[cohort_unexp_unit_rr == "mg/dL", a_0 := a_0/18]
        temp <- temp[cohort_unexp_unit_rr == "mg/dL", a_1 := a_1/18]
        
        temp <- temp[glucose_test %in% c("HbA1c", "HbA1c, tx"), a_0 := a_0*(7/6.5)]
        temp <- temp[glucose_test %in% c("HbA1c", "HbA1c, tx"), a_1 := a_1*(7/6.5)]
        temp <- temp[glucose_test %in% c("HbA1c", "HbA1c, tx"), b_0 := b_0*(7/6.5)]
        temp <- temp[glucose_test %in% c("HbA1c", "HbA1c, tx"), b_1 := b_1*(7/6.5)]
        
        temp <- temp[glucose_test %in% c("OGTT", "OGTT, tx"), a_0 := a_0*(7/11.1)]
        temp <- temp[glucose_test %in% c("OGTT", "OGTT, tx"), a_1 := a_1*(7/11.1)]
        temp <- temp[glucose_test %in% c("OGTT", "OGTT, tx"), b_0 := b_0*(7/11.1)]
        temp <- temp[glucose_test %in% c("OGTT", "OGTT, tx"), b_1 := b_1*(7/11.1)]
    
    ## 2. calculating log transformed SE
        temp <- temp[,lln_standard_error := (log(upper)-log(lower))/3.92]
    
    ## 3. filling in missing lower/upper bound FPG exposure values by region
          
          temp <- merge(temp, loc2, by = "location_id", all.x=T) #get region id
          temp <- temp[region_name == "", region_name := "Global"]
          
          temp <- temp[is.na(b_0), cut := b_1] #if b_0 is missing, cut =b_1
          temp <- temp[, cut := round(cut, digits = 1)] #then round cut
          table(temp$cut)
          table(fpg$cut)
          
          temp <- temp[cut == 4, cut := 3.9]
          temp <- temp[cut == 4.6|cut==4.9, cut := 4.7]
          temp <- temp[cut == 5.4, cut := 5.6]
          temp <- temp[cut == 6.1|cut==6.5, cut := 6.3]
          temp <- temp[cut == 6.7, cut := 7]
          temp <- temp[cut == 7.7 | cut == 7.5, cut := 7.8]
          
          temp <- merge(temp, fpg, by = c("region_name", "cut"), all.x=T) #merge with cut values
          temp <- temp[is.na(b_0), b_0 := lower_impute]
          temp <- subset(temp, select = -c(lower_impute, upper_impute))
          
          temp <- temp[is.na(b_1), cut := b_0]
          temp <- temp[, cut := round(cut, digits = 1)]
          table(temp$cut)
          table(fpg$cut)
          
          temp <- temp[cut == 4.4, cut := 4.7]
          temp <- temp[cut == 4.9, cut := 4.7]
          temp <- temp[cut == 5.9|cut==5.8, cut := 5.6]
          temp <- temp[cut == 6.1|cut==6.5, cut := 6.3]
          temp <- temp[cut == 6.7, cut := 7]
          temp <- temp[cut == 7.5 | cut == 7.9 | cut == 8.1, cut := 7.8]
          temp <- temp[cut == 8.9|cut == 9.8|cut == 8.3 | cut == 8.8 | cut == 9.7|cut==10.8, cut := 7.8]
          temp <- temp[cut ==7.8 &region_name == "Australasia", region_name := "Global"]
          
          temp <- merge(temp, fpg, by = c("region_name", "cut"), all.x=T) #left join
          temp <- temp[is.na(b_1), b_1 := upper_impute]
          temp <- subset(temp, select = -c(lower_impute, upper_impute))
          
          temp <- temp[is.na(a_0), cut := a_1]
          temp <- temp[, cut := round(cut, digits = 1)]
          
          table(temp$cut)
          table(fpg$cut)
          
          temp <- temp[cut == 3.5|cut == 3.8, cut := 3.9]
          temp <- temp[cut == 5.1|cut == 4.9|cut == 5 | cut == 4.8|cut==4.4, cut := 4.7]
          temp <- temp[cut == 5.5|cut==5.4 | cut == 5.3 | cut == 5.9|cut==5.7, cut := 5.6]
          temp <- temp[cut == 6.1|cut==6|cut==6.5, cut := 6.3]
          temp <- temp[cut == 6.7|cut ==7.2, cut := 7]
          temp <- temp[cut == 7.7 | cut ==7.5, cut := 7.8]
          
          temp <- merge(temp, fpg, by = c("region_name", "cut"), all.x=T)
          temp <- temp[is.na(a_0), a_0 := lower_impute]
          temp <- subset(temp, select = -c(lower_impute, upper_impute))
          
          temp <- temp[is.na(a_1), cut := a_0]
          temp <- temp[, cut := round(cut, digits = 1)]
          
          table(temp$cut)
          table(fpg$cut)
          
          temp <- temp[cut == 3.5|cut == 3.8, cut := 3.9]
          temp <- temp[cut == 5.1|cut == 4.9|cut == 5 | cut == 4.8|cut==4.4, cut := 4.7]
          temp <- temp[cut == 5.5|cut==5.4 | cut == 5.3 | cut == 5.9|cut==5.7, cut := 5.6]
          temp <- temp[cut == 6.1|cut==6|cut==6.5, cut := 6.3]
          temp <- temp[cut == 6.7|cut ==7.2, cut := 7]
          temp <- temp[cut == 7.7 | cut ==7.5, cut := 7.8]
          
          temp <- merge(temp, fpg, by = c("region_name", "cut"), all.x=T)
          temp <- temp[is.na(a_1), a_1 := upper_impute]
          temp <- subset(temp, select = -c(lower_impute, upper_impute))
          
          #not sure what this does
          temp <- temp[!(a_0>a_1), ]
          temp <- temp[!(b_0>b_1), ]


##########################
# Formatting data for MR-BRT pipeline
names(temp) <- sub("^bc_", "cov_", names(temp)) 
temp[, ':=' (crosswalk_parent_seq = as.numeric(seq),
             seq= "",
             # study_id = nid,
             bundle_id = 9158,
             bundle_version_id = bv,
             risk_type = "continuous",
             risk_unit = "mmol/L",
             ln_rr = log(mean), #mean?
             ln_rr_se = lln_standard_error, #
             ref_risk_lower = a_0,
             ref_risk_upper = a_1,
             alt_risk_lower = b_0,
             alt_risk_upper = b_1,
             bc_confounder_quality = cov_confounder_quality,
             cov_confounder_quality = ifelse(cov_confounder_quality>=1, 1, 0 ))]

if(i=="cvd_stroke_cerhem"){
temp2 <- subset(temp, select = c(seq, crosswalk_parent_seq, rei, acause, nid, underlying_nid, bundle_id, bundle_version_id,
                                 location_id, location_name, sex, year_start, year_end, age_start, age_end, 
                                 design, is_outlier, risk_type, risk_unit, ln_rr, ln_rr_se, ref_risk_lower, ref_risk_upper,
                                 alt_risk_lower, alt_risk_upper, 
                                 cov_confounder_quality, cov_imputed_fpg,
                                 cov_hba1c, cov_ogtt, cov_exclude_known_dm, cov_tx, 
                                 cov_total_hemorrhage, #this cov should only be included for cerhem
                                 #cov_pad_measurement_quality, #this cov should only be included for pad
                                 cov_medical_records_dm, bc_confounder_quality
                                ))
}else if(i=="cvd_pvd") {
  temp2 <- subset(temp, select = c(seq, crosswalk_parent_seq, rei, acause, nid, underlying_nid, bundle_id, bundle_version_id,
                                   location_id, location_name, sex, year_start, year_end, age_start, age_end, 
                                   design, is_outlier, risk_type, risk_unit, ln_rr, ln_rr_se, ref_risk_lower, ref_risk_upper,
                                   alt_risk_lower, alt_risk_upper, 
                                   cov_confounder_quality, cov_imputed_fpg,
                                   cov_hba1c, cov_ogtt, cov_exclude_known_dm, cov_tx, 
                                   #cov_total_hemorrhage, #this cov should only be included for cerhem
                                   cov_pad_measurement_quality, #this cov should only be included for pad
                                   cov_medical_records_dm, bc_confounder_quality
  ))
}else{
  temp2 <- subset(temp, select = c(seq, crosswalk_parent_seq, rei, acause, nid, underlying_nid, bundle_id, bundle_version_id,
                                   location_id, location_name, sex, year_start, year_end, age_start, age_end, 
                                   design, is_outlier, risk_type, risk_unit, ln_rr, ln_rr_se, ref_risk_lower, ref_risk_upper,
                                   alt_risk_lower, alt_risk_upper, 
                                   cov_confounder_quality, cov_imputed_fpg,
                                   cov_hba1c, cov_ogtt, cov_exclude_known_dm, cov_tx, 
                                   #cov_total_hemorrhage, #this cov should only be included for cerhem
                                   #cov_pad_measurement_quality, #this cov should only be included for pad
                                   cov_medical_records_dm, bc_confounder_quality
  ))
}



upload<- rbind(upload,temp2)
}

w