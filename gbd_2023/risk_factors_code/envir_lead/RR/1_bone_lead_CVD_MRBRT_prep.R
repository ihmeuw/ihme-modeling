# Bone lead-CVD MRBRT prep

# CONFIG #############################################################
rm(list = ls()[ls() != "xwalk"])

# Libraries ############################################################
library(reticulate)
reticulate::use_python("FILEPATH")
cw <- reticulate::import("crosswalk")

library(data.table)
library(openxlsx)
# library(epiR)
library(yaml)
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")

# Values ################################################################
cycle<-"GBD2022" #keeping this as GBD2022 because that is the folder name in the team folder (it wasn't changed to GBD2023)
release<-16 #gbd2023
rei<-243 #lead- can't be bone lead since that isn't the rr bundle
cause<-491 #CVD
bundle_v<-49014
risk<- "continuous"
rei_name<-"envir_lead_bone"
acause_name<-"cvd_ihd"
unit<-"bone lead exposure (ug/dL)" #risk_unit
NHANES_type<-c("deciles")

in.dir<-paste0("FILEPATH/",cycle,"/FILEPATH/")

# Load in extractions ######################################################

#load in bundle version
lead_rr<-get_bundle_version(bundle_v)

#only keep the rows that adjusted for SBP
lead_rr[confounders_hypertension==0,is_outlier:=1]


# Place the columns that start with cv_ into a variable called cov_cols
cv_cols <- names(lead_rr)[names(lead_rr) %like% "cov_"]

#Change all of the cols and cv_cols into numeric
cols <- c("nid","location_id","age_start","age_end","mean","lower","upper","nonCI_uncertainty_value","is_outlier","year_start","year_end")

lead_rr[, c(cols,cv_cols) := lapply(.SD, as.numeric), .SDcols = c(cols,cv_cols)]

# Ln Standard error and RR ################################################
#calculate standard error
lead_calc<-copy(lead_rr)[effect_size_unit!="log",ln_rr_se:=(log(upper) - log(lower))/(1.96*2)]

lead_calc[effect_size_unit=="log",ln_rr_se:=nonCI_uncertainty_value]

#calculate RR
lead_calc[effect_size_unit!="log",ln_rr:=log(mean)]

lead_calc[effect_size_unit=="log",ln_rr:=mean]

# Covariates ############################

# change categorical vars to binary
lead_calc[, cov_confounding_uncontrolled_1 := ifelse(cov_confounding_uncontrolled == 1, 1, 0)]
lead_calc[, cov_confounding_uncontrolled_2 := ifelse(cov_confounding_uncontrolled == 2, 1, 0)]
lead_calc[, cov_selection_bias_1 := ifelse(cov_selection_bias == 1, 1, 0)]
lead_calc[, cov_selection_bias_2 := ifelse(cov_selection_bias == 2, 1, 0)]

#no longer need these columns
lead_calc[, c("cov_confounding_uncontrolled","cov_selection_bias") := NULL]

# Manipulate columns ##############
lead_calc[,':='(#seq=1:.N,
                crosswalk_parent_seq=seq,
                rei_id = rei,
                cause_id = cause,
                risk_type = risk, 
                acause_og = acause,
                acause=acause_name,
                # rei=rei_name,
                risk_unit=unit,
                ref_risk_lower=cohort_unexp_level_rr_lower,
                ref_risk_upper=cohort_unexp_level_rr_upper,
                alt_risk_lower=cohort_exp_level_rr_lower,
                alt_risk_upper=cohort_exp_level_rr_upper)]

# lead_calc[,nid:=.GRP,by=paper_id]
lead_calc[,study_id:=nid]

#now only keep the columns that we need- no need for the cov_exp_measurement and cov_mixed_intervention as they were not applicable for this SR
keep<-c("seq", "crosswalk_parent_seq", "rei", "acause", "nid", "underlying_nid", "location_id", "location_name", "sex", "year_start", "year_end",
        "age_start", "age_end", "design", "risk_type", "risk_unit", "ln_rr", "ln_rr_se", "ref_risk_lower", "ref_risk_upper", "alt_risk_lower",
        "alt_risk_upper", "cov_outcome_def","cov_subpopulation","cov_exposure_population","cov_exposure_selfreport","cov_exposure_study","cov_outcome_selfreport",
        "cov_outcome_unblinded","cov_reverse_causation","cov_confounding_nonrandom","study_id","is_outlier")

lead_final<-lead_calc[,.SD,.SDcols = keep]

#save to excel
write.xlsx(lead_final,"FILEPATH/cw_rr_upload.xlsx",sheetName="extraction")
