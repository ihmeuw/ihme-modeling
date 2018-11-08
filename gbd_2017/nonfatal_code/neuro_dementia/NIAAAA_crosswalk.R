#################################################################################################################
## NIA-AA Crosswalk
## AUTHOR: 
## DATE: March 2018
## NEED TO HAVE COVARIATE COLUMNS FOR REFERENCE AND EACH ALTERNATE DEFINITION, coded 0 & 1
## ARGUMENTS:
##    (1) dt = data.table with pre-crosswalk data
##    (2) matched_dt = data.table with matched data (sources reporting multiple case definitions)
##    (3) ref = string with short name of reference case definition
##    (4) alts = vector of strings, short names of alternative case definitions, matching covariates
##    (5) measure_name = measure you are crosswalking, example: "prevalence" or "incidence"
##    (6) out_dir = filepath for generated plots to be saved as pdfs -- if==0 no plots will be made
## EXAMPLE CALL: crosswalk_from_matched(dt=copd, matched_dt=copd_matched, ref="gold_post", alts=c("gold_pre","lln_post","lln_pre"),
##        measure="prevalence",out_dir=FILEPATH) where dt and matched_dt match the
##        epi template and have covariates cv_gold_post, cv_gold_pre, cv_ln_post, cv_lln_pre
##################################################################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/" 
  h_root <- "/homes/USERNAME/"
} else { 
  j_root <- "J:/"
  h_root <- "H:/"
}
set.seed(98736)

pacman::p_load(data.table, boot)
library(meta, lib.loc = paste0(j_root, FILEPATH))
library(openxlsx, lib.loc = paste0(j_root, FILEPATH))

## SET OBJECTS
functions_dir <- paste0(j_root, FILEPATH)
upload_dir <- paste0(j_root, FILEPATH)
doc_dir <- paste0(j_root, FILEPATH)
bundle <- 146
niaaa_nids <- c(276372,276354, 209272, 315414)
date <- gsub("-", "_", Sys.Date())

## SOURCE CENTRAL FUNCTION
source(paste0(functions_dir, "get_epi_data.R"))

## CROSSWALK FUNCTION
crosswalk_from_cv <- function(dt, ref, alts, measure_name, out_dir){
  
  library(plyr)
  library(data.table)
  library(ggplot2)
  
  ####################################
  ## LOAD & PREP PRE-CROSSWALK BUNDLE
  ####################################
  
  dt <- as.data.table(dt)
  n <- nrow(dt)
  dt$id <- seq(1,n,1) ## set unique identifier 
  pre_data <- copy(dt)
  
  ## just crosswalk data with the measure you've specified
  pre_data <-pre_data[measure==measure_name,]
  
  ## add case_type column
  ## epi template already has case_name, case_definition, and case_diagnostics fields
  types <- c(ref, alts)
  for(t in types){
    cv <- paste0("cv_",t)
    pre_data[get(cv)==1, case_type:=t]
  }
  
  ## Add original unadjusted mean to note_modeler column for reference
  pre_data[case_type %in% alts & !is.na(note_modeler), note_modeler := paste0(note_modeler, " | unadjusted mean : ", mean)]
  pre_data[case_type %in% alts & is.na(note_modeler), note_modeler := paste0("unadjusted mean : ", mean)]
  
  ## Generate age_mid variable
  pre_data[, age_mid := (age_start+age_end)/2]
  
  ## save pre_data as post_data to preserve pre_data as unadjusted
  post_data <- copy(pre_data)
  
  
  ####################################
  ## LOOP OVER ALT DEFINITION
  ####################################
  
  for(alt in alts){
    
    # subset to data from either ref or alt case_type, and countries with
    #   data from both ref AND alt case_type
    xwalk_subset <- post_data[(case_type==ref | case_type==alt),]
    ref_data <- xwalk_subset[case_type==ref,]
    alt_data <- xwalk_subset[case_type==alt,]
    locs <- intersect(unique(ref_data$location_id),unique(alt_data$location_id))
    xwalk_subset <- xwalk_subset[location_id %in% locs,]
    
    xwalk_subset <- xwalk_subset[mean>0,]
    xwalk_subset[, log_mean:=log(mean)]
    
    print(paste0("Crosswalking ", alt))
    
    if(nrow(xwalk_subset)<10) print("WARNING: you have fewer than 10 rows to inform this crosswalk")
    
    # generate interaction variable
    xwalk_subset[,age_mid_alt:=age_mid*get(paste0("cv_",alt))]
    
    # Perform regression and save as a function
    regression <- summary(lm(log_mean~get(paste0("cv_",alt))+age_mid+age_mid_alt, data=xwalk_subset))
    print(regression)
    B1 <- regression$coefficients[2,1] # coefficient for alt
    B3 <- regression$coefficients[4,1] # coefficient for age_mid_alt interaction
    crosswalk <- function(mean, age_mid){
      return(mean/exp(B1+B3*age_mid))
    }
    
    # Apply regression to data & propagte uncertainty
    post_data <- post_data[case_type == alt]
    post_data[case_type==alt, mean_new:=crosswalk(mean, age_mid)]
    B1_SE <- regression$coefficients[2,2]
    B3_SE <- regression$coefficients[4,2]
    post_data[case_type==alt, standard_error_new:=sqrt(standard_error^2*B3_SE^2+standard_error^2*B3^2+mean^2*B3_SE)] 
    post_data[case_type==alt, case_type:=paste0(alt,", crosswalked")]
    post_data[case_type==alt, c("lower","upper", "uncertainty_type_value"):=NA]
    
  } # end loop over alts
  
  post_data[!is.na(mean_new), mean:=mean_new]
  post_data[!is.na(standard_error_new), standard_error:=standard_error_new]
  post_data[,c("mean_new","standard_error_new"):=NULL]
  
  ## Remove lower and upper, epi uploader will recalculate based on new standard error
  post_data[mean>0,c("lower","upper", "uncertainty_type_value", "uncertainty_type",
                     "effective_sample_size", "cases", "sample_size", "seq"):=NA]
  
  ## Cap SE and mean at 1 and remove SE if mean=0
  post_data[standard_error>=1, standard_error:=0.999]
  if(measure_name=="prevalence" & nrow(post_data[mean>1,])>0) print("Warning: some means were crosswalked to more than 1")
  post_data[mean==0, standard_error:=NA]
  post_data <- post_data[mean!=0 | upper!=0,] ## remove if mean lower and upper are all zero
  
  ## add uncrosswalked rows back onto data
  crosswalked_ids <- unique(post_data$id)
  pre_crosswalked_rows <- dt[(id %in% crosswalked_ids)]
  pre_crosswalked_rows[, `:=` (group_review = 0, note_modeler = paste0(note_modeler, " | parent data, has been crosswalked"), 
                               specificity = paste0(note_modeler, " | crosswalk parent"))]
  dt <- rbind.fill(post_data, pre_crosswalked_rows)
  dt <- as.data.table(dt)
  dt[,id:=NULL]
  
  ###################################################
  ## Generate plots of crosswalked data
  ###################################################
  
  if(out_dir!=0){
    
    for(alt in alts){
      
      print(paste0("Generating ggplot for ",alt))
      
      pre_data_short <- pre_data[(case_type==alt|case_type==ref) & measure==measure_name &location_id%in%locs,
                                 c("location_id","nid","sex","age_mid","mean","case_type")]
      post_data_short <- post_data[(case_type==paste0(alt,", crosswalked") | case_type==ref) & measure==measure_name &location_id%in%locs,
                                   c("location_id","nid","sex","age_mid","mean","case_type")]
      
      data <- rbind(pre_data_short,post_data_short)
      
      gg<-ggplot(data=data,aes(x=age_mid,y=mean,color=case_type)) + geom_point(alpha=0.2) + geom_smooth(method=loess) + theme_classic()
      pdf(paste0(out_dir,"/",alt,"_crosswalk.pdf"))
      print(gg)
      dev.off()
      
    }
    
  }
  
  ## return crosswalked data.table
  return(dt)
  
}

## GET DATA
dt_orig <- get_epi_data(bundle_id = bundle)
dt <- dt_orig[!extractor == "" & !group_review == 0 & !nid == 212516]
dt[nid %in% niaaa_nids, `:=` (cv_niaaa = 1, cv_other = 0)]
dt[!nid %in% niaaa_nids, `:=` (cv_niaaa = 0, cv_other = 1)]

## CROSSWALK FROM MATCHED
post_xwalk <- crosswalk_from_cv(dt = dt, ref = c("other"), alts = c("niaaa"), measure_name = "prevalence", out_dir = doc_dir)
post_xwalk[, c("cv_niaaa", "cv_other", "case_type", "age_mid") := NULL]
post_xwalk[, note_modeler := gsub("outliering because use NIA-AA criteria (more inclusive)", "", note_modeler)]
write.xlsx(post_xwalk, paste0(upload_dir, "niaaacrosswalk_", date, ".xlsx"), sheetName = "extraction")
