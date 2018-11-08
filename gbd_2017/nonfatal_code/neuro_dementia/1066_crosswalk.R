#################################################################################################################
## 10/66 Dementia Crosswalk
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
##        measure="prevalence",out_dir="FILEPATH") where dt and matched_dt match the
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
ten66nids <- c(318173, 315454, 276374, 276350, 276336)
date <- gsub("-", "_", Sys.Date())

## SOURCE CENTRAL FUNCTION
source(paste0(functions_dir, "get_epi_data.R"))

## CROSSWALK FUNCTION
crosswalk_from_matched <- function(dt, matched_dt, ref, alts, out_dir){
  
  library(plyr)
  library(data.table)
  library(ggplot2)
  
  ###################################
  ## PREP MATCHED X-WALK DATA
  ###################################
  
  ## Input data
  xwalk_data <- as.data.table(matched_dt)
  
  ## subset on measure
  xwalk_data <- xwalk_data[measure==measure_name,]
  
  ## add case_type column
  ## epi template already has case_name, case_definition, and case_diagnostics fields
  types <- c(ref, alts)
  for(t in types){
    cv <- paste0("cv_",t)
    xwalk_data[get(cv)==1, case_type:=t]
  }
  
  xwalk_data <- xwalk_data[,c("case_type","nid","age_start","age_end","sex","location_id","location_name","sample_size","mean")]
  
  ## log transform mean
  xwalk_data[,mean:=log(mean)]
  
  ## Reshape data wide by case type
  xwalk_data <- dcast.data.table(xwalk_data, nid + age_start + age_end + sex +location_id + location_name ~ case_type, value.var = "mean")
  
  ## Generate age_mid variable (independent var in regression)
  xwalk_data[, age_mid := ((age_start+age_end)/2)]
  
  ####################################
  ## LOAD & PREP PRE-CROSSWALK BUNDLE
  ####################################
  
  dt <- as.data.table(dt)
  n <- nrow(dt)
  dt$id <- seq(1,n,1) ## set unique identifier 
  pre_data <- dt

  ## add case_type column
  ## epi template already has case_name, case_definition, and case_diagnostics fields
  types <- c(ref, alts)
  for(t in types){
    cv <- paste0("cv_",t)
    pre_data[get(cv)==1, case_type:=t]
  }
  
  ## Generate age_mid variable
  pre_data[, age_mid := (age_start+age_end)/2]
  
  ## save pre_data as post_data to preserve pre_data as unadjusted
  post_data <- copy(pre_data)
  
  ## drop mean=0 & log transform mean
  post_data <- post_data[mean>0,]
  post_data[, log_mean:=log(mean)]
  
  
  ####################################
  ## LOOP OVER ALT DEFINITION
  ####################################
  
  for(alt in alts){
    
    print(paste0("Crosswalking ", alt))
    
    #Subset xwalk data to rows with column entries for both ref and alt
    xwalk_subset <- xwalk_data[!is.na(get(ref)) & !is.na(get(alt)),]
    
    if(nrow(xwalk_subset)<10) print("WARNING: you have fewer than 10 rows to inform this crosswalk")
    
    # Perform regression and save as a function
    regression <- summary(lm(get(ref)~get(alt), data=xwalk_subset))
    print(regression)
    B0 <- regression$coefficients[1,1]
    B1 <- regression$coefficients[2,1]
    crosswalk <- function(mean, age){
      return(B0+B1*mean)
    }
    
    # Apply regression to data & propagte uncertainty
    post_data[case_type==alt, log_mean_new:=crosswalk(log_mean, age_mid)]
    B1_SE <- regression$coefficients[2,2]
    post_data_draws <- rnorm(1000, B1, B1_SE)
    cw_SE <- sd(exp(post_data_draws))
    post_data[case_type==alt, standard_error_new:=sqrt(standard_error^2*B1_SE^2+standard_error^2*B1^2+log_mean^2*B1_SE)] 
    post_data[case_type==alt, case_type:=paste0(alt,", crosswalked")]
    
  } # end loop over alts
  
  post_data[!is.na(log_mean_new), log_mean:=log_mean_new]
  post_data[!is.na(standard_error_new), standard_error:=standard_error_new]
  post_data[,c("log_mean_new","standard_error_new"):=NULL]
  
  # Reverse the log transformation
  post_data[,mean:=exp(log_mean)]
  
  ## Remove lower and upper, epi uploader will recalculate based on new standard error
  post_data[mean>0,c("lower","upper", "uncertainty_type_value", "uncertainty_type",
                     "effective_sample_size", "cases", "sample_size"):=NA]
  
  ## Cap SE at 1 and remove SE if mean=0
  post_data[standard_error>=1, standard_error:=0.999]
  post_data[mean==0, standard_error:=NA]
  post_data <- post_data[mean!=0 | upper!=0,] ## remove if mean lower and upper are all zero
  post_data[, seq := ""]
  
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
      
      pre_data_short <- pre_data[(case_type==alt|case_type==ref) & measure == "prevalence",  ## only show prevalence in graph
                                 c("location_id","nid","sex","age_mid","mean","case_type")]
      post_data_short <- post_data[(case_type==paste0(alt,", crosswalked") | case_type==ref) & measure == "prevalence",
                                   c("location_id","nid","sex","age_mid","mean","case_type")]
      
      data <- rbind(pre_data_short,post_data_short)
      
      gg<-ggplot(data=data,aes(x=age_mid,y=mean,color=case_type)) + 
        geom_point(alpha=0.2) + 
        geom_smooth(method=loess) + 
        labs(x = "Age", y = "Mean") +
        guides(color=guide_legend(title="Data Type")) +
        theme_classic()
      pdf(paste0(out_dir,"/",alt,"_crosswalk.pdf"))
      print(gg)
      dev.off()
    }
    
  }
  
  ## return crosswalked data.table
  return(dt)
  
}

## GET DATA
dt <- get_epi_data(bundle_id = bundle)
matched_dt <- copy(dt[nid == 116923 & specificity == "age,sex"])
matched_dt[grep("dsm", case_definition), cv_dsm := 1]
matched_dt[is.na(cv_dsm), cv_dsm := 0]
matched_dt[!grep("dsm", case_definition), cv_1066 := 1]
matched_dt[is.na(cv_1066), cv_1066 := 0]

pre_dt <- dt[nid %in% ten66nids]
pre_dt[, `:=` (cv_dsm = 0, cv_1066 = 1)]


## CROSSWALK FROM MATCHED
post_xwalk <- crosswalk_from_matched(dt = pre_dt, matched_dt = matched_dt, ref = c("dsm"), alts = c("1066"), out_dir = doc_dir)
post_xwalk[, c("cv_dsm", "cv_1066", "case_type", "age_mid", "log_mean") := NULL]
write.xlsx(post_xwalk, paste0(upload_dir, "1066crosswalk_", date, ".xlsx"), sheetName = "extraction")
