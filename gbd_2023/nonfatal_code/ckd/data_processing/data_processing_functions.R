#--------------------------------------------------------------
# Project: GBD Non-fatal CKD Estimation 
# Purpose: Functions to generate processed data from raw data bundles
#--------------------------------------------------------------

# setup -------------------------------------------------------------------
user <- Sys.getenv('USER')
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
} else { 
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}

require(data.table)
require(msm)

# source
ckd_repo<- paste0("FILEPATH",user,"FILEPATH")
source(paste0(ckd_repo,"general_func_lib.R"))
invisible(sapply(list.files("FILEPATH", full.names = T), source))
source(paste0(ckd_repo,"FILEPATH/add_population_denominator.R"))

library(reticulate)
reticulate::use_python("FILEPATH/python")
cw <- reticulate::import("crosswalk")

#   -----------------------------------------------------------------------


# functions ---------------------------------------------------------------

# Age-sex split data from sources reporting both age-specific and sex-specific separately
age_sex_split_lit<-function(dt,split_indicator, bid, output_path){
  # Subset to data needing to be age-sex split
  dt_split<-copy(dt[get(split_indicator)==1])
  dt_nosplit <- setdiff(dt, dt_split)
  
  # Get covariate column names
  cvs <- names(dt_split)[grepl("^cv", names(dt_split))]  
  
  # Fix specificity column
  dt_split[sex == "Both", specificity := "age"]
  dt_split[sex %in% c("Male", "Female"), specificity := "sex"]
  
  # Check that all rows have specificity that is either age or sex 
  if (any(!unique(dt_split[,specificity])%in%c("age","sex"))){
    stop(paste0("There are rows where",split_indicator,"is TRUE but specificity is not age or sex."))
  }
  
  # Create datatable with just sex specific data
  dt_sex<-dt_split[specificity=="sex"]
  
  # Check that there are not rows marked as both sex
  if(any(unique(dt_sex[,sex])%in%c("Both"))){
    stop("There are rows in this bundle that are marked as sex = Both and specificity = sex")
  }
  
  # Calculate proportion of cases male and female
  dt_sex[, cases_total:= sum(cases), by = c("nid", "group", "specificity", "measure", "location_id", "year_start", "year_end", cvs)]
  dt_sex[, prop_cases := cases / cases_total]
  
  # Calculate proportion of sample male and female
  dt_sex[, ss_total:= sum(sample_size), by = c("nid", "group", "specificity", "measure", "location_id", "year_start", "year_end", cvs)]
  dt_sex[, prop_ss := sample_size / ss_total]
  
  # Calculate standard error of % cases & sample_size M and F
  dt_sex[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  dt_sex[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  # Estimate the ratio and standard error of the ratio of %cases/%sample 
  dt_sex[, ratio := prop_cases / prop_ss]
  dt_sex[, se_ratio:= sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) )]
  
  # Subset to necessary cols
  if ("seq" %in% names(dt_sex)){
    dt_sex <- subset(dt_sex, select = c("nid", "group", "measure", "location_id", "year_start", "year_end", cvs, "seq", "sex", "ratio", "se_ratio", "prop_cases", "prop_ss"))
  }else{
    dt_sex <- subset(dt_sex, select = c("nid", "group", "measure", "location_id", "year_start", "year_end", cvs, "sex", "ratio", "se_ratio", "prop_cases", "prop_ss"))
  }
  
  # Rename seq of sex-specific rows
  if("seq" %in% names(dt_sex)) setnames(dt_sex,"seq","parent_seq_sex")
  
  # Prep age-specific rows for split
  dt_age<-copy(dt_split[specificity=="age"])
  
  # Rename seq of age-specific rows 
  if("seq" %in% names(dt_age)) setnames(dt_age,"seq","parent_seq_age")
  
  # Duplicate age dt for both sexes
  dt_age_m<-copy(dt_age[,sex:="Male"])
  dt_age_f<-copy(dt_age[,sex:="Female"])
  dt_age<-rbindlist(list(dt_age_m,dt_age_f),use.names=T)
  
  # Merge on sex ratios
  dt_age<-merge(dt_age,dt_sex,by=c("nid", "group", "measure", "location_id", "year_start", "year_end", cvs, "sex"))
  
  # Calculate age-sex specific mean, standard_error, cases, sample_size
  dt_age[, mean := mean * ratio]
  dt_age[, standard_error := sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2)]
  dt_age[, cases := cases * prop_cases]
  dt_age[, sample_size := sample_size * prop_ss]
  if (!("note_modeler")%in%names(dt_age)) dt_age[,note_modeler:=NA]
  dt_age[, note_modeler := ifelse(is.na(note_modeler),paste("age,sex split using sex ratio", round(ratio, digits = 2)),
                                  paste(note_modeler, "| age,sex split using sex ratio", round(ratio, digits = 2)))]
  dt_age[, c("ratio", "se_ratio", "prop_cases", "prop_ss") := NULL]
  
  # Adjust group, group_review, specficity, and seq for new rows
  dt_age[,group_review:=1]
  dt_age[,specificity:="age,sex"]
  if ("seq" %in% names(dt_age)){
    dt_age[,seq:=""]
    dt_age[,seq_parent:=do.call(paste,c(.SD, sep = ",")),.SDcols=c("parent_seq_age","parent_seq_sex")]
  } 
  
  # Clear other uncertainty information
  dt_age[,`:=`(lower=NA, upper=NA, uncertainty_type_value=NA, uncertainty_type=NA, effective_sample_size=NA)]
  
  # Check rows where mean > 1
  print(paste0("any rows with mean>1 after processing? ", any(dt_age$mean>1)))
  dt_age[mean > 1, `:=` (group_review = 0, note_modeler = paste0(note_modeler, " | group reviewed out because age-sex split over 1"))]
  
  # write out csvs with pre and post split data for possible vetting later
  write.csv(dt_split, paste0(output_path,"FILEPATH",bid, "_pre_age_sex_lit_split.csv"))
  write.csv(dt_age, paste0(output_path,"FILEPATH",bid, "_post_age_sex_lit_split.csv"))
  
  # Return df
  return_dt<-rbindlist(list(dt_nosplit,dt_age),use.names=T,fill=T)
  return(return_dt)
  
}



# Apply MRBRT crosswalk
apply_mrbrt_xwalk <- function(input_dt, bid, alt_def, cv_ref_map_path, output_path){
  # read in cv_ref_map
  cv_ref_map<-fread(cv_ref_map_path)
  cv_ref_map<-cv_ref_map[bundle_id==bid & alt == alt_def] 
  if (nrow(cv_ref_map)>1) stop("the bundle_id-alt_def combination supplied indentifies more than one crosswalk model in the cv_ref_map")
  alt_indicator_cols<-unlist(str_split(cv_ref_map[, unique(cv_alt_indicator_cols)],","))
  ac<-cv_ref_map[, unique(acause)]
  stg<-cv_ref_map[, unique(stage)]
  cv_type<-cv_ref_map[, unique(cv)]
  ref_def<-cv_ref_map[, unique(reference)]
  mod_type<-cv_ref_map[, unique(xwalk_type)]
  comparison<-paste0(stg,"_",paste0(cv_type,collapse="_"),"_",
                     paste0(alt_def,collapse="_"),"_to_",
                     paste0(ref_def,collapse="_"))
  
  message(paste0("Applying crosswalk for ", stg,": crosswalking ", alt_def," points to ", ref_def))
  xwalk_lab<-paste0(stg, " - ", alt_def, " to ", ref_def,":")
  
  # split into alt and reference data sets
  message(paste(xwalk_lab,"separating data into alt and reference datasets - ignoring any data that is marked with group_review = 0"))
  toxwalk_dt <- copy(input_dt)
  noxwalk_dt <- toxwalk_dt[eval(parse(text = paste0("!(",paste0(alt_indicator_cols," %in% c(1)",collapse = "&"), ") | (group_review %in% c(0)) | (mean == 0)", collapse = "")))]
  toxwalk_dt <- fsetdiff(toxwalk_dt,noxwalk_dt)
  
  if (nrow(toxwalk_dt) > 0) {
    
    message(paste(xwalk_lab,"preparing to crosswalk",nrow(toxwalk_dt),"of", nrow(input_dt),"rows of data"))
    
    # calculate missing SE
    toxwalk_dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
    z <- qnorm(0.975)
    toxwalk_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    toxwalk_dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    toxwalk_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    toxwalk_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
    
    # pull in mrbrt model
    message(paste(xwalk_lab,"reading in MRBRT model"))
    model_parent_dir<-paste0(j_root,"FILEPATH")
    if (mod_type == "all_age") {
      xwalk_mod_dir <- "no_covs/"
    }
    if (mod_type == "age_spec") {
      xwalk_mod_dir <- "age_spline/"
    }
    fit1 <- py_load_object(filename = paste0(model_parent_dir, xwalk_mod_dir, comparison, "_mrbrt_object.pkl"), pickle = "dill")
    
    # create covariates columns in data to crosswalk, if applicable
    if (mod_type=="age_spec"){
      toxwalk_dt[,age_midpoint:=(age_start+age_end)/2]
    }
    
    # first deal with 0s and 1s in data to crosswalk - cannot be adjusted for log or logit scale crosswalks
    # 0s are already not included in toxwalk_dt
    message(paste(xwalk_lab," : ",nrow(toxwalk_dt[mean==1]),"of", nrow(toxwalk_dt),"datapoints to be crosswalked with mean = 1"))
    
    if (nrow(toxwalk_dt[mean==1]) > 0) {
      stop(paste0("There are ", nrow(toxwalk_dt[mean==1]), " datapoints to be crosswalked with mean = 1, this will throw an error when applying crosswalk betas"))
    }
    
    # add column for alt def because that's required for crosswalk package adjust_orig_vals function
    toxwalk_dt[, orig_alt_def := alt_def]
    
    # apply crosswalk adjustment using crosswalk package 
    pred_tmp <- fit1$adjust_orig_vals(
      df = toxwalk_dt,
      orig_dorms = "orig_alt_def",
      orig_vals_mean = "mean", # mean in un-transformed space
      orig_vals_se = "standard_error" # SE in un-transformed space
    )
    
    toxwalk_dt[, c(
      "meanvar_adjusted", "sdvar_adjusted", "pred_logit",
      "pred_se_logit", "data_id")] <- pred_tmp
    
    toxwalk_dt[, c("meanvar_adj_logit", "sdvar_adj_logit")]<-as.data.frame(cw$utils$linear_to_logit(
      mean = array(toxwalk_dt$meanvar_adjusted),
      sd = array(toxwalk_dt$sdvar_adjusted)))
    toxwalk_dt$lower_adj_logit<- toxwalk_dt$meanvar_adj_logit - 1.96*toxwalk_dt$sdvar_adj_logit
    toxwalk_dt$upper_adj_logit<- toxwalk_dt$meanvar_adj_logit + 1.96*toxwalk_dt$sdvar_adj_logit
    
    toxwalk_dt[,lower_adj:=exp(lower_adj_logit)/(1+exp(lower_adj_logit)) ]
    toxwalk_dt[,upper_adj:=exp(upper_adj_logit)/(1+exp(upper_adj_logit)) ]
    
    # write out csvs with pre and post crosswalk data for possible vetting later
    if ("date_added_to_bundle" %in% colnames(toxwalk_dt)) {
      toxwalk_dt_temp <- subset(toxwalk_dt, select = -c(date_added_to_bundle))
    }
    if ("date_to_added_bundle" %in% colnames(toxwalk_dt_temp)) {
      toxwalk_dt_temp <- subset(toxwalk_dt_temp, select = -c(date_to_added_bundle))
    }
    
    write.csv(toxwalk_dt_temp, paste0(output_path,"FILEPATH/bundle_",bid, "_", comparison, "_crosswalk.csv"))
    
    # recode adjusted values 
    toxwalk_dt[,mean:=meanvar_adjusted]
    toxwalk_dt[,standard_error:=sdvar_adjusted]
    toxwalk_dt[,upper:= upper_adj]
    toxwalk_dt[,lower:=lower_adj]
    
    # remove extra columns
    toxwalk_dt <- as.data.frame(toxwalk_dt)
    toxwalk_dt <- toxwalk_dt[, intersect(colnames(noxwalk_dt), colnames(toxwalk_dt))]
    toxwalk_dt <- as.data.table(toxwalk_dt)
    
    # fix uncertainty information
    toxwalk_dt[,`:=`(uncertainty_type_value=95)]
    
    # change seq to crosswalk_parent_seq where crosswalk parent seq is na 
    toxwalk_dt[is.na(crosswalk_parent_seq),crosswalk_parent_seq:=as.integer(seq)]
    toxwalk_dt[,seq:=NULL]
    
    total_dt <- rbindlist(list(noxwalk_dt, toxwalk_dt), use.names=T, fill = T)
    return(total_dt)
  } else {
    message(paste(xwalk_lab,"has 0 rows to crosswalk"))
    return(input_dt)
  }
}


#   -----------------------------------------------------------------------

