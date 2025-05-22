##################################################################################################################################### 
# Purpose: To create a new custom CF to adjust for high outpatient cases in total hernia
######################################################################################################################################

rm(list=ls())


## Source central functions
pacman::p_load(data.table, openxlsx, ggplot2, readr, RMySQL, openxlsx, readxl, stringr, tidyr, plyr, dplyr, gtools)
library(msm, lib.loc = FILEPATH)
library(Hmisc, lib.loc = FILEPATH)
library(metafor, lib.loc=FILEPATH)
base_dir <- FILEPATH
functions <- c("get_bundle_data", "upload_bundle_data", "get_bundle_version",
               "save_bundle_version", "get_crosswalk_version", "save_crosswalk_version", "save_bulk_outlier", "get_age_metadata", "get_ids", "get_location_metadata")
lapply(paste0(base_dir, functions, ".R"), source)

###FOR GBD 2019: 
cause_path <- FILEPATH
cause_name <- CAUSE_NAME

####CUSTOM FUNCTIONS:
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}
aggregate_hosp <- function(mark_dt){
  df <- copy(mark_dt)
  df1 <- subset(df, nid == 234766 | nid ==234769 | nid ==234771 |nid ==234772 | nid ==234774 )
  dt <- subset(df, nid ==284421 | nid ==284422 )
  by_vars <- c("age_start", "age_end", "year_start", "year_end", "sex")
  dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = by_vars]
  dt[, `:=` (location_id = 102, location_name = "United States", mean = cases/sample_size,
             lower = NA, upper = NA)]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  dt <- unique(dt, by = by_vars)
  full_dt <- rbind(dt, df1, fill=TRUE)
  return(full_dt)
}
reorder_columns <- function(datasheet){
  ## set ordered list of columns for master crosswalk csv
  template_cols <- c("nid", "age_start", "age_end", "sex", "year_start", "year_end", "location_id", "location_name", "mean","standard_error","cases","sample_size", "clinical_data_type")
  col_order <- template_cols
  ## find which column names are in the extraction template but not in your datasheet
  to_fill_blank <- c()  
  for(column in col_order){
    if(!column %in% names(datasheet)){
      to_fill_blank <- c(to_fill_blank, column)
    }}
  ## create blank column which will be filled in for columns not in your datasheet
  len <- length(datasheet$nid.x)
  blank_col <- rep.int(NA,len)
  
  ## for each column not found in your datasheet, add a blank column and rename it appropriately
  for(column in to_fill_blank){
    datasheet <- cbind(datasheet,blank_col)
    names(datasheet)[names(datasheet)=="blank_col"]<-column
  }
  ## for columns in datasheet but not in epi template or cv list, delete
  dt_cols <- names(datasheet)
  datasheet <- as.data.table(datasheet)
  for(col in dt_cols){
    if(!(col %in% col_order)){
      datasheet[, c(col):=NULL]
    }}
  ## reorder columns with template columns
  setcolorder(datasheet, col_order)
  ## return
  return(datasheet)   
}    

date <- Sys.Date()
date <- gsub("-", "_", date)

sex_dt <- read.csv(paste0(FILEPATH))
sex_dt <- subset(sex_dt, sex_id==c(1, 2))


#For MAD outlier
MAD <- 2
output_filepath_bundle_data <- FILEPATH
output_filepath_mad <- FILEPATH
age_using <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235 ) 
byvars <- c("location_id", "sex", "year_start", "year_end", "nid") 

#Bulk outlier 
outlier_list <- as.data.table(read_excel(FILEPATH))

########################################################################################
## Data prep for OTP:PROC ratios
########################################################################################
dt <- as.data.table(read.csv(FILEPATH))

## aggregate num_enrolid_with_proc_type and num_enrolid_with_dx_type over all years
by_vars = c("sex_id", "age_start", "type")
dt[, `:=` (num_enrolid_with_proc = sum(num_enrolid_with_proc), num_enrolid_with_dx = sum(num_enrolid_with_dx)), by = by_vars]
dt <- unique(dt, by = by_vars)
dt$year <- NULL
dt[, c("cf", "location_name", "location_id")] <- NULL

## update the age end
dt <- as.data.table(dt)
dt[(age_start!=0), age_end2 := age_end-1]
dt[(age_start==0), age_end2 := 0.999]
dt$age_end <- NULL
setnames(dt, "age_end2", "age_end")

## subset to outpatient records only
dt_otp<-subset(dt, type=="OTP")
dt_otp[, opt_proc := num_enrolid_with_proc / num_enrolid_with_dx]
dt_otp[, opt_nonproc := 1-opt_proc]

## add sex variables before merging
sex_names <- get_ids(table = "sex")
dt_otp <- merge(dt_otp, sex_names)
dt_otp$sex_id <- NULL

all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=19))
all_fine_ages <- all_fine_ages[, c("age_group_id", "age_group_name", "age_group_years_start", "age_group_years_end")]
setnames(all_fine_ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
all_fine_ages[age_group_id ==389, age_end := 0.999]
not_babies <- all_fine_ages[!age_group_id %in% c(2:3, 388, 389)]
not_babies[, age_end := age_end-1]
group_babies1 <- all_fine_ages[age_group_id %in% c(2:3, 388, 389)]
group_babies1$age_group_id <- 28
group_babies1$age_end <- 0.999
group_babies1$age_start <- 0
group_babies1$age_group_name <- "0 to 0.999"
group_babies1 <- group_babies1[, c("age_start", "age_end", "age_group_id", "age_group_name")]
group_babies1 <- unique(group_babies1)
all_ages <- rbind(not_babies, group_babies1)  


all_fine_ages[(age_start>0.99), age_end := age_end -1 ]

dt_otp <- merge(dt_otp, all_ages)

########################################################################################
## Inpatient hospital datasets (CF2 and CF3)
########################################################################################
bundle_version_id=BUNDLE_VERSION_ID
df_all = get_bundle_version(bundle_version_id, fetch='all', export = FALSE)
cf3_dt <- copy(df_all)
cf3_dt <- subset(cf3_dt, clinical_data_type=="inpatient")
cf3_dt <- cf3_dt[, c("mean", "lower", "upper", "standard_error", "cases", "sample_size", "location_id", "sex", "age_start", "year_start", "year_end", "nid", "location_name", "age_end", "measure",  "field_citation_value", "clinical_data_type",
                     "unit_type",	"unit_value_as_published",	"uncertainty_type_value",	"representative_name",	"urbanicity_type",	"recall_type",	"recall_type_value",	"sampling_type",	"seq",	"origin_seq",	"origin_id",	"design_effect",	"is_outlier",
                    "source_type", "input_type",	"uncertainty_type",	"underlying_field_citation_value")]
cf3_dt <- get_cases_sample_size(cf3_dt)
cf3_dt <- get_se(cf3_dt)
cf3_dt <- calculate_cases_fromse(cf3_dt)

cf2_dt <- as.data.table(read.csv(FILEPATH))
cf2_dt$measure <- "prevalence"

cf2_dt <- get_cases_sample_size(cf2_dt)
cf2_dt <- get_se(cf2_dt)
cf2_dt <- calculate_cases_fromse(cf2_dt)

cf2_dt <- merge(cf2_dt, sex_dt, by="sex_id")
cf2_dt <- merge(cf2_dt, all_fine_ages, by="age_group_id")

merge_cov <- c("location_id", "sex", "age_start", "age_end", "year_start", "year_end", "nid")
merged_dt <- merge(cf2_dt, cf3_dt, by = merge_cov)

## To create a variable with CF3 - CF2
merged_dt[, cf3_cf2 := mean.y - mean.x]

## To merge <1 and >1 groups separately
merged_dt_baby <- subset(merged_dt, age_start <1)
merged_dt1 <- subset(merged_dt, age_start >0.99)
dt_otp_baby <- subset(dt_otp, age_start <1)
dt_otp_baby[, c("age_start", "age_end", "age_group_id", "age_group_name")] <- NULL

## To merge merged_dt and dt2
merged_dt1 <- merge(merged_dt1, dt_otp, by = c("sex", "age_start", "age_end"))
merged_dt_baby <-merge(merged_dt_baby, dt_otp_baby, by = c("sex"))
merged_dt1 <- as.data.table(rbind.fill(merged_dt_baby, merged_dt1))

## To calculate new_MSinp: CF2 + (CF3-CF2)((opt:proc)))
merged_dt1[, opt_proc_hosp := cf3_cf2*opt_proc]
merged_dt1[, new_MSinp := (mean.x + opt_proc_hosp)]

## To calculate new CF: ALL / new_MSinp = ALL / (Cf2 + opt_proc)
merged_dt1[, new_CF := (mean.y / new_MSinp)]

## To calculated newly adjusted mean: CF2 * new_CF
merged_dt1[, mean := (mean.x *new_CF)]

verify <- merged_dt1[, c("sex", "mean.x", "mean.y", "opt_proc", "new_MSinp", "new_CF", "mean", "location_name")]

write.csv(merged_dt1, FILEPATH, row.names = F)



############################################################################################################
## TAKING 1000 DRAWS TO ESTIMATE NEW CONFIDENCE INTERVALS FOR ADJUSTED MEAN
############################################################################################################
##generate vector of 1000 draws of distribution centered around scalar w/ standard errors 
df_merge_zero <- subset(merged_dt1, mean.y==0)
  df_merge_zero$mean <- df_merge_zero$mean.y
  df_merge_zero$lower <- df_merge_zero$lower.y
  df_merge_zero$upper <- df_merge_zero$upper.y
  df_merge_zero$standard_error <- df_merge_zero$standard_error.y

  
df_merge_nonzero <- subset(merged_dt1, mean.y!=0)
df_merge_nonzero$sd <- (df_merge_nonzero$upper.x-df_merge_nonzero$lower.x)/(2*1.96)  
    
custom_df_adj = copy(as.data.table(df_merge_nonzero))
    draws <- 1000
    for (i in 1:draws) {
      draw_name = paste0("draw_", i)
      custom_df_adj[[draw_name]] = (rnorm(1:nrow(custom_df_adj),
                                                   mean = custom_df_adj$mean.x,
                                                   sd = custom_df_adj$sd))
      }



    draw_cols <- paste0("draw_",1:1000)
    custom_df_adj[, paste0("draw_",1:1000) := lapply(1:1000, function(x) get(paste0("draw_", x)) * new_CF )]
    
    # COLLAPSE EACH ROW TO MEAN, UPPER, LOWER
    custom_df_adj$mean<- rowMeans(custom_df_adj[, 69:1068], na.rm = T)
    custom_df_adj$lower<- custom_df_adj[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
    custom_df_adj$upper <- custom_df_adj[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
    custom_df_adj <- custom_df_adj[, -(draw_cols), with=FALSE] ## OR: custom_df_adj[, c(draws, paste0("draw_", 1:1000)) := NULL]
    custom_df_adj$standard_error <- NA
    custom_df_adj$cases <- NA
    
    custom_df_adj <- rbind(df_merge_zero, custom_df_adj, fill=TRUE)
    custom_df_adj$crosswalk_parent_seq <- NA
    custom_df_adj$crosswalk_parent_seq <-custom_df_adj$seq
    custom_df_adj$cv_hospital <- 1

    custom_df_adj[, c("merged_nid", "run_id",	"measure_id",	"sample_size.x",	"cases.x", "standard_error.x",		"lower.x",	"upper.x",
                      "date_inserted.x",	"inserted_by",	"last_updated.x",	"last_updated_by.x",	"last_updated_action.x", "age_end.x",	
                      "age_group_weight_value", "is_estimate	most_detailed","parent_id",	"path_to_top_parent",	"level", "start_date",	"end_date",
                      "date_inserted.y",	"last_updated.y",	"last_updated_by.y",	"last_updated_action.y",	"lancet_label",	"lower.y",	"upper.y",
                      "standard_error.y",	"cases.y",	"sample_size.y", "sd")] <- NULL
    
    
    custom_df_adj[, c("source_type_id",	"diagnosis_id", "measure.x",	"age_group_name",	"most_detailed.x",	"location_set_version_id",	"location_set_id",	"is_estimate",
                      "most_detailed.y",	"sort_order", "location_name.x",	"location_ascii_name",	"location_name_short",	"location_type_id",	"location_type",	"map_id",
                      "super_region_id",	"super_region_name",	"region_id",	"region_name",	"ihme_loc_id",	"local_id",	"developed",	"date_inserted",	"last_updated",
                      "last_updated_by",	"last_updated_action",	"who_label", "location_name.y")] <- NULL
    
  
    custom_df_adj$lower[custom_df_adj$lower <0] <- 0
    custom_df_adj$note_modeler <- ""
    custom_df_adj[, note_modeler := paste0(note_modeler, " | corrected with custom CF")]
    
    custom_df_adj <- subset(custom_df_adj, mean >=0) 
    setnames(custom_df_adj, c("measure.y", "uncertainty_type_value.y"), c("measure", "uncertainty_type_value"))
    custom_df_adj$underlying_nid <- NA
    custom_df_adj$effective_sample_size <- NA
    custom_df_adj$sample_size <- NA
    custom_df_adj$standard_error[custom_df_adj$standard_error > 1] <- 1
    
    custom_df_adj[, c("age_group_id", "age_group_id.x", "age_group_id.y", "age_group_name", "age_group_name.x", "age_group_name.y")] <- NULL
    
    write.csv(custom_df_adj, FILEPATH, row.names = F)
    custom_df_adj <- as.data.table(read.csv(FILEPATH))
    
########################################################################################################
### Xwalk new Marketscan data
    cf3_dt <- subset(df_all, clinical_data_type=="claims")
    cf3_dt <- cf3_dt[, c("mean", "lower", "upper", "standard_error", "cases", "sample_size", "location_id", "sex", "age_start", "year_start", "year_end", "nid", "location_name", "age_end", "measure",  "field_citation_value", "clinical_data_type",
                         "unit_type",	"unit_value_as_published",		"uncertainty_type_value",	"representative_name",	"urbanicity_type",	"recall_type",	"recall_type_value",	"sampling_type",	"extractor",	"seq",	"origin_seq",	"origin_id",	"design_effect",	"is_outlier",
                         "effective_sample_size", "source_type", "input_type",	"uncertainty_type",	"underlying_field_citation_value")]
    
    cf3_dt = within(cf3_dt, {cv_ms2000 = ifelse(nid==244369, 1, 0)})
    cf3_dt = within(cf3_dt, {cv_marketscan = ifelse((nid==244370 | nid== 336850| nid== 244371| nid== 336849| nid== 336848| nid== 336847| nid== 408680 | nid==433114), 1, 0)})
    
    dt_xwalk <- as.data.table(subset(cf3_dt, cv_marketscan ==1 | cv_ms2000==1))
    dt_noxwalk <- as.data.table(subset(cf3_dt, cv_marketscan ==0 & cv_ms2000==0))
    
    ##create "obs_method" variable
    dt_xwalk[(cv_ms2000==1), `:=` (obs_method = "ms2000")]
    dt_xwalk[(cv_marketscan==1), `:=` (obs_method = "marketscan")]
    custom_df_adj[, `:=` (obs_method = "inpatient")]
    
    ##set crosswalk objects
    method_var <- "obs_method"
    gold_def <- "inpatient"
    year_range <- 5
    
    ##calculate mid-year
    dt_xwalk$mid_year <- (dt_xwalk$year_start + dt_xwalk$year_end)/2
    custom_df_adj$mid_year <- (custom_df_adj$year_start + custom_df_adj$year_end)/2
    custom_df_adj <- get_se(custom_df_adj)
    
    #subset matches that are mean = 0, cann't logit transform
    df_zero <- subset(dt_xwalk, mean ==0 )
    unique(df_zero$obs_method) #check if any of the mean = 0 data points are from alternative case definitions
    df_nonzero <- subset(dt_xwalk, mean >0 & mean <1 & nid!=416752) #drop all rows of data with mean greater than 1

    df_nonzero_ms2000 <- subset(df_nonzero, cv_ms2000 ==1)
    df_nonzero_ms <- subset(df_nonzero, cv_marketscan ==1)

    custom_df_adj1 <- subset(custom_df_adj, mean!=0 )
    
    #logit transform mean and delta-transform SE
    library(crosswalk, lib.loc = FILEPATH)
    df_nonzero[, c("mean_logit", "se_logit")] <- as.data.frame(delta_transform(
                                                            mean = df_nonzero$mean, 
                                                            sd = df_nonzero$standard_error,
                                                            transformation = "linear_to_logit"))
    custom_df_adj1[, c("mean_logit", "se_logit")] <- as.data.frame(delta_transform(
                                                              mean = custom_df_adj1$mean, 
                                                              sd = custom_df_adj1$standard_error,
                                                              transformation = "linear_to_logit"))
                                                          
    
    
    #match alt to ref
    #1. subset datasets based on case definition
    ref <- as.data.table(copy(custom_df_adj1))
    alt_ms2000 <- subset(df_nonzero, cv_ms2000==1)
    alt_ms <- subset(df_nonzero, cv_marketscan==1)
    
    #2. clean ref and alt datasets
    alt_ms2000 <- alt_ms2000[, c("nid", "location_id", "sex", "age_start", "age_end",  "mid_year", "measure", "mean", "standard_error", "cv_ms2000","mean_logit", "se_logit")]
    alt_ms <- alt_ms[, c("nid", "location_id", "sex", "age_start", "age_end", "mid_year", "measure", "mean", "standard_error",  "cv_marketscan","mean_logit", "se_logit")]

    #3. change the mean and standard error variables in both ref and alt datasets for clarification
    setnames(ref, c("mean", "standard_error", "mid_year", "nid", "mean_logit", "se_logit"), c("prev_ref", "prev_se_ref", "mid_year_ref", "nid_ref", "mean_logit_ref", "se_logit_ref"))
    setnames(alt_ms2000, c("mean", "standard_error", "mid_year", "nid", "mean_logit", "se_logit"), c("prev_alt", "prev_se_alt", "mid_year_alt", "nid_alt", "mean_logit_alt", "se_logit_alt"))
    setnames(alt_ms, c("mean", "standard_error", "mid_year", "nid", "mean_logit", "se_logit"), c("prev_alt", "prev_se_alt", "mid_year_alt", "nid_alt", "mean_logit_alt", "se_logit_alt"))
    
    #4. Create a variable identifying different case definitions
    alt_ms2000$dorm_alt<- "ms2000"
    alt_ms$dorm_alt<- "marketscan"
    ref$dorm_ref<- "inpatient"
    
    #5. between-study mathces based on exact location, sex, age
    df_matched_ms2000 <- merge(ref, alt_ms2000, by = c("location_id", "sex", "age_start", "age_end", "measure"))
    df_matched_ms     <- merge(ref, alt_ms, by = c( "location_id", "sex", "age_start", "age_end", "measure"))
    
    #6 between-study matches based on 5-year span
    df_matched_ms2000 <- df_matched_ms2000[abs(df_matched_ms2000$mid_year_ref - df_matched_ms2000$mid_year_alt) <= year_range, ] 
    df_matched_ms <- df_matched_ms[abs(df_matched_ms$mid_year_ref - df_matched_ms$mid_year_alt) <= year_range, ] 
    
    #7. calculate logit difference : logit(prev_alt) - logit(prev_ref)
    df_matched_ms2000[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
      df = df_matched_ms2000, 
      alt_mean = "mean_logit_alt", alt_sd = "se_logit_alt",
      ref_mean = "mean_logit_ref", ref_sd = "se_logit_ref")
    
    df_matched_ms[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
      df = df_matched_ms, 
      alt_mean = "mean_logit_alt", alt_sd = "se_logit_alt",
      ref_mean = "mean_logit_ref", ref_sd = "se_logit_ref")
    
    #9. create study_id 
    df_matched_ms2000 <- as.data.table(df_matched_ms2000)
    df_matched_ms2000[, id := .GRP, by = c("nid_ref", "nid_alt")]
    
    df_matched_ms <- as.data.table(df_matched_ms)
    df_matched_ms[, id := .GRP, by = c("nid_ref", "nid_alt")]
    
    #10. fill in cv_ms2000 and cv_markestcan
    df_matched_ms[(is.na(cv_marketscan)), `:=` (cv_marketscan=0)]
    df_matched_ms2000[(is.na(cv_ms2000)), `:=` (cv_ms2000=0)]
    
    
    #CWData() formats meta-regression data  
    dat1_ms2000 <- CWData(df = df_matched_ms2000, 
                          obs = "logit_diff",                         #matched differences in logit space
                          obs_se = "logit_diff_se",                   #SE of matched differences in logit space
                          alt_dorms = "dorm_alt",                     #var for the alternative def/method
                          ref_dorms = "dorm_ref",                     #var for the reference def/method
                          covs = list("age_start"),  #list of (potential) covariate columes 
                          study_id = "id" )
    
    dat1_ms <- CWData(df = df_matched_ms, 
                      obs = "logit_diff",                         #matched differences in logit space
                      obs_se = "logit_diff_se",                   #SE of matched differences in logit space
                      alt_dorms = "dorm_alt",                     #var for the alternative def/method
                      ref_dorms = "dorm_ref",                     #var for the reference def/method
                      covs = list("age_start"),  #list of (potential) covariate columes 
                      study_id = "id" )
    
    
    #CWModel() runs mrbrt model
    fit1_ms2000 <- CWModel(
                        cwdata = dat1_ms2000,                       #result of CWData() function  call
                        obs_type = "diff_logit",                    #must be "diff_logit" or "diff_log"
                        cov_models = list(
                          CovModel("age_start"),
                          CovModel("intercept")),                   #specify covariate details
                        gold_dorm = "inpatient" )                   #level of "dorm_ref" that is the gold standard
                      
    fit1_ms <- CWModel(
                      cwdata = dat1_ms,                             
                      obs_type = "diff_logit",                   
                      cov_models = list(
                         CovModel("age_start"),
                        CovModel("intercept")),      
                      gold_dorm = "inpatient" )   
    
    fit1_ms2000$fixed_vars
    fit1_ms$fixed_vars
    
    ## check model output
    df_result_ms2000 <- fit1_ms2000$create_result_df()
    df_result_ms <- fit1_ms$create_result_df()
    df_result <- rbind.fill(df_result_ms2000, df_result_ms)
    xwalk_output <- FILEPATH
    write.xlsx(df_result, xwalk_output) 

    
    
    ## apply crosswalk coefficients to original data
    setnames(df_nonzero_ms2000, c("mean", "standard_error"), c("orig_mean", "orig_standard_error"))
    df_nonzero_ms2000[, c("mean", "standard_error", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
      fit_object = fit1_ms2000,       # result of CWModel()
      df = df_nonzero_ms2000,            # original data with obs to be adjusted
      orig_dorms = "obs_method", # name of column with (all) def/method levels
      orig_vals_mean = "orig_mean",  # original mean
      orig_vals_se = "orig_standard_error"  # standard error of original mean
    )
    
    setnames(df_nonzero_ms, c("mean", "standard_error"), c("orig_mean", "orig_standard_error"))
    df_nonzero_ms[, c("mean", "standard_error", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
      fit_object = fit1_ms,       # result of CWModel()
      df = df_nonzero_ms,            # original data with obs to be adjusted
      orig_dorms = "obs_method", # name of column with (all) def/method levels
      orig_vals_mean = "orig_mean",  # original mean
      orig_vals_se = "orig_standard_error"  # standard error of original mean
    )
    
    
    #apply crosswalk variance to mean = zero data points of alternative case definition
    preds <- fit1_ms$fixed_vars
    preds <- as.data.frame(preds)
    preds <- rbind(preds, fit1_ms$beta_sd)
    
    cvs <- c("marketscan")    
    df_zero$note_modeler <- ""
    for (cv in cvs) {
      ladj <- preds[cv][[1]][1]
      ladj_se <- preds[cv][[1]][2]
      df_zero[obs_method == cv , `:=` (ladj = ladj, ladj_se = ladj_se)]
      df_zero$adj_se <- sapply(1:nrow(df_zero), function(i){
        mean_i <- as.numeric(df_zero[i, "ladj"])
        se_i <- as.numeric(df_zero[i, "ladj_se"])
        deltamethod(~exp(x1)/(1+exp(x1)), mean_i, se_i^2)
      })
      df_zero[, `:=` (standard_error = sqrt(standard_error^2 + adj_se^2), 
                      note_modeler = paste0(note_modeler, " | uncertainty from network analysis added"))]
      df_zero[, `:=` (crosswalk_parent_seq = seq, seq = NA)]
    } 
    
    df_nonzero_ms$crosswalk_parent_seq <- df_nonzero_ms$seq
    df_nonzero_ms2000$crosswalk_parent_seq <- df_nonzero_ms2000$seq
    df_zero$crosswalk_parent_seq <- df_zero$seq
    
    df_final <- as.data.table(rbind.fill(df_nonzero_ms, df_nonzero_ms2000, df_zero, custom_df_adj))
    
    
    
#########################################################################################
##STEP 5: MAD outlier
    new_xwalk <- as.data.table(copy(df_final))
    new_xwalk<- subset(new_xwalk, measure=="incidence" | measure == "prevalence")
    unique(new_xwalk$measure)
    
    
    ## GET AGE WEIGHTS
    unique(new_xwalk$age_start)
    
    fine_age_groups <- "yes" #specify how you want to get your age group weights
    
    if (fine_age_groups=="yes") {
      #---option 1: if all data are at the most detailed age groups
      all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=19)) ##For GBD 2020, age group set 19 defines most detailed ages
      setnames(all_fine_ages, c("age_group_years_start"), c("age_start"))
      
      
    } else if (fine_age_groups=="no") {
      #---option 2: if <1 years is aggregated, change that age group id to 28 and aggregate age weights
      all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=19)) ##For GBD 2020, age group set 19 defines most detailed ages
      setnames(all_fine_ages, c("age_group_years_start"), c("age_start"))
      not_babies <- all_fine_ages[!age_group_id %in% c(2:3, 388, 389)]
      not_babies[, age_group_years_end := age_group_years_end-1]
      not_babies <- not_babies[, c("age_start", "age_group_years_end", "age_group_id", "age_group_weight_value")]
      group_babies1 <- all_fine_ages[age_group_id %in% c(2:3, 388, 389)]
      group_babies1$age_group_id <- 28
      group_babies1$age_group_years_end <- 0.999
      group_babies1$age_start <- 0
      group_babies1[, age_group_weight_value := sum(age_group_weight_value)]
      group_babies1 <- group_babies1[, c("age_start", "age_group_years_end", "age_group_id", "age_group_weight_value")]
      group_babies1 <- unique(group_babies1)
      all_fine_ages <- rbind(not_babies, group_babies1)  
      
    }
    
    ## Delete rows with emtpy means
    new_xwalk<- new_xwalk[!is.na(mean)]
    
    
    ##merge age table map and merge on to dataset
    new_xwalk <- as.data.table(merge(new_xwalk, all_fine_ages, by = c("age_start")))
    
    #calculate age-standardized prevalence/incidence below:
    ##create new age-weights for each data source
    new_xwalk <- new_xwalk[, sum1 := sum(age_group_weight_value), by = byvars] #sum of standard age-weights for all the ages we are using, by location-age-sex-nid, sum will be same for all age-groups and possibly less than one 
    new_xwalk <- new_xwalk[, new_weight1 := age_group_weight_value/sum1, by = byvars] #divide each age-group's standard weight by the sum of all the weights in their locaiton-age-sex-nid group 
    
    ##age standardizing per location-year by sex
    #add a column titled "age_std_mean" with the age-standardized mean for the location-year-sex-nid
    new_xwalk[, as_mean := mean * new_weight1] #initially just the weighted mean for that AGE-location-year-sex-nid
    new_xwalk[, as_mean := sum(as_mean), by = byvars] #sum across ages within the location-year-sex-nid group, you now have age-standardized mean for that series
    
    ##mark as outlier if age standardized mean is 0 (either biased data or rare disease in small ppln)
    new_xwalk[as_mean == 0, is_outlier := 1] 
    new_xwalk$note_modeler <- as.character(new_xwalk$note_modeler)
    new_xwalk[as_mean == 0, note_modeler := paste0(note_modeler, " | GBD 2020, outliered this location-year-sex-NID age-series because age standardized mean is 0")]
    
    ## log-transform to pick up low outliers
    new_xwalk[as_mean != 0, as_mean := log(as_mean)]
    
    # calculate median absolute deviation
    new_xwalk[as_mean == 0, as_mean := NA] ## don't count zeros in median calculations
    new_xwalk[,mad:=mad(as_mean,na.rm = T),by=c("sex")]
    new_xwalk[,median:=median(as_mean,na.rm = T),by=c("sex")]
    
    
    # outlier based on MAD
    new_xwalk[as_mean>((MAD*mad)+median), is_outlier := 1]
    new_xwalk[as_mean>((MAD*mad)+median), note_modeler := paste0(note_modeler, " | GBD 2020, outliered because log age-standardized mean for location-year-sex-NID is higher than ", MAD, " MAD above median")]
    new_xwalk[as_mean<(median-(MAD*mad)), is_outlier := 1]
    new_xwalk[as_mean<(median-(MAD*mad)), note_modeler := paste0(note_modeler, " | GBD 2020, outliered because log age-standardized mean for location-year-sex-NID is lower than ", MAD, " MAD below median")]
    with(new_xwalk, table(sex, mad))
    with(new_xwalk, table(sex, median))
    with(new_xwalk, table(sex, exp(median)))
    
    
    # bulk outlier 
    outlier <- subset(outlier_list, bundle_id == 6710)
    outlier$sex[outlier$sex_id==1] <- "Male"
    outlier$sex[outlier$sex_id==2] <- "Female"
    
    outlier <- outlier[, c("nid", "age_group_id", "location_id", "year_start", "year_end", "sex")]
    outlier$bulk_outlier <- 1
    
    new_xwalk2 <- merge(new_xwalk, outlier, by =c("nid", "age_group_id", "location_id", "year_start", "year_end", "sex"),  all.x= T)
    new_xwalk2[(bulk_outlier==1), is_outlier:=1]
    
    # clean for upload
    new_xwalk2[, c("age_group_name",	"age_group_years_start",	"age_group_years_end",	"most_detailed",	"sum1",	"new_weight1","sum", "new_weight", "as_mean","age_group_weight_value", "age_group_id") := NULL]
    new_xwalk2[is.na(is_outlier), is_outlier:=0]
    
    all_final <- rbind.fill(new_xwalk2, new_xwalk_not_MAD)
    
    all_final$standard_error[all_final$standard_error>1] <- 1
    all_final$standard_error[all_final$standard_error<0] <- NA
    
    # save 
    write.xlsx(all_final, output_filepath_mad, sheetName = "extraction", col.names=TRUE)
    
########################################################################################
#STEP 6: Upload MAD-outliered new data 
    description <-DESCRIPTION
    result <- save_crosswalk_version(bundle_version_id, output_filepath_mad, description)
    
    print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
    print(sprintf('Crosswalk version ID from decomp 2/3 best model: %s', result$previous_step_crosswalk_version_id))
    
   