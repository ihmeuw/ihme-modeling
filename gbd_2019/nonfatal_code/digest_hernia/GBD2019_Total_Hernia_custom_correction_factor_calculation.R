##################################################################################################################################### 
# GBD 2019 Total inguinal, femoral, and abdominal hernia
# Purpose: To create a new custom CF to adjust for high outpatient cases in total hernia
######################################################################################################################################

library(plyr)
library(msm, lib.loc="FILEPATH")
library(metafor, lib.loc="FILEPATH")
library(readxl)
library(data.table)
library(dplyr)
library(ggplot2)


library(gtools) 
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")

###FOR GBD 2019: 
cause_path <- "Total_hernia"  
cause_name <- "digest_hernia" 

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


########################################################################################
## Data prep for OTP:PROC ratios
########################################################################################
dt <- as.data.table(read.csv("FILEPATH"))

## aggregate num_enrolid_with_proc_type and num_enrolid_with_dx_type over all years
by_vars = c("sex", "age_start", "type")
dt[, `:=` (num_enrolid_with_proc_type = sum(num_enrolid_with_proc_type), num_enrolid_with_dx_type = sum(num_enrolid_with_dx_type)), by = by_vars]
dt <- unique(dt, by = by_vars)
dt$year <- NULL

## subset to outpatient records only
dt_otp<-subset(dt, type=="otp")
dt_otp[, opt_proc := num_enrolid_with_proc_type / num_enrolid_with_dx_type]
dt_otp[, opt_nonproc := 1-opt_proc]

## add sex variables before merging
sex_names <- get_ids(table = "sex")
setnames(dt_otp, "sex", "sex_id")

dt_otp <- merge(dt_otp, sex_names)
dt_otp$sex_id <- NULL

########################################################################################
## Inpatient hospital datasets (CF2 and CF3)
########################################################################################
#STEP2
cf3_dt <- as.data.table(read.csv("FILEPATH"))
cf3_dt <- subset(cf3_dt, clinical_data_type=="inpatient")
cf3_dt <- cf3_dt[, c("mean", "lower", "upper", "standard_error", "cases", "sample_size", "location_id", "sex", "age_start", "year_start", "year_end", "nid", "location_name", "age_end", "measure",  "field_citation_value", "clinical_data_type")]
cf3_dt <- get_cases_sample_size(cf3_dt)
cf3_dt <- get_se(cf3_dt)
cf3_dt <- calculate_cases_fromse(cf3_dt)

cf2_dt <- as.data.table(read.csv("FILEPATH"))
cf2_dt <- subset(cf2_dt, clinical_data_type=="inpatient")
cf2_dt <- get_cases_sample_size(cf2_dt)
cf2_dt <- get_se(cf2_dt)
cf2_dt <- calculate_cases_fromse(cf2_dt)
cf2_dt$nid <- cf2_dt$merged_nid

age_dt <- as.data.table(get_age_metadata(12))
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]
setnames(age_dt, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_dt$age_end[age_dt$age_group_id==2] <- 0.999
age_dt$age_group_id[age_dt$age_group_id==2] <- 28
age_dt <- subset(age_dt, age_group_id>4)

sex_dt <- read.csv(paste0(j_root, "FILEPATH"))
sex_dt <- subset(sex_dt, sex_id==c(1, 2))

loc_dt <- as.data.table(get_location_metadata(location_set_id = 22))

cf2_dt <- merge(cf2_dt, sex_dt, by="sex_id")
cf2_dt <- merge(cf2_dt, age_dt, by="age_group_id")
cf2_dt <- merge(cf2_dt, loc_dt, by="location_id")


merge_cov <- c("location_id", "sex", "age_start", "year_start", "year_end", "nid", "location_name")
merged_dt <- merge(cf2_dt, cf3_dt, by = merge_cov)

## To create a variable with CF3 - CF2
merged_dt[, cf3_cf2 := mean.y - mean.x]

## To merge merged_dt and dt2
merged_dt1 <- merge(merged_dt, dt_otp, by = c("sex", "age_start"))

## To calculate new_MSinp: CF2 + (CF3-CF2)((opt:proc)))
merged_dt1[, opt_proc_hosp := cf3_cf2*opt_proc]
merged_dt1[, new_MSinp := (mean.x + opt_proc_hosp)]

## To calculate new CF: ALL / new_MSinp = ALL / (Cf2 + opt_proc)
merged_dt1[, new_CF := (mean.y / new_MSinp)]

## To calculated newly adjusted mean: CF2 * new_CF
merged_dt1[, mean := (mean.x *new_CF)]


verify <- merged_dt1[, c("sex", "mean.x", "mean.y", "opt_proc", "new_MSinp", "new_CF", "mean")]

write.csv(merged_dt1, paste0("FILEPATH"), row.names = F)



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
custom_df_adj$mean<- rowMeans(custom_df_adj[, 82:1081], na.rm = T)
custom_df_adj$lower<- custom_df_adj[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
custom_df_adj$upper <- custom_df_adj[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
custom_df_adj <- custom_df_adj[, -(draw_cols), with=FALSE] 
custom_df_adj$standard_error <- NA
custom_df_adj$cases <- NA
    
custom_df_adj <- rbind(df_merge_zero, custom_df_adj, fill=TRUE)
    custom_df_adj$crosswalk_parent_seq <- custom_df_adj$seq
    custom_df_adj$cv_hospital <- 1
    custom_df_adj$age_end <- custom_df_adj$age_end.y
    
    custom_df_adj[, c("merged_nid", "run_id",	"measure_id",	"sample_size.x",	"cases.x", "standard_error.x",	"mean.x",	"lower.x",	"upper.x",
                      "date_inserted.x",	"inserted_by",	"last_updated.x",	"last_updated_by.x",	"last_updated_action.x", "age_end.x",	
                      "age_group_weight_value", "is_estimate	most_detailed","parent_id",	"path_to_top_parent",	"level", "start_date",	"end_date",
                      "date_inserted.y",	"last_updated.y",	"last_updated_by.y",	"last_updated_action.y",	"lancet_label","mean.y",	"lower.y",	"upper.y",
                      "standard_error.y",	"cases.y",	"sample_size.y", "sd")] <- NULL
  
    custom_df_adj$lower[custom_df_adj$lower <0] <- 0
    custom_df_adj$note_modeler <- ""
    custom_df_adj[, note_modeler := paste0(note_modeler, " | corrected with custom CF")]
    
    
#APPEND OTHER DATA (i.e REMISSION AND INCIDENCE) BEFORE SAVING
    dt2<- as.data.table(read.csv("FILEPATH"))
    dt2 <- subset(dt2, measure!="prevalence")
    
    custom_df_adj <- rbind(dt2, dt_final, fill=TRUE)
    custom_df_adj$cv_hospital[is.na(custom_df_adj$cv_hospital)] <- 0
    custom_df_adj[, c("age_group_id", "age_group_weight_value","age_group_id_x", "age_group_weight_value.x", "age_group_id.y", "age_group_weight_value.y")] <- NULL
    custom_df_adj$sex <- as.character(custom_df_adj$sex)
    
write.csv(custom_df_adj, paste0("FILEPATH"), row.names = F)
  




