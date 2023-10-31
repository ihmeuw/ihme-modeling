################################################################################################
### Purpose: Create ratios between alternative & ref gbd hearing thresholds for extracted data from NHANES
#################################################################################################

#setup
user <- Sys.info()["user"]
date <- gsub("-", "_", Sys.Date())
j <- "FILEPATH"


library(data.table)
library(openxlsx)
library(plyr)
library(dplyr)
library(boot)
library(msm, lib.loc = "FILEPATH")
library(ggplot2)

################################### BEGINNING OF HEARING PARALLELIZATION CODE ########################################################

#INSERT PORTION DEFINING ARGS
dt <- combos 
row <- comparison 
thresh <- threshold 
row_num <- row_num 

create_dt <- function(dt, row){
  #format alt data
  alt_colname <- dt$name_alt[row]
  alt_dt <- data.table(age = dt_prep$age, sex = dt_prep$sex, year = dt_prep$year)
  alt_dt[ , sample_alt := .N, by = .(age, sex, year)]
  alt_dt$alt_prev <-  dt_prep[ ,get(alt_colname)]
  alt_dt[alt_prev > 0.99, alt_prev := 0.99]
  alt_dt[alt_prev < 0.001, alt_prev := 0.001]
  alt_dt[ , `:=` (alt_se = sd(alt_prev)/sqrt(sample_alt), logit_alt = logit(alt_prev))]
  alt_dt <- unique(alt_dt, by = c('age', 'sex', 'year'))

  alt_dt[,logit_alt_se:=sapply(1:nrow(alt_dt), function(i){
    mean_i <- alt_dt[i,alt_prev]
    mean_se_i <- alt_dt[i,alt_se]
    deltamethod(~log(x1/(1-x1)),mean_i,mean_se_i^2)
  })]

  #format ref data
  ref_colname <- dt$name_ref[row] 
  ref_dt <- data.table(age = dt_prep$age, sex = dt_prep$sex, year = dt_prep$year)
  ref_dt[ , sample_ref := .N, by = .(age, sex, year)]
  ref_dt$ref_prev <-  dt_prep[ ,get(ref_colname)]
  ref_dt[ref_prev > 0.99, ref_prev := 0.99]
  ref_dt[ref_prev < 0.001, ref_prev := 0.001]
  ref_dt[ , `:=` (ref_se = sd(ref_prev)/sqrt(sample_ref), logit_ref = logit(ref_prev))]
  ref_dt <- unique(ref_dt, by = c('age', 'sex', 'year'))

  ref_dt[,logit_ref_se:=sapply(1:nrow(ref_dt), function(i){
    mean_i <- ref_dt[i, ref_prev]
    mean_se_i <- ref_dt[i, ref_se]
    deltamethod(~log(x1/(1-x1)),mean_i,mean_se_i^2)
  })]

  #merge dts
  dt_merge <- merge(alt_dt, ref_dt, by = c("age", "sex", "year"))
  dt_merge[ ,logit_diff:=logit_alt-logit_ref]
  dt_merge[,logit_diff_se:=sqrt(logit_alt_se^2+logit_ref_se^2)]
  dt_merge[ ,nvals := .N, by = age]

  thresh <- paste0("thresh_", combos$ref_low[row],"_", combos$ref_high[row])
  combo <- paste0("comparison_", combos$combo_id[row])
  out_dir = paste0(xwalk_temp,"FILEPATH",thresh, "FILEPATH")

  return(dt_merge)
}

input_data <- create_dt(dt = combos, row = row_num)

#RUN MRBRT MODEL
hearing_crosswalk <- run_mr_brt(
  output_dir = paste0(xwalk_temp,"FILEPATH",thresh, "FILEPATH"),
  model_label = combo,
  data = input_data,
  mean_var = "logit_diff",
  se_var = "logit_diff_se",
  overwrite_previous = TRUE,
  method = "trim_maxL",
  trim_pct = 0.1,
  remove_x_intercept = FALSE
)
saveRDS(hearing_crosswalk, paste0(output_dir, model_name,"FILEPATH",model_name, "FILEPATH"))
predictions <- predict_mr_brt(hearing_crosswalk, write_draws = T)
